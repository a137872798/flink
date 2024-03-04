/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.LAST_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.PARTIAL_RECORD;

/** @param <T> The type of the record to be deserialized.
 * 该对象用于将记录反序列化
 * */
public class SpillingAdaptiveSpanningRecordDeserializer<T extends IOReadableWritable>
        implements RecordDeserializer<T> {
    public static final int DEFAULT_THRESHOLD_FOR_SPILLING = 5 * 1024 * 1024; // 5 MiBytes
    public static final int DEFAULT_FILE_BUFFER_SIZE = 2 * 1024 * 1024;
    private static final int MIN_THRESHOLD_FOR_SPILLING = 100 * 1024; // 100 KiBytes
    private static final int MIN_FILE_BUFFER_SIZE = 50 * 1024; // 50 KiBytes

    static final int LENGTH_BYTES = Integer.BYTES;

    private final NonSpanningWrapper nonSpanningWrapper;

    private final SpanningWrapper spanningWrapper;

    /**
     * 存储读取出来的一批数据
     */
    @Nullable private Buffer currentBuffer;

    public SpillingAdaptiveSpanningRecordDeserializer(String[] tmpDirectories) {
        this(tmpDirectories, DEFAULT_THRESHOLD_FOR_SPILLING, DEFAULT_FILE_BUFFER_SIZE);
    }

    /**
     *
     * @param tmpDirectories  存储数据使用的一组临时目录
     * @param thresholdForSpilling
     * @param fileBufferSize
     */
    public SpillingAdaptiveSpanningRecordDeserializer(
            String[] tmpDirectories, int thresholdForSpilling, int fileBufferSize) {
        nonSpanningWrapper = new NonSpanningWrapper();
        spanningWrapper =
                new SpanningWrapper(
                        tmpDirectories,
                        Math.max(thresholdForSpilling, MIN_THRESHOLD_FOR_SPILLING),
                        Math.max(fileBufferSize, MIN_FILE_BUFFER_SIZE));
    }

    /**
     * 补充下个要读取的数据
     * @param buffer
     * @throws IOException
     */
    @Override
    public void setNextBuffer(Buffer buffer) throws IOException {
        currentBuffer = buffer;

        int offset = buffer.getMemorySegmentOffset();
        // 包含数据的内存块
        MemorySegment segment = buffer.getMemorySegment();
        int numBytes = buffer.getSize();


        // check if some spanning record deserialization is pending
        // 表示还有数据未消耗
        if (spanningWrapper.getNumGatheredBytes() > 0) {
            // 追加数据
            spanningWrapper.addNextChunkFromMemorySegment(segment, offset, numBytes);
        } else {
            // 更新内部的指针/内存块 等信息
            nonSpanningWrapper.initializeFromMemorySegment(segment, offset, numBytes + offset);
        }
    }

    /**
     * 返回还未被读取的数据
     * @return
     * @throws IOException
     */
    @Override
    public CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException {
        return nonSpanningWrapper.hasRemaining()
                ? nonSpanningWrapper.getUnconsumedSegment()
                : spanningWrapper.getUnconsumedSegment();
    }

    /**
     *
     * @param target
     * @return
     * @throws IOException
     */
    @Override
    public DeserializationResult getNextRecord(T target) throws IOException {
        // always check the non-spanning wrapper first.
        // this should be the majority of the cases for small records
        // for large records, this portion of the work is very small in comparison anyways

        final DeserializationResult result = readNextRecord(target);

        // buffer被使用完了  进行回收
        if (result.isBufferConsumed()) {
            currentBuffer.recycleBuffer();
            currentBuffer = null;
        }
        return result;
    }

    /**
     * 读取数据并填充target
     * @param target
     * @return
     * @throws IOException
     */
    private DeserializationResult readNextRecord(T target) throws IOException {

        // 优先读取nonSpanningWrapper中的数据
        // hasCompleteLength 表示至少还能读取到长度信息
        if (nonSpanningWrapper.hasCompleteLength()) {
            return readNonSpanningRecord(target);

            // 剩余的数据甚至不足一个length  全部转移到spanningWrapper的 lengthBuffer中
        } else if (nonSpanningWrapper.hasRemaining()) {
            nonSpanningWrapper.transferTo(spanningWrapper.lengthBuffer);
            return PARTIAL_RECORD;

            // 另一个容器有完整的数据
        } else if (spanningWrapper.hasFullRecord()) {
            // 使用target读取数据
            target.read(spanningWrapper.getInputView());

            // 将剩余数据转移到nonSpanningWrapper
            spanningWrapper.transferLeftOverTo(nonSpanningWrapper);
            return nonSpanningWrapper.hasRemaining()
                    ? INTERMEDIATE_RECORD_FROM_BUFFER
                    : LAST_RECORD_FROM_BUFFER;  // 刚好全部消耗完  就是2个true

        } else {
            // 本次数据不足
            return PARTIAL_RECORD;
        }
    }

    /**
     * 从nonSpanningWrapper中读取数据
     * @param target
     * @return
     * @throws IOException
     */
    private DeserializationResult readNonSpanningRecord(T target) throws IOException {
        // following three calls to nonSpanningWrapper from object oriented design would be better
        // to encapsulate inside nonSpanningWrapper, but then nonSpanningWrapper.readInto equivalent
        // would have to return a tuple of DeserializationResult and recordLen, which would affect
        // performance too much

        // 如果是按一定规则读取的话 那么首4个字节一定是长度
        int recordLen = nonSpanningWrapper.readInt();
        if (nonSpanningWrapper.canReadRecord(recordLen)) {
            // target具备读取逻辑  会从nonSpanningWrapper中读取数据
            return nonSpanningWrapper.readInto(target);
        } else {
            // 将剩余数据移动到spanningWrapper
            spanningWrapper.transferFrom(nonSpanningWrapper, recordLen);
            return PARTIAL_RECORD;
        }
    }

    /**
     * 清空数据
     */
    @Override
    public void clear() {
        if (currentBuffer != null && !currentBuffer.isRecycled()) {
            // 回收buffer
            currentBuffer.recycleBuffer();
            currentBuffer = null;
        }
        nonSpanningWrapper.clear();
        spanningWrapper.clear();
    }
}
