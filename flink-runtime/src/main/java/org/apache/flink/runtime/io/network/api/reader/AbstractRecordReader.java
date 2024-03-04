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

package org.apache.flink.runtime.io.network.api.reader;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A record-oriented reader.
 *
 * <p>This abstract base class is used by both the mutable and immutable record readers.
 *
 * @param <T> The type of the record that can be read with this record reader.
 */
abstract class AbstractRecordReader<T extends IOReadableWritable> extends AbstractReader
        implements ReaderBase {


    /**
     * InputChannelInfo 代表某个gate下面的某个channel
     * 每个InputChannelInfo 对应一个反序列化对象 用于处理数据
     */
    private final Map<InputChannelInfo, RecordDeserializer<T>> recordDeserializers;

    /**
     * 表示内部是否还有残留数据
     */
    private final Map<RecordDeserializer<T>, Boolean> partialData = new IdentityHashMap<>();

    @Nullable private RecordDeserializer<T> currentRecordDeserializer;

    private boolean finishedStateReading;

    private boolean requestedPartitions;

    private boolean isFinished;

    /**
     * Creates a new AbstractRecordReader that de-serializes records from the given input gate and
     * can spill partial records to disk, if they grow large.
     *
     * @param inputGate The input gate to read from.   该对象提供数据
     * @param tmpDirectories The temp directories. USed for spilling if the reader concurrently
     *     reconstructs multiple large records.   将数据拆分到几个目录下
     */
    @SuppressWarnings("unchecked")
    protected AbstractRecordReader(InputGate inputGate, String[] tmpDirectories) {
        super(inputGate);

        // Initialize one deserializer per input channel
        recordDeserializers =
                inputGate.getChannelInfos().stream()
                        .collect(
                                Collectors.toMap(
                                        Function.identity(),
                                        channelInfo ->
                                                new SpillingAdaptiveSpanningRecordDeserializer<>(
                                                        tmpDirectories)));

        // 初始化时 认为每个对象都没有残留数据 (因为还未开始读取)
        for (RecordDeserializer<T> serializer : recordDeserializers.values()) {
            partialData.put(serializer, Boolean.FALSE);
        }
    }

    /**
     * @param target  该对象会读取数据
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    protected boolean getNextRecord(T target) throws IOException, InterruptedException {
        // The action of partition request was removed from InputGate#setup since FLINK-16536, and
        // this is the only
        // unified way for launching partition request for batch jobs. In order to avoid potential
        // performance concern,
        // we might consider migrating this action back to the setup based on some condition
        // judgement future.

        // 这个分支只会进入一次   应该是要先恢复之前的数据
        if (!finishedStateReading) {
            inputGate.finishReadRecoveredState();
            finishedStateReading = true;
        }

        // 需要调用该方法才会拉取分区数据
        if (!requestedPartitions) {
            CompletableFuture<Void> stateConsumedFuture = inputGate.getStateConsumedFuture();

            // 等待检查点状态被消耗完 此时已经完成数据恢复了
            while (!stateConsumedFuture.isDone()) {
                Optional<BufferOrEvent> polled = inputGate.pollNext();
                Preconditions.checkState(!polled.isPresent());
            }
            // 设置用于生成检查点的对象
            inputGate.setChannelStateWriter(ChannelStateWriter.NO_OP);
            // 触发拉取动作
            inputGate.requestPartitions();
            requestedPartitions = true;
        }

        // 表示已经读取完数据了
        if (isFinished) {
            return false;
        }

        while (true) {
            if (currentRecordDeserializer != null) {
                // 使用target读取数据  并产生result
                DeserializationResult result = currentRecordDeserializer.getNextRecord(target);

                // 表示内部的buffer已经消费完了
                if (result.isBufferConsumed()) {
                    partialData.put(currentRecordDeserializer, Boolean.FALSE);
                    currentRecordDeserializer = null;
                }

                // 读到了一条完整数据就可以返回了
                if (result.isFullRecord()) {
                    return true;
                }
            }

            // 获取下一个装有数据的buffer
            final BufferOrEvent bufferOrEvent =
                    inputGate.getNext().orElseThrow(IllegalStateException::new);

            if (bufferOrEvent.isBuffer()) {
                // 找到对应的反序列化对象
                currentRecordDeserializer = recordDeserializers.get(bufferOrEvent.getChannelInfo());
                // 填充数据   因为本对象被使用了 所以认为还有残留数据
                currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
                partialData.put(currentRecordDeserializer, Boolean.TRUE);
            } else {
                // sanity check for leftover data in deserializers. events should only come between
                // records, not in the middle of a fragment
                // 如果还有残留数据的情况下 收到event属于异常情况
                if (partialData.get(recordDeserializers.get(bufferOrEvent.getChannelInfo()))) {
                    throw new IOException(
                            "Received an event in channel "
                                    + bufferOrEvent.getChannelInfo()
                                    + " while still having "
                                    + "data from a record. This indicates broken serialization logic. "
                                    + "If you are using custom serialization code (Writable or Value types), check their "
                                    + "serialization routines. In the case of Kryo, check the respective Kryo serializer.");
                }

                // 成功处理事件
                if (handleEvent(bufferOrEvent.getEvent())) {
                    // 表示数据读取完了
                    if (inputGate.isFinished()) {
                        isFinished = true;
                        return false;
                    } else if (hasReachedEndOfSuperstep()) {
                        return false;
                    }
                    // else: More data is coming...
                }
            }
        }
    }

    public void clearBuffers() {
        for (RecordDeserializer<?> deserializer : recordDeserializers.values()) {
            deserializer.clear();
        }
    }
}
