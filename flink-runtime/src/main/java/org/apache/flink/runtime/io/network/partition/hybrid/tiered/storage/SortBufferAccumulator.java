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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferWithChannel;
import org.apache.flink.runtime.io.network.partition.DataBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The sort-based implementation of the {@link BufferAccumulator}. The {@link BufferAccumulator}
 * receives the records from {@link TieredStorageProducerClient} and the records will accumulate and
 * transform to finished buffers. The accumulated buffers will be transferred to the corresponding
 * tier dynamically.
 *
 * <p>The {@link SortBufferAccumulator} can help use less buffers to accumulate data, which
 * decouples the buffer usage with the number of parallelism. The number of buffers used by the
 * {@link SortBufferAccumulator} will be numBuffers at most. Once the {@link DataBuffer} is full, or
 * switching from broadcast to non-broadcast(or vice versa), the buffer in the sort buffer will be
 * flushed to the tiers.
 *
 * <p>Note that this class need not be thread-safe, because it should only be accessed from the main
 * thread.
 *
 * 基于排序的？
 */
public class SortBufferAccumulator implements BufferAccumulator {

    /** The number of the subpartitions. */
    private final int numSubpartitions;

    /** The total number of the buffers used by the {@link SortBufferAccumulator}.
     * 表示需要使用多少buffer
     * */
    private final int numBuffers;

    /** The byte size of one single buffer.
     * 单个buffer的大小
     * */
    private final int bufferSizeBytes;

    /** The empty buffers without storing data. */
    private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

    /** The memory manager of the tiered storage.
     * 通过该对象申请内存
     * */
    private final TieredStorageMemoryManager memoryManager;

    /**
     * The {@link DataBuffer} is utilized to accumulate the incoming records. Whenever there is a
     * transition from broadcast to non-broadcast (or vice versa), the buffer is flushed to ensure
     * data integrity. Note that this can be null before using it to store records, and this {@link
     * DataBuffer} will be released once flushed.
     * 当前使用的buffer
     */
    @Nullable private DataBuffer currentDataBuffer;

    /**
     * The buffer recycler. Note that this can be null before requesting buffers from the memory
     * manager.
     */
    @Nullable private BufferRecycler bufferRecycler;

    /**
     * The {@link SortBufferAccumulator}'s accumulated buffer flusher is not prepared during
     * construction, requiring the field to be initialized during setup. Therefore, it is necessary
     * to verify whether this field is null before using it.
     * 包含聚合逻辑
     */
    @Nullable
    private BiConsumer<TieredStorageSubpartitionId, List<Buffer>> accumulatedBufferFlusher;

    /** Whether the current {@link DataBuffer} is a broadcast sort buffer. */
    private boolean isBroadcastDataBuffer;

    public SortBufferAccumulator(
            int numSubpartitions,
            int numBuffers,
            int bufferSizeBytes,
            TieredStorageMemoryManager memoryManager) {
        this.numSubpartitions = numSubpartitions;
        this.bufferSizeBytes = bufferSizeBytes;
        this.numBuffers = numBuffers;
        this.memoryManager = memoryManager;
    }

    @Override
    public void setup(BiConsumer<TieredStorageSubpartitionId, List<Buffer>> bufferFlusher) {
        this.accumulatedBufferFlusher = bufferFlusher;
    }

    /**
     * 处理收到的数据
     * @param record the received record     表示收到的数据
     * @param subpartitionId the subpartition id of the record  通过子分区定位到buffer
     * @param dataType the data type of the record
     * @param isBroadcast whether the record is a broadcast record
     * @throws IOException
     */
    @Override
    public void receive(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException {
        int targetSubpartition = subpartitionId.getSubpartitionId();
        // 在广播和非广播之间切换
        switchCurrentDataBufferIfNeeded(isBroadcast);

        // buffer没满时 返回false
        if (!checkNotNull(currentDataBuffer).append(record, targetSubpartition, dataType)) {
            return;
        }

        // The sort buffer is empty, but we failed to write the record into it, which indicates the
        // record is larger than the sort buffer can hold. So the record is written into multiple
        // buffers directly.

        if (!currentDataBuffer.hasRemaining()) {
            currentDataBuffer.release();
            // 一次性写入
            writeLargeRecord(record, targetSubpartition, dataType);
            return;
        }

        // 将剩余数据刷盘
        flushDataBuffer();
        checkState(record.hasRemaining(), "Empty record.");
        receive(record, subpartitionId, dataType, isBroadcast);
    }

    @Override
    public void close() {
        flushCurrentDataBuffer();
        releaseFreeBuffers();
        if (currentDataBuffer != null) {
            currentDataBuffer.release();
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void switchCurrentDataBufferIfNeeded(boolean isBroadcast) {
        // 表示当前正在使用broadcastBuffer
        if (isBroadcast == isBroadcastDataBuffer
                && currentDataBuffer != null
                && !currentDataBuffer.isReleased()
                && !currentDataBuffer.isFinished()) {
            return;
        }
        isBroadcastDataBuffer = isBroadcast;

        // 将buffer之前囤积的数据刷盘
        flushCurrentDataBuffer();
        // 申请一个新buffer
        currentDataBuffer = createNewDataBuffer();
    }

    /**
     * 申请新buffer
     * @return
     */
    private DataBuffer createNewDataBuffer() {
        requestBuffers();

        // Use the half of the buffers for writing, and the other half for reading
        // 一半读一半写
        int numBuffersForSort = freeSegments.size() / 2;
        return new TieredStorageSortBuffer(
                freeSegments,
                this::recycleBuffer,
                numSubpartitions,
                bufferSizeBytes,
                numBuffersForSort);
    }


    /**
     * 维持一定数量的buffer
     */
    private void requestBuffers() {
        while (freeSegments.size() < numBuffers) {
            Buffer buffer = requestBuffer();
            freeSegments.add(checkNotNull(buffer).getMemorySegment());
            if (bufferRecycler == null) {
                bufferRecycler = buffer.getRecycler();
            }
        }
    }

    /**
     * 将数据刷盘
     */
    private void flushDataBuffer() {
        if (currentDataBuffer == null
                || currentDataBuffer.isReleased()
                || !currentDataBuffer.hasRemaining()) {
            return;
        }
        currentDataBuffer.finish();

        do {
            MemorySegment freeSegment = getFreeSegment();
            // 将buffer的数据移动到seg中
            BufferWithChannel bufferWithChannel = currentDataBuffer.getNextBuffer(freeSegment);
            if (bufferWithChannel == null) {
                break;
            }
            flushBuffer(bufferWithChannel);
        } while (true);

        releaseFreeBuffers();
        currentDataBuffer.release();
    }

    private void flushCurrentDataBuffer() {
        if (currentDataBuffer != null) {
            flushDataBuffer();
            currentDataBuffer = null;
        }
    }

    /**
     * 一次性写入全部数据
     * @param record
     * @param subpartitionId
     * @param dataType
     */
    private void writeLargeRecord(ByteBuffer record, int subpartitionId, Buffer.DataType dataType) {

        checkState(dataType != Buffer.DataType.EVENT_BUFFER);
        while (record.hasRemaining()) {
            int toCopy = Math.min(record.remaining(), bufferSizeBytes);
            MemorySegment writeBuffer = requestBuffer().getMemorySegment();
            writeBuffer.put(0, record, toCopy);

            // 每读取一个 触发一次flush
            flushBuffer(
                    new BufferWithChannel(
                            new NetworkBuffer(
                                    writeBuffer, checkNotNull(bufferRecycler), dataType, toCopy),
                            subpartitionId));
        }

        releaseFreeBuffers();
    }

    private MemorySegment getFreeSegment() {
        MemorySegment freeSegment = freeSegments.poll();
        if (freeSegment == null) {
            freeSegment = requestBuffer().getMemorySegment();
        }
        return freeSegment;
    }

    private void flushBuffer(BufferWithChannel bufferWithChannel) {
        checkNotNull(accumulatedBufferFlusher)
                .accept(
                        new TieredStorageSubpartitionId(bufferWithChannel.getChannelIndex()),
                        Collections.singletonList(bufferWithChannel.getBuffer()));
    }

    /**
     * 申请buffer
     * @return
     */
    private Buffer requestBuffer() {
        BufferBuilder bufferBuilder = memoryManager.requestBufferBlocking(this);
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumerFromBeginning();
        Buffer buffer = bufferConsumer.build();
        bufferBuilder.close();
        bufferConsumer.close();
        return buffer;
    }

    private void releaseFreeBuffers() {
        freeSegments.forEach(this::recycleBuffer);
        freeSegments.clear();
    }

    private void recycleBuffer(MemorySegment memorySegment) {
        checkNotNull(bufferRecycler).recycle(memorySegment);
    }
}
