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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * * A {@link DataBuffer} implementation which sorts all appended records only by subpartition
 * index. Records of the same subpartition keep the appended order.
 *
 * <p>Different from the {@link SortBasedDataBuffer}, in this {@link DataBuffer} implementation,
 * memory segment boundary serves as the nature data boundary of different subpartitions, which
 * means that one memory segment can never contain data from different subpartitions.
 * 表示基于hash的数据缓冲区
 */
public class HashBasedDataBuffer implements DataBuffer {

    /** A list of {@link MemorySegment}s used to store data in memory.
     * 存储数据的容器
     * */
    private final LinkedList<MemorySegment> freeSegments;

    /** {@link BufferRecycler} used to recycle {@link #freeSegments}. */
    private final BufferRecycler bufferRecycler;

    /** Number of guaranteed buffers can be allocated from the buffer pool for data sort.
     * 需要维护的buffer数量
     * */
    private final int numGuaranteedBuffers;

    /** Buffers containing data for all subpartitions.
     * 每个ArrayDeque<BufferConsumer> 对应一个子分区
     * */
    private final ArrayDeque<BufferConsumer>[] buffers;

    /** Size of buffers requested from buffer pool. All buffers must be of the same size. */
    private final int bufferSize;

    // ---------------------------------------------------------------------------------------------
    // Statistics and states
    // ---------------------------------------------------------------------------------------------

    /** Total number of bytes already appended to this sort buffer. */
    private long numTotalBytes;

    /** Total number of records already appended to this sort buffer. */
    private long numTotalRecords;

    /** Whether this sort buffer is finished. One can only read a finished sort buffer. */
    private boolean isFinished;

    /** Whether this sort buffer is released. A released sort buffer can not be used. */
    private boolean isReleased;

    // ---------------------------------------------------------------------------------------------
    // For writing
    // ---------------------------------------------------------------------------------------------

    /** Partial buffers to be appended data for each channel. */
    private final BufferBuilder[] builders;

    /** Total number of network buffers already occupied currently by this sort buffer. */
    private int numBuffersOccupied;

    // ---------------------------------------------------------------------------------------------
    // For reading
    // ---------------------------------------------------------------------------------------------

    /** Used to index the current available channel to read data from.
     * 记录当前读取到的下标
     * */
    private int readOrderIndex;

    /** Data of different subpartitions in this sort buffer will be read in this order.
     * 可以利用该数组进行一次重排序
     * */
    private final int[] subpartitionReadOrder;

    /** Total number of bytes already read from this sort buffer. */
    private long numTotalBytesRead;

    /**
     *
     * @param freeSegments  一开始提供一组空闲的内存块
     * @param bufferRecycler
     * @param numSubpartitions
     * @param bufferSize
     * @param numGuaranteedBuffers
     * @param customReadOrder
     */
    public HashBasedDataBuffer(
            LinkedList<MemorySegment> freeSegments,
            BufferRecycler bufferRecycler,
            int numSubpartitions,
            int bufferSize,
            int numGuaranteedBuffers,
            @Nullable int[] customReadOrder) {
        checkArgument(numGuaranteedBuffers > 0, "No guaranteed buffers for sort.");

        this.freeSegments = checkNotNull(freeSegments);
        this.bufferRecycler = checkNotNull(bufferRecycler);
        this.bufferSize = bufferSize;
        this.numGuaranteedBuffers = numGuaranteedBuffers;
        checkState(numGuaranteedBuffers <= freeSegments.size(), "Wrong number of free segments.");

        this.builders = new BufferBuilder[numSubpartitions];
        this.buffers = new ArrayDeque[numSubpartitions];
        for (int channel = 0; channel < numSubpartitions; ++channel) {
            this.buffers[channel] = new ArrayDeque<>();
        }

        // 如果传了顺序 就按照顺序读取
        this.subpartitionReadOrder = new int[numSubpartitions];
        if (customReadOrder != null) {
            checkArgument(customReadOrder.length == numSubpartitions, "Illegal data read order.");
            System.arraycopy(customReadOrder, 0, this.subpartitionReadOrder, 0, numSubpartitions);
        } else {
            // 默认顺序就是 1234567
            for (int channel = 0; channel < numSubpartitions; ++channel) {
                this.subpartitionReadOrder[channel] = channel;
            }
        }
    }

    /**
     * Partial data of the target record can be written if this {@link HashBasedDataBuffer} is full.
     * The remaining data of the target record will be written to the next data region (a new data
     * buffer or this data buffer after reset).
     */
    @Override
    public boolean append(ByteBuffer source, int targetChannel, Buffer.DataType dataType)
            throws IOException {
        checkArgument(source.hasRemaining(), "Cannot append empty data.");
        checkState(!isFinished, "Sort buffer is already finished.");
        checkState(!isReleased, "Sort buffer is already released.");

        // 还有数据 才有append的必要
        int totalBytes = source.remaining();
        if (dataType.isBuffer()) {
            writeRecord(source, targetChannel);
        } else {
            writeEvent(source, targetChannel, dataType);
        }

        // 数据未处理完 返回true
        if (source.hasRemaining()) {
            return true;
        }
        ++numTotalRecords;
        numTotalBytes += totalBytes - source.remaining();
        return false;
    }

    /**
     * 写入事件数据
     * @param source
     * @param targetChannel
     * @param dataType
     */
    private void writeEvent(ByteBuffer source, int targetChannel, Buffer.DataType dataType) {
        // 要直接替换buffer
        BufferBuilder builder = builders[targetChannel];
        if (builder != null) {
            builder.finish();
            builder.close();
            builders[targetChannel] = null;
        }

        MemorySegment segment =
                MemorySegmentFactory.allocateUnpooledOffHeapMemory(source.remaining());
        segment.put(0, source, segment.size());

        // 追加事件
        BufferConsumer consumer =
                new BufferConsumer(
                        new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType),
                        segment.size());
        buffers[targetChannel].add(consumer);
    }

    /**
     * 添加记录
     * @param source
     * @param targetChannel
     */
    private void writeRecord(ByteBuffer source, int targetChannel) {
        BufferBuilder builder = builders[targetChannel];
        int availableBytes = builder != null ? builder.getWritableBytes() : 0;

        // 表示空间不足 直接返回
        if (source.remaining()
                > availableBytes
                        + (numGuaranteedBuffers - numBuffersOccupied) * (long) bufferSize) {
            return;
        }

        do {
            if (builder == null) {
                // 首次初始化  利用一个空闲的segment
                builder = new BufferBuilder(freeSegments.poll(), bufferRecycler);
                // 追加到相关队列
                buffers[targetChannel].add(builder.createBufferConsumer());
                ++numBuffersOccupied;
                // 设置当前channel对应的内存块
                builders[targetChannel] = builder;
            }

            // 剩余空间足够添加数据
            builder.append(source);
            // 该buffer被写满
            if (builder.isFull()) {
                // 重置builder
                builder.finish();
                builder.close();
                builders[targetChannel] = null;
                builder = null;
            }
        } while (source.hasRemaining());
    }

    /**
     *
     * @param transitBuffer  用于存储读取的数据
     * @return
     */
    @Override
    public BufferWithChannel getNextBuffer(MemorySegment transitBuffer) {
        checkState(isFinished, "Sort buffer is not ready to be read.");
        checkState(!isReleased, "Sort buffer is already released.");

        BufferWithChannel buffer = null;
        // 表示数据读完了
        if (!hasRemaining() || readOrderIndex >= subpartitionReadOrder.length) {
            return null;
        }

        int targetChannel = subpartitionReadOrder[readOrderIndex];
        while (buffer == null) {
            // 读取数据
            BufferConsumer consumer = buffers[targetChannel].poll();
            if (consumer != null) {
                buffer = new BufferWithChannel(consumer.build(), targetChannel);
                numBuffersOccupied -= buffer.getBuffer().isBuffer() ? 1 : 0;
                numTotalBytesRead += buffer.getBuffer().readableBytes();
                consumer.close();
            } else {
                // 表示当前index读取完了 切换到下个下标
                if (++readOrderIndex >= subpartitionReadOrder.length) {
                    break;
                }
                targetChannel = subpartitionReadOrder[readOrderIndex];
            }
        }
        return buffer;
    }

    @Override
    public long numTotalRecords() {
        return numTotalRecords;
    }

    @Override
    public long numTotalBytes() {
        return numTotalBytes;
    }

    @Override
    public boolean hasRemaining() {
        return numTotalBytesRead < numTotalBytes;
    }

    @Override
    public void finish() {
        checkState(!isFinished, "DataBuffer is already finished.");

        isFinished = true;
        for (int channel = 0; channel < builders.length; ++channel) {
            BufferBuilder builder = builders[channel];
            if (builder != null) {
                builder.finish();
                builder.close();
                builders[channel] = null;
            }
        }
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public void release() {
        if (isReleased) {
            return;
        }
        isReleased = true;

        for (int channel = 0; channel < builders.length; ++channel) {
            BufferBuilder builder = builders[channel];
            if (builder != null) {
                builder.close();
                builders[channel] = null;
            }
        }

        // 释放buffer
        for (ArrayDeque<BufferConsumer> buffer : buffers) {
            BufferConsumer consumer = buffer.poll();
            while (consumer != null) {
                consumer.close();
                consumer = buffer.poll();
            }
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }
}
