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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceProducer;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link DiskIOScheduler} is a scheduler that controls the reading of data from shuffle files.
 * It ensures the correct order of buffers in each subpartition during file reading. The scheduler
 * implements the {@link NettyServiceProducer} interface to send the buffers to the Netty server
 * through the {@link NettyConnectionWriter}.
 */
public class DiskIOScheduler implements Runnable, BufferRecycler, NettyServiceProducer {

    private static final Logger LOG = LoggerFactory.getLogger(DiskIOScheduler.class);

    private final Object lock = new Object();

    /** The partition id.
     * 对应的分区
     * */
    private final TieredStoragePartitionId partitionId;

    /** The executor is responsible for scheduling the disk read process. */
    private final ScheduledExecutorService ioExecutor;

    /**
     * The buffer pool is specifically designed for reading from disk and shared in the TaskManager.
     * 申请内存用的内存池
     */
    private final BatchShuffleReadBufferPool bufferPool;

    /**
     * The maximum number of buffers that can be allocated and still not recycled for a
     * subpartition, which ensures that each subpartition can be consumed evenly.
     */
    private final int maxBufferReadAhead;

    /**
     * The maximum number of buffers that can be allocated and still not recycled by a single {@link
     * DiskIOScheduler} for all subpartitions. This ensures that different {@link DiskIOScheduler}s
     * in the TaskManager can evenly use the buffer pool.
     */
    private final int maxRequestedBuffers;

    /**
     * The maximum time to wait when requesting read buffers from the buffer pool before throwing an
     * exception.
     */
    private final Duration bufferRequestTimeout;

    /**
     * Retrieve the segment id if the buffer index represents the first buffer. The first integer is
     * the id of subpartition, and the second integer is buffer index and the value is segment id.
     * 提供查询能力
     */
    private final BiFunction<Integer, Integer, Integer> firstBufferIndexInSegmentRetriever;

    /**
     * 包含数据信息  传入坐标(子分区id seg_id 等可以查询到数据)
     */
    private final PartitionFileReader partitionFileReader;

    @GuardedBy("lock")
    private final Map<NettyConnectionId, ScheduledSubpartitionReader> allScheduledReaders =
            new HashMap<>();

    @GuardedBy("lock")
    private boolean isRunning;

    @GuardedBy("lock")
    private int numRequestedBuffers;

    @GuardedBy("lock")
    private boolean isReleased;

    public DiskIOScheduler(
            TieredStoragePartitionId partitionId,
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            int maxRequestedBuffers,
            Duration bufferRequestTimeout,
            int maxBufferReadAhead,
            BiFunction<Integer, Integer, Integer> firstBufferIndexInSegmentRetriever,
            PartitionFileReader partitionFileReader) {
        this.partitionId = partitionId;
        this.bufferPool = checkNotNull(bufferPool);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.maxRequestedBuffers = maxRequestedBuffers;
        this.bufferRequestTimeout = checkNotNull(bufferRequestTimeout);
        this.maxBufferReadAhead = maxBufferReadAhead;
        this.firstBufferIndexInSegmentRetriever = firstBufferIndexInSegmentRetriever;
        this.partitionFileReader = partitionFileReader;
        bufferPool.registerRequester(this);
    }

    @Override
    public synchronized void run() {
        // 读取数据 还会写入到payload队列
        // 返回值表示读取了多少buffer
        int numBuffersRead = readBuffersFromFile();
        synchronized (lock) {
            numRequestedBuffers += numBuffersRead;
            isRunning = false;
        }
        // 没读取到数据 延时处理
        if (numBuffersRead == 0) {
            ioExecutor.schedule(this::triggerScheduling, 5, TimeUnit.MILLISECONDS);
        } else {
            triggerScheduling();
        }
    }

    /**
     * 一个连接对应一个 NettyConnectionWriter
     * @param subpartitionId subpartition id indicates the id of subpartition.
     * @param nettyConnectionWriter writer is used to write buffers to netty connection.
     */
    @Override
    public void connectionEstablished(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        synchronized (lock) {
            checkState(!isReleased, "DiskIOScheduler is already released.");
            // 每个reader会记录自己的progress 所以同一份数据可以按照连接数被发送多次
            ScheduledSubpartitionReader scheduledSubpartitionReader =
                    new ScheduledSubpartitionReader(subpartitionId, nettyConnectionWriter);
            allScheduledReaders.put(
                    nettyConnectionWriter.getNettyConnectionId(), scheduledSubpartitionReader);
            triggerScheduling();
        }
    }

    /**
     * 解除连接 就是移除某个reader
     * @param id
     */
    @Override
    public void connectionBroken(NettyConnectionId id) {
        synchronized (lock) {
            allScheduledReaders.remove(id);
        }
    }

    @Override
    public void recycle(MemorySegment segment) {
        synchronized (lock) {
            bufferPool.recycle(segment);
            --numRequestedBuffers;
            triggerScheduling();
        }
    }

    public void release() {
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            allScheduledReaders.clear();
            partitionFileReader.release();
            bufferPool.unregisterRequester(this);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    /**
     * 从文件中读取数据
     * @return
     */
    private int readBuffersFromFile() {
        // 先进行准备工作
        List<ScheduledSubpartitionReader> scheduledReaders = sortScheduledReaders();
        if (scheduledReaders.isEmpty()) {
            return 0;
        }
        Queue<MemorySegment> buffers;
        try {
            buffers = allocateBuffers();
        } catch (Exception exception) {
            failScheduledReaders(scheduledReaders, exception);
            LOG.error("Failed to request buffers for data reading.", exception);
            return 0;
        }

        // 未申请到buffer
        int numBuffersAllocated = buffers.size();
        if (numBuffersAllocated <= 0) {
            return 0;
        }

        for (ScheduledSubpartitionReader scheduledReader : scheduledReaders) {
            if (buffers.isEmpty()) {
                break;
            }
            try {
                // 每个reader都读取数据   这里还会将数据写入到 payload队列
                scheduledReader.loadDiskDataToBuffers(buffers, this);
            } catch (Exception throwable) {
                failScheduledReaders(Collections.singletonList(scheduledReader), throwable);
                LOG.debug("Failed to read shuffle data.", throwable);
            }
        }
        // 表示读取了多少个buffer的数据
        int numBuffersRead = numBuffersAllocated - buffers.size();
        releaseBuffers(buffers);
        return numBuffersRead;
    }

    /**
     * 让所有reader做好准备工作
     * @return
     */
    private List<ScheduledSubpartitionReader> sortScheduledReaders() {
        List<ScheduledSubpartitionReader> scheduledReaders;
        synchronized (lock) {
            if (isReleased) {
                return new ArrayList<>();
            }
            scheduledReaders = new ArrayList<>(allScheduledReaders.values());
        }
        for (ScheduledSubpartitionReader reader : scheduledReaders) {
            reader.prepareForScheduling();
        }
        Collections.sort(scheduledReaders);
        return scheduledReaders;
    }

    /**
     * 申请buffer
     * @return
     * @throws Exception
     */
    private Queue<MemorySegment> allocateBuffers() throws Exception {
        long timeoutTime = getBufferRequestTimeoutTime();
        do {
            // 申请一定量的buffer
            List<MemorySegment> buffers = bufferPool.requestBuffers();
            if (!buffers.isEmpty()) {
                return new ArrayDeque<>(buffers);
            }
            synchronized (lock) {
                if (isReleased) {
                    return new ArrayDeque<>();
                }
            }
        } while (System.currentTimeMillis() < timeoutTime
                || System.currentTimeMillis() < (timeoutTime = getBufferRequestTimeoutTime()));
        throw new TimeoutException(
                String.format(
                        "Buffer request timeout, this means there is a fierce contention of"
                                + " the batch shuffle read memory, please increase '%s'.",
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key()));
    }

    private void failScheduledReaders(
            List<ScheduledSubpartitionReader> scheduledReaders, Throwable failureCause) {
        for (ScheduledSubpartitionReader scheduledReader : scheduledReaders) {
            synchronized (lock) {
                allScheduledReaders.remove(scheduledReader.getId());
            }
            scheduledReader.failReader(failureCause);
        }
    }

    /**
     * 归还内存
     * @param buffers
     */
    private void releaseBuffers(Queue<MemorySegment> buffers) {
        if (!buffers.isEmpty()) {
            try {
                bufferPool.recycle(buffers);
                buffers.clear();
            } catch (Throwable throwable) {
                // this should never happen so just trigger fatal error
                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                        Thread.currentThread(), throwable);
            }
        }
    }

    /**
     * 在一定延时后 重新触发 run
     */
    private void triggerScheduling() {
        synchronized (lock) {
            if (!isRunning
                    && !allScheduledReaders.isEmpty()
                    && numRequestedBuffers + bufferPool.getNumBuffersPerRequest()
                            <= maxRequestedBuffers
                    && numRequestedBuffers < bufferPool.getAverageBuffersPerRequester()) {
                isRunning = true;
                ioExecutor.execute(
                        () -> {
                            try {
                                run();
                            } catch (Throwable throwable) {
                                LOG.error("Failed to read data.", throwable);
                                // handle un-expected exception as unhandledExceptionHandler is not
                                // worked for ScheduledExecutorService.
                                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                                        Thread.currentThread(), throwable);
                            }
                        });
            }
        }
    }

    private long getBufferRequestTimeoutTime() {
        return bufferPool.getLastBufferOperationTimestamp() + bufferRequestTimeout.toMillis();
    }

    /**
     * The {@link ScheduledSubpartitionReader} is responsible for reading a subpartition from disk,
     * and is scheduled by the {@link DiskIOScheduler}.
     */
    private class ScheduledSubpartitionReader implements Comparable<ScheduledSubpartitionReader> {

        private final TieredStorageSubpartitionId subpartitionId;

        /**
         * 该对象包含一个队列  维护payload
         */
        private final NettyConnectionWriter nettyConnectionWriter;

        private int nextSegmentId = -1;

        private int nextBufferIndex;

        private long priority;

        private boolean isFailed;

        /**
         * 记录读取的进度
         */
        @Nullable private PartitionFileReader.ReadProgress readProgress;

        private ScheduledSubpartitionReader(
                TieredStorageSubpartitionId subpartitionId,
                NettyConnectionWriter nettyConnectionWriter) {
            this.subpartitionId = subpartitionId;
            this.nettyConnectionWriter = nettyConnectionWriter;
        }

        /**
         * 从磁盘读取数据
         * @param buffers
         * @param recycler
         * @throws IOException
         */
        private void loadDiskDataToBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
                throws IOException {

            if (isFailed) {
                throw new IOException(
                        "The scheduled subpartition reader for "
                                + subpartitionId
                                + " has already been failed.");
            }

            CompositeBuffer partialBuffer = null;
            boolean shouldContinueRead = true;
            try {
                while (!buffers.isEmpty() && shouldContinueRead && nextSegmentId >= 0) {
                    MemorySegment memorySegment = buffers.poll();
                    PartitionFileReader.ReadBufferResult readBufferResult;
                    try {
                        // 利用reader读取数据
                        readBufferResult =
                                partitionFileReader.readBuffer(
                                        partitionId,
                                        subpartitionId,
                                        nextSegmentId,
                                        nextBufferIndex,
                                        memorySegment,
                                        recycler,
                                        readProgress,
                                        partialBuffer);
                        if (readBufferResult == null) {
                            buffers.add(memorySegment);
                            break;
                        }
                    } catch (Throwable throwable) {
                        buffers.add(memorySegment);
                        throw throwable;
                    }

                    List<Buffer> readBuffers = readBufferResult.getReadBuffers();
                    shouldContinueRead = readBufferResult.continuousReadSuggested();
                    readProgress = readBufferResult.getReadProgress();
                    if (readBuffers.isEmpty()) {
                        buffers.add(memorySegment);
                        break;
                    }

                    // 解析出部分数据
                    partialBuffer = writeFullBuffersAndGetPartialBuffer(readBuffers);
                }
            } finally {
                if (partialBuffer != null) {
                    partialBuffer.recycleBuffer();
                }
            }
        }

        @Override
        public int compareTo(ScheduledSubpartitionReader reader) {
            checkArgument(reader != null);
            return Long.compare(getPriority(), reader.getPriority());
        }

        /**
         * 准备开始读取数据并传输了
         */
        private void prepareForScheduling() {
            if (nextSegmentId < 0) {
                updateSegmentId();
            }
            priority =
                    nextSegmentId < 0
                            ? Long.MAX_VALUE
                            : partitionFileReader.getPriority(
                                    partitionId,
                                    subpartitionId,
                                    nextSegmentId,
                                    nextBufferIndex,
                                    readProgress);
        }

        /**
         * 从buffer中读取完整数据 并解析出多出的数据
         * @param readBuffers
         * @return
         */
        private CompositeBuffer writeFullBuffersAndGetPartialBuffer(List<Buffer> readBuffers) {
            CompositeBuffer partialBuffer = null;
            for (int i = 0; i < readBuffers.size(); i++) {
                Buffer readBuffer = readBuffers.get(i);
                if (i == readBuffers.size() - 1 && isPartialBuffer(readBuffer)) {
                    partialBuffer = (CompositeBuffer) readBuffer;
                    continue;
                }
                writeNettyBufferAndUpdateSegmentId(readBuffer);
            }
            return partialBuffer;
        }

        private boolean isPartialBuffer(Buffer readBuffer) {
            return readBuffer instanceof CompositeBuffer
                    && ((CompositeBuffer) readBuffer).missingLength() > 0;
        }

        /**
         *
         * @param readBuffer   完整的数据
         */
        private void writeNettyBufferAndUpdateSegmentId(Buffer readBuffer) {
            writeToNettyConnectionWriter(
                    NettyPayload.newBuffer(
                            readBuffer, nextBufferIndex++, subpartitionId.getSubpartitionId()));

            // 当发现收到一个seg_end事件 更换seg
            if (readBuffer.getDataType() == Buffer.DataType.END_OF_SEGMENT) {
                nextSegmentId = -1;
                updateSegmentId();
            }
        }

        /**
         * 将数据包装成 payload 通过writer发送
         * @param nettyPayload
         */
        private void writeToNettyConnectionWriter(NettyPayload nettyPayload) {
            nettyConnectionWriter.writeNettyPayload(nettyPayload);

            // 首次添加数据 通知可用
            if (nettyConnectionWriter.numQueuedPayloads() <= 1
                    || nettyConnectionWriter.numQueuedBufferPayloads() <= 1) {
                notifyAvailable();
            }
        }

        private long getPriority() {
            return priority;
        }

        private void notifyAvailable() {
            nettyConnectionWriter.notifyAvailable();
        }

        private void failReader(Throwable failureCause) {
            if (isFailed) {
                return;
            }
            isFailed = true;
            nettyConnectionWriter.close(failureCause);
            nettyConnectionWriter.notifyAvailable();
        }

        /**
         * 更新seg
         */
        private void updateSegmentId() {
            // 每写入一个数据 nextBufferIndex 都会增加 使用最新的子分区id bufferIndex去查询segid
            Integer segmentId =
                    firstBufferIndexInSegmentRetriever.apply(
                            subpartitionId.getSubpartitionId(), nextBufferIndex);
            if (segmentId != null) {
                nextSegmentId = segmentId;
                // 写入新的seg 提示下游要切换seg了
                writeToNettyConnectionWriter(NettyPayload.newSegment(segmentId));
            }
        }

        private NettyConnectionId getId() {
            return nettyConnectionWriter.getNettyConnectionId();
        }
    }
}
