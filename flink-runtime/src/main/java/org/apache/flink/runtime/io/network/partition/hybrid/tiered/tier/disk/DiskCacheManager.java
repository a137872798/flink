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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.util.concurrent.FutureUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link DiskCacheManager} is responsible for managing cached buffers before flushing to files.
 * 管理多个子分区的数据
 */
class DiskCacheManager {

    private final TieredStoragePartitionId partitionId;

    /**
     * 该分区下有多少子分区
     */
    private final int numSubpartitions;

    /**
     * 在触发刷盘前 最多允许维护多少byte
     */
    private final int maxCachedBytesBeforeFlush;

    /**
     * 该对象用于写入数据
     */
    private final PartitionFileWriter partitionFileWriter;

    /**
     * 每个对象对应一个子分区
     */
    private final SubpartitionDiskCacheManager[] subpartitionCacheManagers;

    /** Whether the current flush process has completed. */
    private CompletableFuture<Void> hasFlushCompleted;

    /**
     * The number of all subpartition's cached bytes in the cache manager. Note that the counter can
     * only be accessed by the task thread and does not require locks.
     */
    private int numCachedBytesCounter;

    DiskCacheManager(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int maxCachedBytesBeforeFlush,
            TieredStorageMemoryManager memoryManager,
            PartitionFileWriter partitionFileWriter) {
        this.partitionId = partitionId;
        this.numSubpartitions = numSubpartitions;
        this.maxCachedBytesBeforeFlush = maxCachedBytesBeforeFlush;
        this.partitionFileWriter = partitionFileWriter;
        this.subpartitionCacheManagers = new SubpartitionDiskCacheManager[numSubpartitions];
        this.hasFlushCompleted = FutureUtils.completedVoidFuture();

        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionCacheManagers[subpartitionId] = new SubpartitionDiskCacheManager();
        }
        memoryManager.listenBufferReclaimRequest(this::notifyFlushCachedBuffers);
    }

    // ------------------------------------------------------------------------
    //  Called by DiskTierProducerAgent
    // ------------------------------------------------------------------------

    /**
     * 找到对应的子分区 并更新seg
     * @param subpartitionId
     * @param segmentIndex
     */
    void startSegment(int subpartitionId, int segmentIndex) {
        subpartitionCacheManagers[subpartitionId].startSegment(segmentIndex);
    }

    /**
     * Append buffer to {@link DiskCacheManager}.
     *
     * @param buffer to be managed by this class.
     * @param subpartitionId the subpartition of this record.
     *                       找到对应的子分区 追加buffer
     */
    void append(Buffer buffer, int subpartitionId) {
        subpartitionCacheManagers[subpartitionId].append(buffer);
        increaseNumCachedBytesAndCheckFlush(buffer.readableBytes());
    }

    /**
     * Append the end-of-segment event to {@link DiskCacheManager}, which indicates the segment has
     * finished.
     *
     * @param record the end-of-segment event
     * @param subpartitionId target subpartition of this record.
     */
    void appendEndOfSegmentEvent(ByteBuffer record, int subpartitionId) {
        subpartitionCacheManagers[subpartitionId].appendEndOfSegmentEvent(record);
        increaseNumCachedBytesAndCheckFlush(record.remaining());
    }

    /**
     * Return the current buffer index.
     *
     * @param subpartitionId the target subpartition id
     * @return the finished buffer index
     * 返回当前buffer对应的下标
     */
    int getBufferIndex(int subpartitionId) {
        return subpartitionCacheManagers[subpartitionId].getBufferIndex();
    }

    /** Close this {@link DiskCacheManager}, it means no data can append to memory. */
    void close() {
        forceFlushCachedBuffers();
    }

    /**
     * Release this {@link DiskCacheManager}, it means all memory taken by this class will recycle.
     */
    void release() {
        Arrays.stream(subpartitionCacheManagers).forEach(SubpartitionDiskCacheManager::release);
        partitionFileWriter.release();
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void increaseNumCachedBytesAndCheckFlush(int numIncreasedCachedBytes) {
        numCachedBytesCounter += numIncreasedCachedBytes;
        // 当缓存的量达到一定值 强制触发刷盘
        if (numCachedBytesCounter > maxCachedBytesBeforeFlush) {
            forceFlushCachedBuffers();
        }
    }

    /**
     * 当收到回收buffer的请求时 触发flush
     */
    private void notifyFlushCachedBuffers() {
        flushBuffers(false);
    }

    private void forceFlushCachedBuffers() {
        flushBuffers(true);
    }

    /**
     * Note that the request of flushing buffers may come from the disk check thread or the task
     * thread, so the method itself should ensure the thread safety.
     * 刷盘并释放buffer
     */
    private synchronized void flushBuffers(boolean forceFlush) {
        // 表示上次刷盘还未完成
        if (!forceFlush && !hasFlushCompleted.isDone()) {
            return;
        }
        List<PartitionFileWriter.SubpartitionBufferContext> buffersToFlush = new ArrayList<>();
        // 找到需要刷盘的数据
        int numToWriteBuffers = getSubpartitionToFlushBuffers(buffersToFlush);

        if (numToWriteBuffers > 0) {
            CompletableFuture<Void> flushCompletableFuture =
                    partitionFileWriter.write(partitionId, buffersToFlush);
            if (!forceFlush) {
                hasFlushCompleted = flushCompletableFuture;
            }
        }
        numCachedBytesCounter = 0;
    }

    /**
     * 找到需要刷盘的数据
     * @param buffersToFlush
     * @return
     */
    private int getSubpartitionToFlushBuffers(
            List<PartitionFileWriter.SubpartitionBufferContext> buffersToFlush) {
        int numToWriteBuffers = 0;
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; subpartitionId++) {

            // 取出缓存在内存的数据
            List<Tuple2<Buffer, Integer>> bufferWithIndexes =
                    subpartitionCacheManagers[subpartitionId].removeAllBuffers();
            buffersToFlush.add(
                    // 产生上下文信息
                    new PartitionFileWriter.SubpartitionBufferContext(
                            subpartitionId,
                            Collections.singletonList(
                                    new PartitionFileWriter.SegmentBufferContext(
                                            subpartitionCacheManagers[subpartitionId]
                                                    .getSegmentId(),
                                            bufferWithIndexes,
                                            false))));
            numToWriteBuffers += bufferWithIndexes.size();
        }
        return numToWriteBuffers;
    }
}
