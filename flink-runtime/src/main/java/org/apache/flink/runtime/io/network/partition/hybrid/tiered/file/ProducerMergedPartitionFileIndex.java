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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.runtime.io.network.partition.hybrid.index.FileDataIndexCache;
import org.apache.flink.runtime.io.network.partition.hybrid.index.FileDataIndexRegionHelper;
import org.apache.flink.runtime.io.network.partition.hybrid.index.FileDataIndexSpilledRegionManagerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.index.FileRegionWriteReadUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.hybrid.index.FileRegionWriteReadUtils.allocateAndConfigureBuffer;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link ProducerMergedPartitionFileIndex} is used by {@link ProducerMergedPartitionFileWriter}
 * and {@link ProducerMergedPartitionFileReader}, to maintain the offset of each buffer in the
 * physical file.
 *
 * <p>For efficiency, buffers from the same subpartition that are both logically (i.e. index in the
 * subpartition) and physically (i.e. offset in the file) consecutive are combined into a {@link
 * FixedSizeRegion}.
 *
 * <pre>For example, the following buffers (indicated by subpartitionId-bufferIndex):
 *   1-1, 1-2, 1-3, 2-1, 2-2, 2-5, 1-4, 1-5, 2-6
 * will be combined into 5 regions (separated by '|'):
 *   1-1, 1-2, 1-3 | 2-1, 2-2 | 2-5 | 1-4, 1-5 | 2-6
 * </pre>
 * 存储分区数据的文件
 */
public class ProducerMergedPartitionFileIndex {

    private final Path indexFilePath;

    /**
     * The regions belonging to each subpartitions.
     *
     * <p>Note that the field can be accessed by the writing and reading IO thread, so the lock is
     * to ensure the thread safety.
     * 缓存region的元数据信息
     */
    @GuardedBy("lock")
    private final FileDataIndexCache<FixedSizeRegion> indexCache;

    private final Object lock = new Object();

    public ProducerMergedPartitionFileIndex(
            int numSubpartitions,
            Path indexFilePath,
            int regionGroupSizeInBytes,
            long numRetainedInMemoryRegionsMax) {
        this.indexFilePath = indexFilePath;
        // 产生了缓存对象
        this.indexCache =
                new FileDataIndexCache<>(
                        numSubpartitions,
                        indexFilePath,
                        numRetainedInMemoryRegionsMax,
                        new FileDataIndexSpilledRegionManagerImpl.Factory<>(
                                regionGroupSizeInBytes,
                                numRetainedInMemoryRegionsMax,
                                FixedSizeRegion.REGION_SIZE,
                                ProducerMergedPartitionFileDataIndexRegionHelper.INSTANCE));
    }

    /**
     * Add buffers to the index.
     *
     * @param buffers to be added. Note, the provided buffers are required to be physically
     *     consecutive and in the same order as in the file.
     */
    void addBuffers(List<FlushedBuffer> buffers) {
        if (buffers.isEmpty()) {
            return;
        }

        Map<Integer, List<FixedSizeRegion>> convertedRegions = convertToRegions(buffers);
        synchronized (lock) {
            convertedRegions.forEach(indexCache::put);
        }
    }

    /**
     * Get the subpartition's {@link FixedSizeRegion} containing the specific buffer index.
     *
     * @param subpartitionId the subpartition id
     * @param bufferIndex the buffer index
     * @return the region containing the buffer index, or return emtpy if the region is not found.
     */
    Optional<FixedSizeRegion> getRegion(
            TieredStorageSubpartitionId subpartitionId, int bufferIndex) {
        synchronized (lock) {
            return indexCache.get(subpartitionId.getSubpartitionId(), bufferIndex);
        }
    }

    void release() {
        synchronized (lock) {
            try {
                indexCache.close();
                IOUtils.deleteFileQuietly(indexFilePath);
            } catch (IOException e) {
                ExceptionUtils.rethrow(e);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    /**
     * 当一组buffer已经完成刷盘时  构建索引信息
     * @param buffers
     * @return
     */
    private static Map<Integer, List<FixedSizeRegion>> convertToRegions(
            List<FlushedBuffer> buffers) {
        Map<Integer, List<FixedSizeRegion>> subpartitionRegionMap = new HashMap<>();
        Iterator<FlushedBuffer> iterator = buffers.iterator();
        FlushedBuffer firstBufferInRegion = iterator.next();
        FlushedBuffer lastBufferInRegion = firstBufferInRegion;

        while (iterator.hasNext()) {
            // 遍历每个 FlushedBuffer
            FlushedBuffer currentBuffer = iterator.next();
            if (currentBuffer.getSubpartitionId() != firstBufferInRegion.getSubpartitionId()
                    || currentBuffer.getBufferIndex() != lastBufferInRegion.getBufferIndex() + 1) {
                // The current buffer belongs to a new region, add the current region to the map
                // buffer可能会不连续  这样就会产生多个FixedSizeRegion  然后仅当buffer不连续或者子分区变化 才触发一次写入
                addRegionToMap(firstBufferInRegion, lastBufferInRegion, subpartitionRegionMap);

                firstBufferInRegion = currentBuffer;
            }
            lastBufferInRegion = currentBuffer;
        }

        // Add the last region to the map
        addRegionToMap(firstBufferInRegion, lastBufferInRegion, subpartitionRegionMap);
        return subpartitionRegionMap;
    }

    /**
     *
     * @param firstBufferInRegion  表示region下第一个buffer
     * @param lastBufferInRegion   上一个buffer
     * @param subpartitionRegionMap
     */
    private static void addRegionToMap(
            FlushedBuffer firstBufferInRegion,
            FlushedBuffer lastBufferInRegion,
            Map<Integer, List<FixedSizeRegion>> subpartitionRegionMap) {
        checkArgument(
                firstBufferInRegion.getSubpartitionId() == lastBufferInRegion.getSubpartitionId());
        checkArgument(firstBufferInRegion.getBufferIndex() <= lastBufferInRegion.getBufferIndex());

        subpartitionRegionMap
                .computeIfAbsent(firstBufferInRegion.getSubpartitionId(), ArrayList::new)
                .add(
                        // 为每个子分区添加region信息    每个FixedSizeRegion 对应一个region信息 然后又有regionGroup的概念
                        new FixedSizeRegion(
                                firstBufferInRegion.getBufferIndex(),
                                firstBufferInRegion.getFileOffset(),
                                lastBufferInRegion.getFileOffset()
                                        + lastBufferInRegion.getBufferSizeBytes(),
                                lastBufferInRegion.getBufferIndex()
                                        - firstBufferInRegion.getBufferIndex()
                                        + 1));
    }

    // ------------------------------------------------------------------------
    //  Internal Classes
    // ------------------------------------------------------------------------

    /** Represents a buffer to be flushed.
     * 维护一些刷盘信息
     * */
    static class FlushedBuffer {
        /** The subpartition id that the buffer belongs to. */
        private final int subpartitionId;

        /** The buffer index within the subpartition. */
        private final int bufferIndex;

        // buffer在文件的位置  以及buffer内的数据长度

        /** The file offset that the buffer begin with. */
        private final long fileOffset;

        private final long bufferSizeBytes;

        FlushedBuffer(int subpartitionId, int bufferIndex, long fileOffset, long bufferSizeBytes) {
            this.subpartitionId = subpartitionId;
            this.bufferIndex = bufferIndex;
            this.fileOffset = fileOffset;
            this.bufferSizeBytes = bufferSizeBytes;
        }

        int getSubpartitionId() {
            return subpartitionId;
        }

        int getBufferIndex() {
            return bufferIndex;
        }

        long getFileOffset() {
            return fileOffset;
        }

        long getBufferSizeBytes() {
            return bufferSizeBytes;
        }
    }

    /**
     * The implementation of {@link FileDataIndexRegionHelper} to writing a region to the file or
     * reading a region from the file.
     *
     * <p>Note that this type of region's length is fixed.
     * 提供写入/读取 文件数据的能力
     */
    static class ProducerMergedPartitionFileDataIndexRegionHelper
            implements FileDataIndexRegionHelper<FixedSizeRegion> {

        /** Reusable buffer used to read and write the immutable part of region. */
        private final ByteBuffer regionBuffer =
                allocateAndConfigureBuffer(FixedSizeRegion.REGION_SIZE);

        static final ProducerMergedPartitionFileDataIndexRegionHelper INSTANCE =
                new ProducerMergedPartitionFileDataIndexRegionHelper();

        private ProducerMergedPartitionFileDataIndexRegionHelper() {}

        @Override
        public void writeRegionToFile(FileChannel channel, FixedSizeRegion region)
                throws IOException {
            FileRegionWriteReadUtils.writeFixedSizeRegionToFile(channel, regionBuffer, region);
        }

        @Override
        public FixedSizeRegion readRegionFromFile(FileChannel channel, long fileOffset)
                throws IOException {
            return FileRegionWriteReadUtils.readFixedSizeRegionFromFile(
                    channel, regionBuffer, fileOffset);
        }
    }

    /**
     * Represents a series of buffers that are:
     *
     * <ul>
     *   <li>From the same subpartition
     *   <li>Logically (i.e. buffer index) consecutive
     *   <li>Physically (i.e. offset in the file) consecutive
     * </ul>
     *
     * <p>Note that the region has a fixed size.
     * 描述region信息
     */
    public static class FixedSizeRegion implements FileDataIndexRegionHelper.Region {

        public static final int REGION_SIZE =
                Integer.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES;

        // 维护buffer的下标

        /** The buffer index of first buffer. */
        private final int firstBufferIndex;

        /** The file offset of the region. */
        private final long regionStartOffset;

        private final long regionEndOffset;

        /** The number of buffers that the region contains. */
        private final int numBuffers;

        public FixedSizeRegion(
                int firstBufferIndex,
                long regionStartOffset,
                long regionEndOffset,
                int numBuffers) {
            this.firstBufferIndex = firstBufferIndex;
            this.regionStartOffset = regionStartOffset;
            this.regionEndOffset = regionEndOffset;
            this.numBuffers = numBuffers;
        }

        @Override
        public boolean containBuffer(int bufferIndex) {
            return bufferIndex >= firstBufferIndex && bufferIndex < firstBufferIndex + numBuffers;
        }

        /** Get the total size in bytes of this region, including the fields and the buffers. */
        @Override
        public int getSize() {
            return REGION_SIZE + numBuffers;
        }

        @Override
        public long getRegionStartOffset() {
            return regionStartOffset;
        }

        @Override
        public long getRegionEndOffset() {
            return regionEndOffset;
        }

        @Override
        public int getNumBuffers() {
            return numBuffers;
        }

        @Override
        public int getFirstBufferIndex() {
            return firstBufferIndex;
        }
    }
}
