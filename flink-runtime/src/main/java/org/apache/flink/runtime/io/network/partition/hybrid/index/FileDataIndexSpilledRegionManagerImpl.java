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

package org.apache.flink.runtime.io.network.partition.hybrid.index;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.function.BiConsumer;

/**
 * Default implementation of {@link FileDataIndexSpilledRegionManager}. This manager will handle and
 * spill regions in the following way:
 *
 * <ul>
 *   <li>All regions will be written to the same file, namely index file.
 *   <li>Multiple regions belonging to the same subpartition form a region group.
 *   <li>The regions in the same region group have no special relationship, but are only related to
 *       the order in which they are spilled.
 *   <li>Each region group is independent. Even if the previous region group is not full, the next
 *       region group can still be allocated.
 *   <li>If a region has been written to the index file already, spill it again will overwrite the
 *       previous region.
 *   <li>The very large region will monopolize a single region group.
 * </ul>
 *
 * <p>The relationships between index file and region group are shown below.
 *
 * <pre>
 *
 *         - - - - - - - - - Index File - - — - - - - - - - - -
 *        |                                                     |
 *        | - - — -RegionGroup1 - -   - - RegionGroup2- - - -   |
 *        ||SP1 R1｜｜SP1 R2｜ Free | |SP2 R3| SP2 R1| SP2 R2 |  |
 *        | - - - - - - - - - - - -   - - - - - - - - - - - -   |
 *        |                                                     |
 *        | - - - - - - - -RegionGroup3 - - - - -               |
 *        ||              Big Region             |              |
 *        | - - - - - - - - - - - - - - - - - - -               |
 *         - - - - - - - - - - - - - - - - - - - - - -- - - - -
 * </pre>
 */
public class FileDataIndexSpilledRegionManagerImpl<T extends FileDataIndexRegionHelper.Region>
        implements FileDataIndexSpilledRegionManager<T> {

    /**
     * List of subpartition's region group meta. Each element is a treeMap contains all {@link
     * RegionGroup}'s of specific subpartition corresponding to the subscript. The value of this
     * treeMap is a {@link RegionGroup}, and the key is minBufferIndex of this region group. Only
     * finished(i.e. no longer appended) region group will be put to here.
     */
    private final List<TreeMap<Integer, RegionGroup>> subpartitionFinishedRegionGroupMetas;

    /**
     * 对应索引文件
     */
    private FileChannel channel;

    /** The Offset of next region group, new region group will start from this offset. */
    private long nextRegionGroupOffset = 0L;

    private final long[] subpartitionCurrentOffset;

    /** Free space of every subpartition's current region group. */
    private final int[] subpartitionFreeSpaceInBytes;

    /** Metadata of every subpartition's current region group.
     * 每个子分区当前的regionGroup
     * */
    private final RegionGroup[] currentRegionGroup;

    /**
     * Default size of region group. If the size of a region is larger than this value, it will be
     * allocated and occupy a single region group.
     * 每个regionGroup的大小
     */
    private final int regionGroupSizeInBytes;

    /**
     * This consumer is used to load region to cache. The first parameter is subpartition id, and
     * second parameter is the region to load.
     */
    private final BiConsumer<Integer, T> cacheRegionConsumer;

    private final FileDataIndexRegionHelper<T> fileDataIndexRegionHelper;

    /**
     * When region in region group needs to be loaded to cache, whether to load all regions of the
     * entire region group.
     */
    private final boolean loadEntireRegionGroupToCache;

    public FileDataIndexSpilledRegionManagerImpl(
            int numSubpartitions,
            Path indexFilePath,
            int regionGroupSizeInBytes,
            long maxCacheCapacity,
            int regionHeaderSize,
            BiConsumer<Integer, T> cacheRegionConsumer,
            FileDataIndexRegionHelper<T> fileDataIndexRegionHelper) {
        try {
            // 打开索引文件
            this.channel =
                    FileChannel.open(
                            indexFilePath,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        this.loadEntireRegionGroupToCache =
                shouldLoadEntireRegionGroupToCache(
                        numSubpartitions,
                        regionGroupSizeInBytes,
                        maxCacheCapacity,
                        regionHeaderSize);
        this.subpartitionFinishedRegionGroupMetas = new ArrayList<>(numSubpartitions);
        this.subpartitionCurrentOffset = new long[numSubpartitions];
        this.subpartitionFreeSpaceInBytes = new int[numSubpartitions];
        this.currentRegionGroup = new RegionGroup[numSubpartitions];

        // 按照子分区数量 初始化结构
        for (int i = 0; i < numSubpartitions; i++) {
            subpartitionFinishedRegionGroupMetas.add(new TreeMap<>());
        }
        this.cacheRegionConsumer = cacheRegionConsumer;
        this.fileDataIndexRegionHelper = fileDataIndexRegionHelper;
        this.regionGroupSizeInBytes = regionGroupSizeInBytes;
    }

    /**
     * 查找某个region
     * @param subpartition the subpartition id that target region belong to.
     * @param bufferIndex the buffer index that target region contains.
     * @param loadToCache whether to load the found region into the cache.
     * @return
     */
    @Override
    public long findRegion(int subpartition, int bufferIndex, boolean loadToCache) {
        // first of all, find the region from current writing region group.
        RegionGroup regionGroup = currentRegionGroup[subpartition];

        // 首先尝试从currentRegionGroup中查找
        if (regionGroup != null) {
            // 尝试加载指定buffer  得到并返回
            long regionOffset =
                    findRegionInRegionGroup(subpartition, bufferIndex, regionGroup, loadToCache);
            if (regionOffset != -1) {
                return regionOffset;
            }
        }

        // next, find the region from finished region groups.
        TreeMap<Integer, RegionGroup> subpartitionRegionGroupMetaTreeMap =
                subpartitionFinishedRegionGroupMetas.get(subpartition);
        // all region groups with a minBufferIndex less than or equal to this target buffer index
        // may contain the target region.
        // 从所有regionGroup中查找
        for (RegionGroup meta :
                subpartitionRegionGroupMetaTreeMap.headMap(bufferIndex, true).values()) {
            long regionOffset =
                    findRegionInRegionGroup(subpartition, bufferIndex, meta, loadToCache);
            if (regionOffset != -1) {
                return regionOffset;
            }
        }
        return -1;
    }

    /**
     * 查找某个regionGroup下某个buffer数据
     * @param subpartition
     * @param bufferIndex
     * @param meta
     * @param loadToCache
     * @return
     */
    private long findRegionInRegionGroup(
            int subpartition, int bufferIndex, RegionGroup meta, boolean loadToCache) {
        if (bufferIndex <= meta.getMaxBufferIndex()) {
            try {
                return readRegionGroupAndLoadToCacheIfNeeded(
                        subpartition, bufferIndex, meta, loadToCache);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // -1 indicates that target region is not founded from this region group
        return -1;
    }

    /**
     * 读取数据
     * @param subpartition
     * @param bufferIndex
     * @param meta
     * @param loadToCache
     * @return
     * @throws IOException
     */
    private long readRegionGroupAndLoadToCacheIfNeeded(
            int subpartition, int bufferIndex, RegionGroup meta, boolean loadToCache)
            throws IOException {
        // read all regions belong to this region group.
        // 加载该group下 所有的region数据
        List<Tuple2<T, Long>> regionAndOffsets =
                readRegionGroup(meta.getOffset(), meta.getNumRegions());
        // -1 indicates that target region is not founded from this region group.
        long targetRegionOffset = -1;
        T targetRegion = null;
        // traverse all regions to find target.
        Iterator<Tuple2<T, Long>> it = regionAndOffsets.iterator();
        while (it.hasNext()) {
            Tuple2<T, Long> regionAndOffset = it.next();
            T region = regionAndOffset.f0;
            // whether the region contains this buffer.
            // 检查该region是否包含 index对应的buffer数据
            if (region.containBuffer(bufferIndex)) {
                // target region is founded.
                targetRegion = region;
                // 记录偏移量
                targetRegionOffset = regionAndOffset.f1;
                it.remove();
            }
        }

        // target region is founded and need to load to cache.
        if (targetRegion != null && loadToCache) {
            if (loadEntireRegionGroupToCache) {
                // first of all, load all regions except target to cache.
                regionAndOffsets.forEach(
                        (regionAndOffsetTuple) ->
                                cacheRegionConsumer.accept(subpartition, regionAndOffsetTuple.f0));
                // load target region to cache in the end, this is to prevent the target
                // from being eliminated.
                cacheRegionConsumer.accept(subpartition, targetRegion);
            } else {
                // only load target region to cache.
                cacheRegionConsumer.accept(subpartition, targetRegion);
            }
        }
        // return the offset of target region.
        return targetRegionOffset;
    }

    @Override
    public void appendOrOverwriteRegion(int subpartition, T newRegion) throws IOException {
        // This method will only be called when we want to eliminate a region. We can't let the
        // region be reloaded into the cache, otherwise it will lead to an infinite loop.
        long oldRegionOffset = findRegion(subpartition, newRegion.getFirstBufferIndex(), false);
        if (oldRegionOffset != -1) {
            // if region is already exists in file, overwrite it.
            // 表示覆盖操作
            writeRegionToOffset(oldRegionOffset, newRegion);
        } else {
            // otherwise, append region to region group.
            // 添加数据
            appendRegion(subpartition, newRegion);
        }
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }

    private static boolean shouldLoadEntireRegionGroupToCache(
            int numSubpartitions,
            int regionGroupSizeInBytes,
            long maxCacheCapacity,
            int regionHeaderSize) {
        // If the cache can put at least two region groups (one for reading and one for writing) for
        // each subpartition, it is reasonable to load the entire region group into memory, which
        // can improve the cache hit rate. On the contrary, if the cache capacity is small, loading
        // a large number of regions will lead to performance degradation,only the target region
        // should be loaded.
        return ((long) 2 * numSubpartitions * regionGroupSizeInBytes) / regionHeaderSize
                <= maxCacheCapacity;
    }

    /**
     * 往某个子分区添加一个region
     * @param subpartition
     * @param region
     * @throws IOException
     */
    private void appendRegion(int subpartition, T region) throws IOException {
        int regionSize = region.getSize();
        // check whether we have enough space to append this region.
        // 空间不足
        if (subpartitionFreeSpaceInBytes[subpartition] < regionSize) {
            // No enough free space, start a new region group. Note that if region is larger than
            // region group's size, this will start a new region group only contains the big region.
            // 创建新region
            startNewRegionGroup(subpartition, Math.max(regionSize, regionGroupSizeInBytes));
        }
        // spill this region to current offset of file index.
        writeRegionToOffset(subpartitionCurrentOffset[subpartition], region);
        // a new region was appended to region group, update it.
        // 将数据写入regionGroup
        updateRegionGroup(subpartition, region);
    }

    /**
     * 将channel数据写入文件
     * @param offset
     * @param region
     * @throws IOException
     */
    private void writeRegionToOffset(long offset, T region) throws IOException {
        channel.position(offset);
        fileDataIndexRegionHelper.writeRegionToFile(channel, region);
    }

    /**
     * 创建一个新的regionGroup
     * @param subpartition
     * @param newRegionGroupSize
     */
    private void startNewRegionGroup(int subpartition, int newRegionGroupSize) {
        RegionGroup oldRegionGroup = currentRegionGroup[subpartition];
        // 更新当前使用的regionGroup
        currentRegionGroup[subpartition] = new RegionGroup(nextRegionGroupOffset);
        // 更新当前偏移量
        subpartitionCurrentOffset[subpartition] = nextRegionGroupOffset;
        nextRegionGroupOffset += newRegionGroupSize;
        // 更新当前可用空间
        subpartitionFreeSpaceInBytes[subpartition] = newRegionGroupSize;
        if (oldRegionGroup != null) {
            // put the finished region group to subpartitionFinishedRegionGroupMetas.
            // 被替换后 才加入tree中
            subpartitionFinishedRegionGroupMetas
                    .get(subpartition)
                    .put(oldRegionGroup.minBufferIndex, oldRegionGroup);
        }
    }

    /**
     * 更新regionGroup数据
     * @param subpartition
     * @param region
     */
    private void updateRegionGroup(int subpartition, T region) {
        int regionSize = region.getSize();
        subpartitionFreeSpaceInBytes[subpartition] -= regionSize;
        subpartitionCurrentOffset[subpartition] += regionSize;
        RegionGroup regionGroup = currentRegionGroup[subpartition];
        // 往group中追加一个region
        regionGroup.addRegion(
                region.getFirstBufferIndex(),
                region.getFirstBufferIndex() + region.getNumBuffers() - 1);
    }

    /**
     * Read region group from index file.
     *
     * @param offset offset of this region group.
     * @param numRegions number of regions of this region group.
     * @return List of all regions and its offset belong to this region group.
     * 从文件中加载数据
     */
    private List<Tuple2<T, Long>> readRegionGroup(long offset, int numRegions) throws IOException {
        List<Tuple2<T, Long>> regionAndOffsets = new ArrayList<>();
        // 因为本次针对的是一个regionGroup 所以挨个读取region的数据
        for (int i = 0; i < numRegions; i++) {
            T region = fileDataIndexRegionHelper.readRegionFromFile(channel, offset);
            regionAndOffsets.add(Tuple2.of(region, offset));
            // 推进偏移量
            offset += region.getSize();
        }
        return regionAndOffsets;
    }

    /**
     * Metadata of spilled regions region group. When a region group is finished(i.e. no longer
     * appended), its corresponding {@link RegionGroup} becomes immutable.
     * 多个region被称为  regionGroup
     */
    private static class RegionGroup {

        // 内部buffer 最小索引和最大索引  同时还有region的数量 和该regionGroup的偏移量

        /**
         * Minimum buffer index of this region group. It is the smallest bufferIndex(inclusive) in
         * all regions belong to this region group.
         */
        private int minBufferIndex;

        /**
         * Maximum buffer index of this region group. It is the largest bufferIndex(inclusive) in
         * all regions belong to this region group.
         */
        private int maxBufferIndex;

        /** Number of regions belong to this region group. */
        private int numRegions;

        /** The index file offset of this region group. */
        private final long offset;

        public RegionGroup(long offset) {
            this.offset = offset;
            this.minBufferIndex = Integer.MAX_VALUE;
            this.maxBufferIndex = 0;
            this.numRegions = 0;
        }

        public int getMaxBufferIndex() {
            return maxBufferIndex;
        }

        public long getOffset() {
            return offset;
        }

        public int getNumRegions() {
            return numRegions;
        }

        /**
         * 表示添加了一个新的region  尝试更新index 和数量
         * @param firstBufferIndexOfRegion
         * @param maxBufferIndexOfRegion
         */
        public void addRegion(int firstBufferIndexOfRegion, int maxBufferIndexOfRegion) {
            if (firstBufferIndexOfRegion < minBufferIndex) {
                this.minBufferIndex = firstBufferIndexOfRegion;
            }
            if (maxBufferIndexOfRegion > maxBufferIndex) {
                this.maxBufferIndex = maxBufferIndexOfRegion;
            }
            this.numRegions++;
        }
    }

    /** Factory of {@link FileDataIndexSpilledRegionManager}.
     * 创建regionManager的工厂
     * */
    public static class Factory<T extends FileDataIndexRegionHelper.Region>
            implements FileDataIndexSpilledRegionManager.Factory<T> {

        /**
         * 每个regionGroup的大小
         */
        private final int regionGroupSizeInBytes;

        private final long maxCacheCapacity;

        /**
         * 每个region的header大小
         */
        private final int regionHeaderSize;

        /**
         * 该对象包含读写逻辑
         */
        private final FileDataIndexRegionHelper<T> fileDataIndexRegionHelper;

        public Factory(
                int regionGroupSizeInBytes,
                long maxCacheCapacity,
                int regionHeaderSize,
                FileDataIndexRegionHelper<T> fileDataIndexRegionHelper) {
            this.regionGroupSizeInBytes = regionGroupSizeInBytes;
            this.maxCacheCapacity = maxCacheCapacity;
            this.regionHeaderSize = regionHeaderSize;
            this.fileDataIndexRegionHelper = fileDataIndexRegionHelper;
        }

        @Override
        public FileDataIndexSpilledRegionManager<T> create(
                int numSubpartitions,
                Path indexFilePath,
                BiConsumer<Integer, T> cacheRegionConsumer) {
            return new FileDataIndexSpilledRegionManagerImpl<>(
                    numSubpartitions,
                    indexFilePath,
                    regionGroupSizeInBytes,
                    maxCacheCapacity,
                    regionHeaderSize,
                    cacheRegionConsumer,
                    fileDataIndexRegionHelper);
        }
    }
}
