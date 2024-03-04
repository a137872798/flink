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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.ConsumeStatusWithId;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.SpillStatus;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.TreeMap;

/** A special implementation of {@link HsSpillingStrategy} that spilled all buffers to disk.
 * 有关倾倒策略的   当发生一些行为时 会产生一些相关的决定
 * */
public class HsFullSpillingStrategy implements HsSpillingStrategy {

    /**
     * buffer数量达到一定比率 触发倾倒
     */
    private final float numBuffersTriggerSpillingRatio;

    private final float releaseBufferRatio;

    private final float releaseThreshold;

    public HsFullSpillingStrategy(HybridShuffleConfiguration hybridShuffleConfiguration) {
        this.numBuffersTriggerSpillingRatio =
                hybridShuffleConfiguration.getFullStrategyNumBuffersTriggerSpillingRatio();
        this.releaseThreshold = hybridShuffleConfiguration.getFullStrategyReleaseThreshold();
        this.releaseBufferRatio = hybridShuffleConfiguration.getFullStrategyReleaseBufferRatio();
    }

    // For the case of buffer finished, whenever the number of unSpillBuffers reaches
    // numBuffersTriggerSpillingRatio times currentPoolSize, make a decision based on global
    // information. Otherwise, no need to take action.
    @Override
    public Optional<Decision> onBufferFinished(int numTotalUnSpillBuffers, int currentPoolSize) {
        // 表示当前buffer数 没有达到触发 spill的条件
        return numTotalUnSpillBuffers < numBuffersTriggerSpillingRatio * currentPoolSize
                ? Optional.of(Decision.NO_ACTION)
                : Optional.empty();
    }

    // For the case of buffer consumed, there is no need to take action for HsFullSpillingStrategy.
    @Override
    public Optional<Decision> onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        return Optional.of(Decision.NO_ACTION);
    }

    // When the amount of memory used exceeds the threshold, decide action based on global
    // information. Otherwise, no need to take action.
    @Override
    public Optional<Decision> onMemoryUsageChanged(
            int numTotalRequestedBuffers, int currentPoolSize) {
        // 请求的buffer量 没有达到要release的程度
        return numTotalRequestedBuffers < currentPoolSize * releaseThreshold
                ? Optional.of(Decision.NO_ACTION)
                : Optional.empty();
    }

    @Override
    public Decision decideActionWithGlobalInfo(HsSpillingInfoProvider spillingInfoProvider) {
        Decision.Builder builder = Decision.builder();
        // Save the cost of lock, if pool size is changed between checkSpill and checkRelease, pool
        // size checker will handle this inconsistency.
        int poolSize = spillingInfoProvider.getPoolSize();
        checkSpill(spillingInfoProvider, poolSize, builder);
        checkRelease(spillingInfoProvider, poolSize, builder);
        return builder.build();
    }

    @Override
    public Decision onResultPartitionClosed(HsSpillingInfoProvider spillingInfoProvider) {
        Decision.Builder builder = Decision.builder();
        for (int subpartitionId = 0;
                subpartitionId < spillingInfoProvider.getNumSubpartitions();
                subpartitionId++) {
            builder.addBufferToSpill(
                            subpartitionId,
                            // get all not start spilling buffers.
                            spillingInfoProvider.getBuffersInOrder(  // 根据条件检索一组buffer
                                    subpartitionId,
                                    SpillStatus.NOT_SPILL,
                                    ConsumeStatusWithId.ALL_ANY))
                    .addBufferToRelease(
                            subpartitionId,
                            // get all not released buffers.
                            spillingInfoProvider.getBuffersInOrder(
                                    subpartitionId, SpillStatus.ALL, ConsumeStatusWithId.ALL_ANY));
        }
        return builder.build();
    }

    /**
     * 检查是否要 spill
     * @param spillingInfoProvider
     * @param poolSize
     * @param builder
     */
    private void checkSpill(
            HsSpillingInfoProvider spillingInfoProvider, int poolSize, Decision.Builder builder) {
        if (spillingInfoProvider.getNumTotalUnSpillBuffers()
                < numBuffersTriggerSpillingRatio * poolSize) {
            // In case situation changed since onBufferFinished() returns Optional#empty()
            return;
        }
        // Spill all not spill buffers.  满足条件时 为每个子分区添加
        for (int i = 0; i < spillingInfoProvider.getNumSubpartitions(); i++) {
            builder.addBufferToSpill(
                    i,
                    spillingInfoProvider.getBuffersInOrder(  // 基于条件找到一组buffer
                            i, SpillStatus.NOT_SPILL, ConsumeStatusWithId.ALL_ANY));
        }
    }

    /**
     * Release subpartition's spilled buffer from head. Each subpartition fairly retains a fixed
     * number of buffers, and all the remaining buffers are released. If this subpartition does not
     * have so many qualified buffers, all of them will be retained.
     * 检查是否要释放
     */
    private void checkRelease(
            HsSpillingInfoProvider spillingInfoProvider, int poolSize, Decision.Builder builder) {

        // 不满足条件不需要处理
        if (spillingInfoProvider.getNumTotalRequestedBuffers() < poolSize * releaseThreshold) {
            // In case situation changed since onMemoryUsageChanged() returns Optional#empty()
            return;
        }

        int releaseNum = (int) (poolSize * releaseBufferRatio);
        int numSubpartitions = spillingInfoProvider.getNumSubpartitions();
        // 每个子分区要释放多少
        int expectedSubpartitionReleaseNum = releaseNum / numSubpartitions;
        TreeMap<Integer, Deque<BufferIndexAndChannel>> bufferToRelease = new TreeMap<>();

        for (int subpartitionId = 0; subpartitionId < numSubpartitions; subpartitionId++) {
            // 按照条件得到一组buffer
            Deque<BufferIndexAndChannel> buffersInOrder =
                    spillingInfoProvider.getBuffersInOrder(
                            subpartitionId, SpillStatus.SPILL, ConsumeStatusWithId.ALL_ANY);
            // if the number of subpartition spilling buffers less than expected release number,
            // release all of them.
            int subpartitionReleaseNum =
                    Math.min(buffersInOrder.size(), expectedSubpartitionReleaseNum);

            // 表示多出的buffer数量
            int subpartitionSurvivedNum = buffersInOrder.size() - subpartitionReleaseNum;
            // 将多出的buffer都释放掉
            while (subpartitionSurvivedNum-- != 0) {
                buffersInOrder.pollLast();
            }
            // 剩下的加入 bufferToRelease
            bufferToRelease.put(subpartitionId, buffersInOrder);
        }

        // collect results in order
        for (int i = 0; i < numSubpartitions; i++) {
            Deque<BufferIndexAndChannel> bufferIndexAndChannels = bufferToRelease.get(i);
            if (bufferIndexAndChannels != null && !bufferIndexAndChannels.isEmpty()) {
                builder.addBufferToRelease(i, bufferToRelease.getOrDefault(i, new ArrayDeque<>()));
            }
        }
    }
}
