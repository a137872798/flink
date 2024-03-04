/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.scheduler.SchedulingTopologyListener;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Releases blocking intermediate result partitions that are incident to a {@link
 * SchedulingPipelinedRegion}, as soon as the region's execution vertices are finished.
 *
 * 当某个顶点结束/未结束时  会触发相应的动作
 */
public class RegionPartitionGroupReleaseStrategy
        implements PartitionGroupReleaseStrategy, SchedulingTopologyListener {

    /**
     * 内部包含一个拓扑图
     */
    private final SchedulingTopology schedulingTopology;

    /**
     * 每个顶点关联一个 流水线执行图   多个顶点对应一个流水线
     */
    private final Map<ExecutionVertexID, PipelinedRegionExecutionView> regionExecutionViewByVertex =
            new HashMap<>();

    /**
     * 然后一个数据集可以被多个流水线消费  这些流水线就构成了 ConsumerRegionGroupExecutionView
     */
    private final Map<ConsumedPartitionGroup, ConsumerRegionGroupExecutionView>
            partitionGroupConsumerRegions = new HashMap<>();

    /**
     * 方便操作的对象
     */
    private final ConsumerRegionGroupExecutionViewMaintainer
            consumerRegionGroupExecutionViewMaintainer;

    public RegionPartitionGroupReleaseStrategy(final SchedulingTopology schedulingTopology) {
        this.schedulingTopology = checkNotNull(schedulingTopology);
        this.consumerRegionGroupExecutionViewMaintainer =
                new ConsumerRegionGroupExecutionViewMaintainer();

        // 本对象作为监听器 进行注册
        schedulingTopology.registerSchedulingTopologyListener(this);
        notifySchedulingTopologyUpdatedInternal(schedulingTopology.getAllPipelinedRegions());
    }

    /**
     * 填充容器
     * @param newRegions
     */
    private void initRegionExecutionViewByVertex(
            Iterable<? extends SchedulingPipelinedRegion> newRegions) {
        for (SchedulingPipelinedRegion pipelinedRegion : newRegions) {

            // 使用流水线初始化view
            final PipelinedRegionExecutionView regionExecutionView =
                    new PipelinedRegionExecutionView(pipelinedRegion);
            // 建立关联关系
            for (SchedulingExecutionVertex executionVertexId : pipelinedRegion.getVertices()) {
                regionExecutionViewByVertex.put(executionVertexId.getId(), regionExecutionView);
            }
        }
    }

    /**
     * 初始化数据集相关的
     * @param newRegions
     * @return
     */
    private Iterable<ConsumerRegionGroupExecutionView> initPartitionGroupConsumerRegions(
            Iterable<? extends SchedulingPipelinedRegion> newRegions) {

        final List<ConsumerRegionGroupExecutionView> newConsumerRegionGroups = new ArrayList<>();

        for (SchedulingPipelinedRegion region : newRegions) {
            // 获取每个流水线要消费的数据集
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    region.getAllReleaseBySchedulerConsumedPartitionGroups()) {
                partitionGroupConsumerRegions
                        .computeIfAbsent(
                                consumedPartitionGroup,
                                g -> {
                                    // 同样一个数据集可能被多个流水线消费
                                    ConsumerRegionGroupExecutionView regionGroup =
                                            new ConsumerRegionGroupExecutionView();
                                    newConsumerRegionGroups.add(regionGroup);
                                    return regionGroup;
                                })
                        .add(region);
            }
        }

        return newConsumerRegionGroups;
    }

    /**
     * 表示某个顶点处理完了
     * @param finishedVertex Id of the vertex that finished the execution  表示一个消费中间数据集的对象
     * @return
     */
    @Override
    public List<ConsumedPartitionGroup> vertexFinished(final ExecutionVertexID finishedVertex) {
        final PipelinedRegionExecutionView regionExecutionView =
                getPipelinedRegionExecutionViewForVertex(finishedVertex);
        regionExecutionView.vertexFinished(finishedVertex);

        // 该流水线的全部顶点处理完了
        if (regionExecutionView.isFinished()) {
            final SchedulingPipelinedRegion pipelinedRegion =
                    schedulingTopology.getPipelinedRegionOfVertex(finishedVertex);
            // 表示该流水线处理完了
            consumerRegionGroupExecutionViewMaintainer.regionFinished(pipelinedRegion);

            // 返回借由该顶点完成影响到的被消费的数据集
            return filterReleasablePartitionGroups(
                    pipelinedRegion.getAllReleaseBySchedulerConsumedPartitionGroups());
        }
        return Collections.emptyList();
    }

    /**
     * 表示某个顶点未完成
     * @param executionVertexId
     */
    @Override
    public void vertexUnfinished(final ExecutionVertexID executionVertexId) {
        final PipelinedRegionExecutionView regionExecutionView =
                getPipelinedRegionExecutionViewForVertex(executionVertexId);
        regionExecutionView.vertexUnfinished(executionVertexId);

        // 表示该流水线未完成
        final SchedulingPipelinedRegion pipelinedRegion =
                schedulingTopology.getPipelinedRegionOfVertex(executionVertexId);
        consumerRegionGroupExecutionViewMaintainer.regionUnfinished(pipelinedRegion);
    }

    private PipelinedRegionExecutionView getPipelinedRegionExecutionViewForVertex(
            final ExecutionVertexID executionVertexId) {
        final PipelinedRegionExecutionView pipelinedRegionExecutionView =
                regionExecutionViewByVertex.get(executionVertexId);
        checkState(
                pipelinedRegionExecutionView != null,
                "PipelinedRegionExecutionView not found for execution vertex %s",
                executionVertexId);
        return pipelinedRegionExecutionView;
    }

    /**
     *
     * @param consumedPartitionGroups
     * @return
     */
    private List<ConsumedPartitionGroup> filterReleasablePartitionGroups(
            final Iterable<ConsumedPartitionGroup> consumedPartitionGroups) {

        final List<ConsumedPartitionGroup> releasablePartitionGroups = new ArrayList<>();

        for (ConsumedPartitionGroup consumedPartitionGroup : consumedPartitionGroups) {
            final ConsumerRegionGroupExecutionView consumerRegionGroup =
                    partitionGroupConsumerRegions.get(consumedPartitionGroup);
            // 该消费组已经处理完了
            if (consumerRegionGroup.isFinished()
                    // 并且是非持久化模式
                    && !consumedPartitionGroup.getResultPartitionType().isPersistent()) {
                // At present, there's only one ConsumerVertexGroup for each
                // ConsumedPartitionGroup, so if a ConsumedPartitionGroup is fully consumed, all
                // its partitions are releasable.
                releasablePartitionGroups.add(consumedPartitionGroup);
            }
        }

        return releasablePartitionGroups;
    }

    /**
     * 设置所有流水线
     * @param newRegions
     */
    private void notifySchedulingTopologyUpdatedInternal(
            Iterable<? extends SchedulingPipelinedRegion> newRegions) {

        // 初始化流水线
        initRegionExecutionViewByVertex(newRegions);

        // 初始化数据集
        Iterable<ConsumerRegionGroupExecutionView> newConsumerRegionGroups =
                initPartitionGroupConsumerRegions(newRegions);

        // 设置映射关系
        consumerRegionGroupExecutionViewMaintainer.notifyNewRegionGroupExecutionViews(
                newConsumerRegionGroups);
    }

    @VisibleForTesting
    public boolean isRegionOfVertexFinished(final ExecutionVertexID executionVertexId) {
        final PipelinedRegionExecutionView regionExecutionView =
                getPipelinedRegionExecutionViewForVertex(executionVertexId);
        return regionExecutionView.isFinished();
    }

    /**
     * 当拓扑图发生变化时触发
     * @param schedulingTopology the scheduling topology which is just updated
     * @param newExecutionVertices the newly added execution vertices.
     *
     */
    @Override
    public void notifySchedulingTopologyUpdated(
            SchedulingTopology schedulingTopology, List<ExecutionVertexID> newExecutionVertices) {

        final Set<SchedulingPipelinedRegion> newRegions =
                newExecutionVertices.stream()
                        .map(schedulingTopology::getPipelinedRegionOfVertex)
                        .collect(Collectors.toSet());

        notifySchedulingTopologyUpdatedInternal(newRegions);
    }

    /** Factory for {@link PartitionGroupReleaseStrategy}. */
    public static class Factory implements PartitionGroupReleaseStrategy.Factory {

        @Override
        public PartitionGroupReleaseStrategy createInstance(
                final SchedulingTopology schedulingStrategy) {
            return new RegionPartitionGroupReleaseStrategy(schedulingStrategy);
        }
    }
}
