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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.runtime.scheduler.SchedulingTopologyListener;
import org.apache.flink.util.IterableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of vertex (which
 * indicates this strategy only supports batch jobs). Note that this strategy implements {@link
 * SchedulingTopologyListener}, so it can handle the updates of scheduling topology.
 *
 * 也是一个调度策略
 */
public class VertexwiseSchedulingStrategy
        implements SchedulingStrategy, SchedulingTopologyListener {

    private static final Logger LOG = LoggerFactory.getLogger(VertexwiseSchedulingStrategy.class);

    /**
     * 可以为execution分配slot 以及部署
     */
    private final SchedulerOperations schedulerOperations;

    /**
     * 拓扑图可以查顶点和数据集
     */
    private final SchedulingTopology schedulingTopology;

    /**
     * 新添加的顶点
     */
    private final Set<ExecutionVertexID> newVertices = new HashSet<>();

    /**
     * 已经完成调度的顶点
     */
    private final Set<ExecutionVertexID> scheduledVertices = new HashSet<>();

    /**
     * 判断能否消费数据
     */
    private final InputConsumableDecider inputConsumableDecider;

    public VertexwiseSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology,
            final InputConsumableDecider.Factory inputConsumableDeciderFactory) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);
        this.inputConsumableDecider =
                inputConsumableDeciderFactory.createInstance(
                        schedulingTopology, scheduledVertices::contains);
        LOG.info(
                "Using InputConsumableDecider {} for VertexwiseSchedulingStrategy.",
                inputConsumableDecider.getClass().getName());
        schedulingTopology.registerSchedulingTopologyListener(this);
    }

    /**
     * 开始调度
     */
    @Override
    public void startScheduling() {
        Set<ExecutionVertexID> sourceVertices =
                IterableUtils.toStream(schedulingTopology.getVertices())
                        // 找到消费组为空的顶点   应该就是产生起点数据的顶点   所以被称为 源顶点
                        .filter(vertex -> vertex.getConsumedPartitionGroups().isEmpty())
                        .map(SchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet());

        maybeScheduleVertices(sourceVertices);
    }

    @Override
    public void restartTasks(Set<ExecutionVertexID> verticesToRestart) {
        // 重新调度
        scheduledVertices.removeAll(verticesToRestart);
        maybeScheduleVertices(verticesToRestart);
    }

    @Override
    public void onExecutionStateChange(
            ExecutionVertexID executionVertexId, ExecutionState executionState) {
        // 应该是代表当上一个顶点处理完后  调度下游的顶点
        if (executionState == ExecutionState.FINISHED) {
            SchedulingExecutionVertex executionVertex =
                    schedulingTopology.getVertex(executionVertexId);

            Set<ExecutionVertexID> consumerVertices =
                    // 遍历该顶点产生的所有数据
                    IterableUtils.toStream(executionVertex.getProducedResults())
                            // 找到他们的消费对象
                            .map(SchedulingResultPartition::getConsumerVertexGroups)
                            .flatMap(Collection::stream)
                            .filter(
                                    group ->
                                            // 表示该对象的消费数据产生完毕时 能否调度
                                            inputConsumableDecider
                                                    .isConsumableBasedOnFinishedProducers(
                                                            group.getConsumedPartitionGroup()))
                            .flatMap(IterableUtils::toStream)
                            .collect(Collectors.toSet());

            maybeScheduleVertices(consumerVertices);
        }
    }

    @Override
    public void onPartitionConsumable(IntermediateResultPartitionID resultPartitionId) {}

    @Override
    public void notifySchedulingTopologyUpdated(
            SchedulingTopology schedulingTopology, List<ExecutionVertexID> newExecutionVertices) {
        checkState(schedulingTopology == this.schedulingTopology);
        // 惰性处理
        newVertices.addAll(newExecutionVertices);
    }

    /**
     * 尝试调度这些顶点
     * @param vertices
     */
    private void maybeScheduleVertices(final Set<ExecutionVertexID> vertices) {
        Set<ExecutionVertexID> allCandidates;
        if (newVertices.isEmpty()) {
            allCandidates = vertices;
        } else {
            // 追加要调度的顶点
            allCandidates = new HashSet<>(vertices);
            allCandidates.addAll(newVertices);
            newVertices.clear();
        }

        final Set<ExecutionVertexID> verticesToSchedule = new HashSet<>();

        Set<ExecutionVertexID> nextVertices = allCandidates;
        while (!nextVertices.isEmpty()) {
            nextVertices = addToScheduleAndGetVertices(nextVertices, verticesToSchedule);
        }

        scheduleVerticesOneByOne(verticesToSchedule);
        scheduledVertices.addAll(verticesToSchedule);
    }

    /**
     * 可能会衍生出更多需要调度的顶点
     * @param currentVertices 当前需要调度器的节点
     * @param verticesToSchedule  通过拓扑图查询顶点间的关系
     * @return
     */
    private Set<ExecutionVertexID> addToScheduleAndGetVertices(
            Set<ExecutionVertexID> currentVertices, Set<ExecutionVertexID> verticesToSchedule) {
        Set<ExecutionVertexID> nextVertices = new HashSet<>();
        // cache consumedPartitionGroup's consumable status to avoid compute repeatedly.
        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new IdentityHashMap<>();
        final Set<ConsumerVertexGroup> visitedConsumerVertexGroup =
                Collections.newSetFromMap(new IdentityHashMap<>());

        for (ExecutionVertexID currentVertex : currentVertices) {
            // 首先要能够调度
            if (isVertexSchedulable(currentVertex, consumableStatusCache, verticesToSchedule)) {
                verticesToSchedule.add(currentVertex);
                // 找到可以流水线消费的组   流水线消费 也就是不用等待上游finish 就可以直接启动的顶点
                Set<ConsumerVertexGroup> canBePipelinedConsumerVertexGroups =
                        IterableUtils.toStream(
                                        schedulingTopology
                                                .getVertex(currentVertex)
                                                .getProducedResults())  // 表示该顶点会产生的结果
                                .map(SchedulingResultPartition::getConsumerVertexGroups)  // 表示会消费ProducedResults的组 也就是下游
                                .flatMap(Collection::stream)
                                .filter(
                                        (consumerVertexGroup) ->
                                                consumerVertexGroup
                                                        .getResultPartitionType()
                                                        .canBePipelinedConsumed())
                                .collect(Collectors.toSet());
                for (ConsumerVertexGroup consumerVertexGroup : canBePipelinedConsumerVertexGroups) {
                    if (!visitedConsumerVertexGroup.contains(consumerVertexGroup)) {
                        visitedConsumerVertexGroup.add(consumerVertexGroup);
                        nextVertices.addAll(
                                // 这些顶点是之后要探索的顶点
                                IterableUtils.toStream(consumerVertexGroup)
                                        .collect(Collectors.toSet()));
                    }
                }
            }
        }
        return nextVertices;
    }

    /**
     * 判断顶点能否调度
     * @param vertex
     * @param consumableStatusCache
     * @param verticesToSchedule
     * @return
     */
    private boolean isVertexSchedulable(
            final ExecutionVertexID vertex,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,
            final Set<ExecutionVertexID> verticesToSchedule) {

        // 代表本次会调度 避免重复添加
        return !verticesToSchedule.contains(vertex)
                // 必须还未调度
                && !scheduledVertices.contains(vertex)
                // 表示该顶点需要的上游数据(input) 是否可以消费了
                && inputConsumableDecider.isInputConsumable(
                        schedulingTopology.getVertex(vertex),
                        verticesToSchedule,
                        consumableStatusCache);
    }

    /**
     * 挨个调度
     * @param verticesToSchedule
     */
    private void scheduleVerticesOneByOne(final Set<ExecutionVertexID> verticesToSchedule) {
        if (verticesToSchedule.isEmpty()) {
            return;
        }
        final List<ExecutionVertexID> sortedVerticesToSchedule =
                SchedulingStrategyUtils.sortExecutionVerticesInTopologicalOrder(
                        schedulingTopology, verticesToSchedule);

        sortedVerticesToSchedule.forEach(
                id -> schedulerOperations.allocateSlotsAndDeploy(Collections.singletonList(id)));
    }

    /** The factory for creating {@link VertexwiseSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        private final InputConsumableDecider.Factory inputConsumableDeciderFactory;

        public Factory(InputConsumableDecider.Factory inputConsumableDeciderFactory) {
            this.inputConsumableDeciderFactory = inputConsumableDeciderFactory;
        }

        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new VertexwiseSchedulingStrategy(
                    schedulerOperations, schedulingTopology, inputConsumableDeciderFactory);
        }
    }
}
