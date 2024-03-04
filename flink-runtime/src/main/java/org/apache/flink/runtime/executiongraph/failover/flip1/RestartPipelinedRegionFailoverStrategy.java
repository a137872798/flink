/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A failover strategy that proposes to restart involved regions when a vertex fails. A region is
 * defined by this strategy as tasks that communicate via pipelined data exchange.
 * 重启时 将整条流水线相关的节点重启
 */
public class RestartPipelinedRegionFailoverStrategy implements FailoverStrategy {

    /** The topology containing info about all the vertices and result partitions. */
    private final SchedulingTopology topology;

    /** The checker helps to query result partition availability.
     * 该对象用于判断数据集是否可用
     * */
    private final RegionFailoverResultPartitionAvailabilityChecker
            resultPartitionAvailabilityChecker;

    /**
     * Creates a new failover strategy to restart pipelined regions that works on the given
     * topology. The result partitions are always considered to be available if no data consumption
     * error happens.
     *
     * @param topology containing info about all the vertices and result partitions
     */
    @VisibleForTesting
    public RestartPipelinedRegionFailoverStrategy(SchedulingTopology topology) {
        this(topology, resultPartitionID -> true);
    }

    /**
     * Creates a new failover strategy to restart pipelined regions that works on the given
     * topology.
     *
     * @param topology containing info about all the vertices and result partitions
     * @param resultPartitionAvailabilityChecker helps to query result partition availability
     */
    public RestartPipelinedRegionFailoverStrategy(
            SchedulingTopology topology,
            ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

        this.topology = checkNotNull(topology);
        this.resultPartitionAvailabilityChecker =
                new RegionFailoverResultPartitionAvailabilityChecker(
                        resultPartitionAvailabilityChecker,
                        (intermediateResultPartitionID ->
                                topology.getResultPartition(intermediateResultPartitionID)
                                        .getResultType()));
    }

    // ------------------------------------------------------------------------
    //  task failure handling
    // ------------------------------------------------------------------------

    /**
     * Returns a set of IDs corresponding to the set of vertices that should be restarted. In this
     * strategy, all task vertices in 'involved' regions are proposed to be restarted. The
     * 'involved' regions are calculated with rules below: 1. The region containing the failed task
     * is always involved 2. If an input result partition of an involved region is not available,
     * i.e. Missing or Corrupted, the region containing the partition producer task is involved 3.
     * If a region is involved, all of its consumer regions are involved
     *
     * @param executionVertexId ID of the failed task
     * @param cause cause of the failure
     * @return set of IDs of vertices to restart
     * 当该顶点出错时  找到相关需要重启的节点
     */
    @Override
    public Set<ExecutionVertexID> getTasksNeedingRestart(
            ExecutionVertexID executionVertexId, Throwable cause) {

        // 找到相关的流水线
        final SchedulingPipelinedRegion failedRegion =
                topology.getPipelinedRegionOfVertex(executionVertexId);
        if (failedRegion == null) {
            // TODO: show the task name in the log
            throw new IllegalStateException(
                    "Can not find the failover region for task " + executionVertexId, cause);
        }

        // if the failure cause is data consumption error, mark the corresponding data partition to
        // be failed,
        // so that the failover process will try to recover it
        Optional<PartitionException> dataConsumptionException =
                ExceptionUtils.findThrowable(cause, PartitionException.class);

        // 如果是由于分区异常 进行记录
        if (dataConsumptionException.isPresent()) {
            resultPartitionAvailabilityChecker.markResultPartitionFailed(
                    dataConsumptionException.get().getPartitionId().getPartitionId());
        }

        // calculate the tasks to restart based on the result of regions to restart
        // 找到所有需要重启的流水线
        Set<ExecutionVertexID> tasksToRestart = new HashSet<>();

        // 上游和下游都查了个遍
        for (SchedulingPipelinedRegion region : getRegionsToRestart(failedRegion)) {
            // 找到所有顶点
            for (SchedulingExecutionVertex vertex : region.getVertices()) {
                // we do not need to restart tasks which are already in the initial state
                // CREATED 表示顶点正处于初始阶段 还未调度  那么就不需要重复重启了
                if (vertex.getState() != ExecutionState.CREATED) {
                    tasksToRestart.add(vertex.getId());
                }
            }
        }

        // the previous failed partition will be recovered. remove its failed state from the checker
        if (dataConsumptionException.isPresent()) {
            resultPartitionAvailabilityChecker.removeResultPartitionFromFailedState(
                    dataConsumptionException.get().getPartitionId().getPartitionId());
        }

        return tasksToRestart;
    }

    /**
     * All 'involved' regions are proposed to be restarted. The 'involved' regions are calculated
     * with rules below: 1. The region containing the failed task is always involved 2. If an input
     * result partition of an involved region is not available, i.e. Missing or Corrupted, the
     * region containing the partition producer task is involved 3. If a region is involved, all of
     * its consumer regions are involved
     * 找到相关的所有流水线
     */
    private Set<SchedulingPipelinedRegion> getRegionsToRestart(
            SchedulingPipelinedRegion failedRegion) {
        Set<SchedulingPipelinedRegion> regionsToRestart =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // 避免重复访问
        Set<SchedulingPipelinedRegion> visitedRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // 记录访问过的数据集
        Set<ConsumedPartitionGroup> visitedConsumedResultGroups =
                Collections.newSetFromMap(new IdentityHashMap<>());
        Set<ConsumerVertexGroup> visitedConsumerVertexGroups =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // start from the failed region to visit all involved regions
        Queue<SchedulingPipelinedRegion> regionsToVisit = new ArrayDeque<>();
        visitedRegions.add(failedRegion);
        regionsToVisit.add(failedRegion);

        // 此时要访问的流水线   如果引出更多的流水线  就会加入到regionsToVisit 并进入下次循环
        while (!regionsToVisit.isEmpty()) {
            SchedulingPipelinedRegion regionToRestart = regionsToVisit.poll();

            // an involved region should be restarted
            // 首先本次被观测的流水线肯定要重启
            regionsToRestart.add(regionToRestart);

            // if a needed input result partition is not available, its producer region is involved
            // 找到本流水线要消费的所有数据来源
            for (IntermediateResultPartitionID consumedPartitionId :
                    getConsumedPartitionsToVisit(regionToRestart, visitedConsumedResultGroups)) {
                // 这些分区发现不能用了 必须要在生产端重启  重新产生数据
                if (!resultPartitionAvailabilityChecker.isAvailable(consumedPartitionId)) {

                    // 找到数据集
                    SchedulingResultPartition consumedPartition =
                            topology.getResultPartition(consumedPartitionId);

                    // 找到产生该数据集的流水线
                    SchedulingPipelinedRegion producerRegion =
                            topology.getPipelinedRegionOfVertex(
                                    consumedPartition.getProducer().getId());
                    // 加入regionsToVisit 就会在之后的循环中访问
                    if (!visitedRegions.contains(producerRegion)) {
                        visitedRegions.add(producerRegion);
                        regionsToVisit.add(producerRegion);
                    }
                }
            }

            // all consumer regions of an involved region should be involved
            // 找到下游会消费该数据集的对象
            for (ExecutionVertexID consumerVertexId :
                    getConsumerVerticesToVisit(regionToRestart, visitedConsumerVertexGroups)) {

                // 找到相关的流水线
                SchedulingPipelinedRegion consumerRegion =
                        topology.getPipelinedRegionOfVertex(consumerVertexId);
                if (!visitedRegions.contains(consumerRegion)) {
                    visitedRegions.add(consumerRegion);
                    regionsToVisit.add(consumerRegion);
                }
            }
        }

        return regionsToRestart;
    }

    /**
     *
     * @param regionToRestart
     * @param visitedConsumedResultGroups  需要跳过的数据集
     * @return
     */
    private Iterable<IntermediateResultPartitionID> getConsumedPartitionsToVisit(
            SchedulingPipelinedRegion regionToRestart,
            Set<ConsumedPartitionGroup> visitedConsumedResultGroups) {

        final List<ConsumedPartitionGroup> consumedPartitionGroupsToVisit = new ArrayList<>();

        // 参与流水线的所有顶点
        for (SchedulingExecutionVertex vertex : regionToRestart.getVertices()) {

            // 每个顶点要消费的数据 通过反查这些数据的来源就可以找到关联的流水线了
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    vertex.getConsumedPartitionGroups()) {
                // 表示未被访问过
                if (!visitedConsumedResultGroups.contains(consumedPartitionGroup)) {
                    visitedConsumedResultGroups.add(consumedPartitionGroup);
                    consumedPartitionGroupsToVisit.add(consumedPartitionGroup);
                }
            }
        }

        // 展开每个数据集 得到一组IntermediateResultPartitionID
        return IterableUtils.flatMap(consumedPartitionGroupsToVisit, Function.identity());
    }

    /**
     * 得到消费该流水线数据的所有消费者
     * @param regionToRestart  本次遍历的流水线
     * @param visitedConsumerVertexGroups
     * @return
     */
    private Iterable<ExecutionVertexID> getConsumerVerticesToVisit(
            SchedulingPipelinedRegion regionToRestart,
            Set<ConsumerVertexGroup> visitedConsumerVertexGroups) {
        final List<ConsumerVertexGroup> consumerVertexGroupsToVisit = new ArrayList<>();

        for (SchedulingExecutionVertex vertex : regionToRestart.getVertices()) {
            // 得到每个顶点产生的数据
            for (SchedulingResultPartition producedPartition : vertex.getProducedResults()) {
                // 描述该数据会由哪些顶点消费
                for (ConsumerVertexGroup consumerVertexGroup :
                        producedPartition.getConsumerVertexGroups()) {
                    if (!visitedConsumerVertexGroups.contains(consumerVertexGroup)) {
                        visitedConsumerVertexGroups.add(consumerVertexGroup);
                        consumerVertexGroupsToVisit.add(consumerVertexGroup);
                    }
                }
            }
        }

        return IterableUtils.flatMap(consumerVertexGroupsToVisit, Function.identity());
    }

    // ------------------------------------------------------------------------
    //  testing
    // ------------------------------------------------------------------------

    /**
     * Returns the failover region that contains the given execution vertex.
     *
     * @return the failover region that contains the given execution vertex
     */
    @VisibleForTesting
    public SchedulingPipelinedRegion getFailoverRegion(ExecutionVertexID vertexID) {
        return topology.getPipelinedRegionOfVertex(vertexID);
    }

    /**
     * A stateful {@link ResultPartitionAvailabilityChecker} which maintains the failed partitions
     * which are not available.
     * 判断某个分区的数据集是否还可用
     */
    private static class RegionFailoverResultPartitionAvailabilityChecker
            implements ResultPartitionAvailabilityChecker {

        /** Result partition state checker from the shuffle master. */
        private final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker;

        /** Records partitions which has caused {@link PartitionException}. */
        private final HashSet<IntermediateResultPartitionID> failedPartitions;

        /** Retrieve {@link ResultPartitionType} by {@link IntermediateResultPartitionID}. */
        private final Function<IntermediateResultPartitionID, ResultPartitionType>
                resultPartitionTypeRetriever;

        RegionFailoverResultPartitionAvailabilityChecker(
                ResultPartitionAvailabilityChecker checker,
                Function<IntermediateResultPartitionID, ResultPartitionType>
                        resultPartitionTypeRetriever) {
            this.resultPartitionAvailabilityChecker = checkNotNull(checker);
            this.failedPartitions = new HashSet<>();
            this.resultPartitionTypeRetriever = checkNotNull(resultPartitionTypeRetriever);
        }

        @Override
        public boolean isAvailable(IntermediateResultPartitionID resultPartitionID) {
            // 如果该分区已经存在于failedPartitions 就表示不可用
            return !failedPartitions.contains(resultPartitionID)
                    // 内部对象也认为可用
                    && resultPartitionAvailabilityChecker.isAvailable(resultPartitionID)
                    // If the result partition is available in the partition tracker and does not
                    // fail, it will be available if it can be re-consumption, and it may also be
                    // available for PIPELINED_APPROXIMATE type.
                    // 属于可重复消费 或者近似类型
                    && isResultPartitionIsReConsumableOrPipelinedApproximate(resultPartitionID);
        }

        /**
         * 标记某个分区失败
         * @param resultPartitionID
         */
        public void markResultPartitionFailed(IntermediateResultPartitionID resultPartitionID) {
            failedPartitions.add(resultPartitionID);
        }

        /**
         * 表示某个分区解除失败
         * @param resultPartitionID
         */
        public void removeResultPartitionFromFailedState(
                IntermediateResultPartitionID resultPartitionID) {
            failedPartitions.remove(resultPartitionID);
        }

        /**
         * 检查该数据是否可重复消费
         * @param resultPartitionID
         * @return
         */
        private boolean isResultPartitionIsReConsumableOrPipelinedApproximate(
                IntermediateResultPartitionID resultPartitionID) {
            ResultPartitionType resultPartitionType =
                    resultPartitionTypeRetriever.apply(resultPartitionID);
            // 可重复消费  或者只是近似数据
            return resultPartitionType.isReconsumable()
                    || resultPartitionType == ResultPartitionType.PIPELINED_APPROXIMATE;
        }
    }

    /** The factory to instantiate {@link RestartPipelinedRegionFailoverStrategy}. */
    public static class Factory implements FailoverStrategy.Factory {

        @Override
        public FailoverStrategy create(
                final SchedulingTopology topology,
                final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

            return new RestartPipelinedRegionFailoverStrategy(
                    topology, resultPartitionAvailabilityChecker);
        }
    }
}
