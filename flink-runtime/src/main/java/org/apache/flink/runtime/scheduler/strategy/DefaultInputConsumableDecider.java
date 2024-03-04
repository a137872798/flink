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

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Default implementation of {@link InputConsumableDecider}. This decider will judge whether the
 * executionVertex's inputs are consumable as follows:
 *
 * <p>For blocking consumed partition group: Whether all result partitions in the group are
 * finished.
 *
 * <p>For canBePipelined consumed partition group: whether all result partitions in the group are
 * scheduled.
 * 默认判定对象
 */
public class DefaultInputConsumableDecider implements InputConsumableDecider {
    private final Function<IntermediateResultPartitionID, SchedulingResultPartition>
            resultPartitionRetriever;

    private final Function<ExecutionVertexID, Boolean> scheduledVertexRetriever;

    DefaultInputConsumableDecider(
            Function<ExecutionVertexID, Boolean> scheduledVertexRetriever,
            Function<IntermediateResultPartitionID, SchedulingResultPartition>
                    resultPartitionRetriever) {
        this.scheduledVertexRetriever = scheduledVertexRetriever;
        this.resultPartitionRetriever = resultPartitionRetriever;
    }

    /**
     * 判断executionVertex的数据是否满足消费条件
     * @param executionVertex to be determined whether it's input is consumable.
     * @param verticesToSchedule vertices that are not yet scheduled but already decided to be
     *     scheduled.  表示要使用这些对象来消费数据
     * @param consumableStatusCache a cache for {@link ConsumedPartitionGroup} consumable status.
     * @return
     */
    @Override
    public boolean isInputConsumable(
            SchedulingExecutionVertex executionVertex,
            Set<ExecutionVertexID> verticesToSchedule,
            Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {

        // 先遍历所有待消费数据
        for (ConsumedPartitionGroup consumedPartitionGroup :
                executionVertex.getConsumedPartitionGroups()) {

            if (!consumableStatusCache.computeIfAbsent(
                    consumedPartitionGroup,
                    // 只要有一个未满足条件  即认为此时不可消费顶点数据
                    (group) -> isConsumedPartitionGroupConsumable(group, verticesToSchedule))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 表示能否消费是否取决于处理完所有中间结果集
     * @param consumedPartitionGroup to be determined whether it is consumable.
     *
     * @return
     */
    @Override
    public boolean isConsumableBasedOnFinishedProducers(
            final ConsumedPartitionGroup consumedPartitionGroup) {

        // 如果本节点本身就是流水线模式  那么不需要依赖该方法来触发调度  所以这种情况直接返回false (因为这一步结果并不重要)
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            // For canBePipelined consumed partition group, whether it is consumable does not depend
            // on task finish. To optimize performance and avoid unnecessary computation, we simply
            // return false.
            return false;
        } else {
            // 所有分区数据都消费完
            return consumedPartitionGroup.areAllPartitionsFinished();
        }
    }

    /**
     * @param consumedPartitionGroup  需要准备好的待消费数据
     * @param verticesToSchedule  本次打算调度的顶点
     * @return
     */
    private boolean isConsumedPartitionGroupConsumable(
            final ConsumedPartitionGroup consumedPartitionGroup,
            final Set<ExecutionVertexID> verticesToSchedule) {

        // 该数据集允许以管道模式消费
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {

            // 对应一个个待消费的中间结果集
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                // 找到产生该数据的顶点
                ExecutionVertexID producerVertex =
                        resultPartitionRetriever.apply(partitionId).getProducer().getId();

                // 表示依赖的上游顶点还未调度
                if (!verticesToSchedule.contains(producerVertex)
                        && !scheduledVertexRetriever.apply(producerVertex)) {
                    return false;
                }
            }
        } else {
            // 阻塞执行
            // 遍历该消费数据中各个 result
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                // 阻塞模式下 必须等待所有数据消费完毕
                if (resultPartitionRetriever.apply(partitionId).getState()
                        != ResultPartitionState.ALL_DATA_PRODUCED) {
                    return false;
                }
            }
        }
        return true;
    }

    /** Factory for {@link DefaultInputConsumableDecider}. */
    public static class Factory implements InputConsumableDecider.Factory {

        public static final InputConsumableDecider.Factory INSTANCE = new Factory();

        // disable public instantiation.
        private Factory() {}

        @Override
        public InputConsumableDecider createInstance(
                SchedulingTopology schedulingTopology,
                Function<ExecutionVertexID, Boolean> scheduledVertexRetriever) {
            return new DefaultInputConsumableDecider(
                    scheduledVertexRetriever, schedulingTopology::getResultPartition);
        }
    }
}
