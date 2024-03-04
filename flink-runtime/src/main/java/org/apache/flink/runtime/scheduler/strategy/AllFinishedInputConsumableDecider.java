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

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * {@link AllFinishedInputConsumableDecider} is a special {@link InputConsumableDecider}. The input
 * is considered to be consumable only when all producer partitions are finished.
 * 只有当所有生产者分区都完成时  才能进行消费
 */
public class AllFinishedInputConsumableDecider implements InputConsumableDecider {

    /**
     * 当executionVertex内所有消费者组的数据都处理完后 才能交由下面的顶点处理(处理新产生的数据)
     */
    @Override
    public boolean isInputConsumable(
            SchedulingExecutionVertex executionVertex,
            Set<ExecutionVertexID> verticesToDeploy,
            Map<ConsumedPartitionGroup, Boolean> consumableStatusCache) {

        // 遍历这个顶点要消费的数据
        for (ConsumedPartitionGroup consumedPartitionGroup :
                executionVertex.getConsumedPartitionGroups()) {

            if (!consumableStatusCache.computeIfAbsent(
                    consumedPartitionGroup, this::isConsumableBasedOnFinishedProducers)) {
                // 有一个未完成就返回false
                return false;
            }
        }

        // 全部都完成返回true
        return true;
    }

    /**
     * 判断某个分区组是否已经全部处理完
     * @param consumedPartitionGroup to be determined whether it is consumable.
     *
     * @return
     */
    @Override
    public boolean isConsumableBasedOnFinishedProducers(
            final ConsumedPartitionGroup consumedPartitionGroup) {
        return consumedPartitionGroup.getNumberOfUnfinishedPartitions() == 0;
    }

    /** Factory for {@link AllFinishedInputConsumableDecider}. */
    public static class Factory implements InputConsumableDecider.Factory {

        public static final Factory INSTANCE = new Factory();

        private Factory() {}

        @Override
        public InputConsumableDecider createInstance(
                SchedulingTopology schedulingTopology,
                Function<ExecutionVertexID, Boolean> scheduledVertexRetriever) {
            // 因为要求所有中间集都被消耗完 所以在初始化时其实不需要其他参数
            return new AllFinishedInputConsumableDecider();
        }
    }
}
