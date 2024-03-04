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
 * {@link InputConsumableDecider} is responsible for determining whether the input of an
 * executionVertex or a consumed partition group is consumable.
 * 用于判断是否可以消费的对象
 */
public interface InputConsumableDecider {
    /**
     * Determining whether the input of an execution vertex is consumable.
     *
     * @param executionVertex to be determined whether it's input is consumable.
     * @param verticesToSchedule vertices that are not yet scheduled but already decided to be
     *     scheduled.
     * @param consumableStatusCache a cache for {@link ConsumedPartitionGroup} consumable status.
     *     This is to avoid repetitive computation.
     *                              表示该顶点需要的上游数据(input) 是否可以消费了
     */
    boolean isInputConsumable(
            SchedulingExecutionVertex executionVertex,   // 本次被考察的节点
            Set<ExecutionVertexID> verticesToSchedule,   // 表示还未调度  但是即将被调度的
            Map<ConsumedPartitionGroup, Boolean> consumableStatusCache);  // 缓存了executionVertex中某些消费组是否已经finish

    /**
     * Determining whether the consumed partition group is consumable based on finished producers.
     *
     * @param consumedPartitionGroup to be determined whether it is consumable.
     * 根据生产者是否结束来判断 数据能否消费
     */
    boolean isConsumableBasedOnFinishedProducers(
            final ConsumedPartitionGroup consumedPartitionGroup);

    /** Factory for {@link InputConsumableDecider}.
     * 该工厂根据相关信息可以产生 decider对象
     * */
    interface Factory {
        InputConsumableDecider createInstance(
                SchedulingTopology schedulingTopology,
                Function<ExecutionVertexID, Boolean> scheduledVertexRetriever);
    }
}
