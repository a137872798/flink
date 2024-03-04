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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Class that manages all the connections between tasks.
 * 可以将顶点连接起来
 * */
public class EdgeManager {

    /**
     * IntermediateResultPartitionID 代表一个中间结果集   可以被多个subtask消耗 所以对应多个ConsumerVertexGroup
     */
    private final Map<IntermediateResultPartitionID, List<ConsumerVertexGroup>> partitionConsumers =
            new HashMap<>();

    /**
     * 每个ExecutionVertexID 代表一个subtask 用于消费数据
     * ConsumedPartitionGroup 就代表被消费的数据
     */
    private final Map<ExecutionVertexID, List<ConsumedPartitionGroup>> vertexConsumedPartitions =
            new HashMap<>();

    /**
     * 包含该id的所有数据集   因为同一个中间数据集是可以被消费多次的  所以对应的id可以出现在多个数据集组中
     */
    private final Map<IntermediateResultPartitionID, List<ConsumedPartitionGroup>>
            consumedPartitionsById = new HashMap<>();

    /**
     * 表示某结果集会由某个子任务消费
     */
    public void connectPartitionWithConsumerVertexGroup(
            IntermediateResultPartitionID resultPartitionId,
            ConsumerVertexGroup consumerVertexGroup) {

        checkNotNull(consumerVertexGroup);

        // 获取结果集的所有消费组   也可以为同一个结果集追加多个消费组
        List<ConsumerVertexGroup> groups =
                getConsumerVertexGroupsForPartitionInternal(resultPartitionId);
        groups.add(consumerVertexGroup);
    }

    /**
     * 添加一个消费者与数据集的关系
     * @param executionVertexId
     * @param consumedPartitionGroup
     */
    public void connectVertexWithConsumedPartitionGroup(
            ExecutionVertexID executionVertexId, ConsumedPartitionGroup consumedPartitionGroup) {

        checkNotNull(consumedPartitionGroup);

        final List<ConsumedPartitionGroup> consumedPartitions =
                getConsumedPartitionGroupsForVertexInternal(executionVertexId);

        consumedPartitions.add(consumedPartitionGroup);
    }

    /**
     * 获取消费该数据集的所有subtask
     * @param resultPartitionId
     * @return
     */
    private List<ConsumerVertexGroup> getConsumerVertexGroupsForPartitionInternal(
            IntermediateResultPartitionID resultPartitionId) {
        return partitionConsumers.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
    }

    /**
     * 获取该subtask消费的所有数据集
     * @param executionVertexId
     * @return
     */
    private List<ConsumedPartitionGroup> getConsumedPartitionGroupsForVertexInternal(
            ExecutionVertexID executionVertexId) {
        return vertexConsumedPartitions.computeIfAbsent(executionVertexId, id -> new ArrayList<>());
    }

    public List<ConsumerVertexGroup> getConsumerVertexGroupsForPartition(
            IntermediateResultPartitionID resultPartitionId) {
        return Collections.unmodifiableList(
                getConsumerVertexGroupsForPartitionInternal(resultPartitionId));
    }

    public List<ConsumedPartitionGroup> getConsumedPartitionGroupsForVertex(
            ExecutionVertexID executionVertexId) {
        return Collections.unmodifiableList(
                getConsumedPartitionGroupsForVertexInternal(executionVertexId));
    }

    /**
     * 注册数据集
     * @param group
     */
    public void registerConsumedPartitionGroup(ConsumedPartitionGroup group) {
        for (IntermediateResultPartitionID partitionId : group) {
            consumedPartitionsById
                    .computeIfAbsent(partitionId, ignore -> new ArrayList<>())
                    .add(group);
        }
    }

    private List<ConsumedPartitionGroup> getConsumedPartitionGroupsByIdInternal(
            IntermediateResultPartitionID resultPartitionId) {
        return consumedPartitionsById.computeIfAbsent(resultPartitionId, id -> new ArrayList<>());
    }

    public List<ConsumedPartitionGroup> getConsumedPartitionGroupsById(
            IntermediateResultPartitionID resultPartitionId) {
        return Collections.unmodifiableList(
                getConsumedPartitionGroupsByIdInternal(resultPartitionId));
    }

    public int getNumberOfConsumedPartitionGroupsById(
            IntermediateResultPartitionID resultPartitionId) {
        return getConsumedPartitionGroupsByIdInternal(resultPartitionId).size();
    }
}
