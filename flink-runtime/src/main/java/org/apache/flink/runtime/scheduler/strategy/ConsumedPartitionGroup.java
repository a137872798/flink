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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Group of consumed {@link IntermediateResultPartitionID}s. One such a group corresponds to one
 * {@link ConsumerVertexGroup}.
 * 表示已经被消耗完的中间集数据    刚初始化的时候默认 resultPartitions的数据都还没被处理完
 */
public class ConsumedPartitionGroup implements Iterable<IntermediateResultPartitionID> {

    /**
     * 该对象是一个数据集 每个IntermediateResultPartitionID 对应一部分数据的id
     */
    private final List<IntermediateResultPartitionID> resultPartitions;

    /**
     * 表示内部还未处理完的分区数据
     */
    private final AtomicInteger unfinishedPartitions;

    /**
     * 这些分区数据隶属于同一个结果集
     */
    private final IntermediateDataSetID intermediateDataSetID;

    /**
     * 表示分区的结果类型
     */
    private final ResultPartitionType resultPartitionType;

    /** Number of consumer tasks in the corresponding {@link ConsumerVertexGroup}.
     * 表示下游有多少消费者   数量跟分区数没有直接关系
     * */
    private final int numConsumers;

    /**
     * 内部存储了ExecutionVertexID  每个ExecutionVertexID对应顶点+subtaskIndex  所以代表一个消费对象
     */
    @Nullable private ConsumerVertexGroup consumerVertexGroup;

    private ConsumedPartitionGroup(
            int numConsumers,
            List<IntermediateResultPartitionID> resultPartitions,  // 指向数据集的某个分区
            ResultPartitionType resultPartitionType) {
        checkArgument(
                resultPartitions.size() > 0,
                "The size of result partitions in the ConsumedPartitionGroup should be larger than 0.");
        this.numConsumers = numConsumers;

        // 这些数据集对象的id都是一样的
        this.intermediateDataSetID = resultPartitions.get(0).getIntermediateDataSetID();
        this.resultPartitionType = Preconditions.checkNotNull(resultPartitionType);

        // Sanity check: all the partitions in one ConsumedPartitionGroup should have the same
        // IntermediateDataSetID
        // 确保他们id一致
        for (IntermediateResultPartitionID resultPartition : resultPartitions) {
            checkArgument(
                    resultPartition.getIntermediateDataSetID().equals(this.intermediateDataSetID));
        }
        this.resultPartitions = resultPartitions;

        // 表示需要被消费的数据集分区数量
        this.unfinishedPartitions = new AtomicInteger(resultPartitions.size());
    }

    public static ConsumedPartitionGroup fromMultiplePartitions(
            int numConsumers,
            List<IntermediateResultPartitionID> resultPartitions,
            ResultPartitionType resultPartitionType) {
        return new ConsumedPartitionGroup(numConsumers, resultPartitions, resultPartitionType);
    }

    public static ConsumedPartitionGroup fromSinglePartition(
            int numConsumers,
            IntermediateResultPartitionID resultPartition,
            ResultPartitionType resultPartitionType) {
        return new ConsumedPartitionGroup(
                numConsumers, Collections.singletonList(resultPartition), resultPartitionType);
    }

    @Override
    public Iterator<IntermediateResultPartitionID> iterator() {
        return resultPartitions.iterator();
    }

    public int size() {
        return resultPartitions.size();
    }

    public boolean isEmpty() {
        return resultPartitions.isEmpty();
    }

    /**
     * In dynamic graph cases, the number of consumers of ConsumedPartitionGroup can be different
     * even if they contain the same IntermediateResultPartition.
     */
    public int getNumConsumers() {
        return numConsumers;
    }

    public IntermediateResultPartitionID getFirst() {
        return iterator().next();
    }

    /** Get the ID of IntermediateDataSet this ConsumedPartitionGroup belongs to. */
    public IntermediateDataSetID getIntermediateDataSetID() {
        return intermediateDataSetID;
    }

    public int partitionUnfinished() {
        return unfinishedPartitions.incrementAndGet();
    }

    public int partitionFinished() {
        return unfinishedPartitions.decrementAndGet();
    }

    public int getNumberOfUnfinishedPartitions() {
        return unfinishedPartitions.get();
    }

    public boolean areAllPartitionsFinished() {
        return unfinishedPartitions.get() == 0;
    }

    public ResultPartitionType getResultPartitionType() {
        return resultPartitionType;
    }

    public ConsumerVertexGroup getConsumerVertexGroup() {
        return checkNotNull(consumerVertexGroup, "ConsumerVertexGroup is not properly set.");
    }

    /**
     * 可以手动设置消耗该组数据的 消费者组
     * @param consumerVertexGroup
     */
    public void setConsumerVertexGroup(ConsumerVertexGroup consumerVertexGroup) {
        checkState(this.consumerVertexGroup == null);
        this.consumerVertexGroup = checkNotNull(consumerVertexGroup);
    }
}
