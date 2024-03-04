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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Util to compute {@link JobVertexInputInfo}s for execution job vertex. */
public class VertexInputInfoComputationUtils {

    /**
     *
     * @param ejv  待处理的task
     * @param intermediateResultRetriever  通过id查找中间结果集
     * @return
     * @throws JobException
     */
    public static Map<IntermediateDataSetID, JobVertexInputInfo> computeVertexInputInfos(
            ExecutionJobVertex ejv,
            Function<IntermediateDataSetID, IntermediateResult> intermediateResultRetriever)
            throws JobException {
        checkState(ejv.isParallelismDecided());
        final List<IntermediateResultInfo> intermediateResultInfos = new ArrayList<>();
        for (JobEdge edge : ejv.getJobVertex().getInputs()) {
            IntermediateResult ires = intermediateResultRetriever.apply(edge.getSourceId());
            if (ires == null) {
                throw new JobException(
                        "Cannot connect this job graph to the previous graph. No previous intermediate result found for ID "
                                + edge.getSourceId());
            }
            intermediateResultInfos.add(new IntermediateResultWrapper(ires));
        }
        return computeVertexInputInfos(
                ejv.getParallelism(), intermediateResultInfos, ejv.getGraph().isDynamic());
    }

    /**
     * 计算数据消费
     * @param parallelism  使用的并行度
     * @param inputs  描述输入的数据量
     * @param isDynamicGraph
     * @return
     */
    public static Map<IntermediateDataSetID, JobVertexInputInfo> computeVertexInputInfos(
            int parallelism,
            List<? extends IntermediateResultInfo> inputs,
            boolean isDynamicGraph) {

        checkArgument(parallelism > 0);
        final Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos =
                new LinkedHashMap<>();

        for (IntermediateResultInfo input : inputs) {
            // 获取该输入的分区数
            int sourceParallelism = input.getNumPartitions();

            // 对应点态
            if (input.isPointwise()) {
                jobVertexInputInfos.putIfAbsent(
                        input.getResultId(),
                        computeVertexInputInfoForPointwise(
                                sourceParallelism,
                                parallelism,
                                input::getNumSubpartitions,
                                isDynamicGraph));
            } else {
                // 对应 allToAll模式   也就是各分区相同子分区数据要被同一个对象消费
                jobVertexInputInfos.putIfAbsent(
                        input.getResultId(),
                        computeVertexInputInfoForAllToAll(
                                sourceParallelism,
                                parallelism,
                                input::getNumSubpartitions,
                                isDynamicGraph,
                                input.isBroadcast()));
            }
        }

        return jobVertexInputInfos;
    }

    /**
     * Compute the {@link JobVertexInputInfo} for a {@link DistributionPattern#POINTWISE} edge. This
     * computation algorithm will evenly distribute subpartitions to downstream subtasks according
     * to the number of subpartitions. Different downstream subtasks consume roughly the same number
     * of subpartitions.
     *
     * @param sourceCount the parallelism of upstream
     * @param targetCount the parallelism of downstream
     * @param numOfSubpartitionsRetriever a retriever to get the number of subpartitions
     * @param isDynamicGraph whether is dynamic graph
     * @return the computed {@link JobVertexInputInfo}
     * 针对点态模式 为每个子任务分配上游消费的数据
     */
    static JobVertexInputInfo computeVertexInputInfoForPointwise(
            int sourceCount,
            int targetCount,
            Function<Integer, Integer> numOfSubpartitionsRetriever,
            boolean isDynamicGraph) {

        final List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();

        // 上游数量超过下游
        if (sourceCount >= targetCount) {
            for (int index = 0; index < targetCount; index++) {

                // 按比例计算 得到分区范围
                int start = index * sourceCount / targetCount;
                int end = (index + 1) * sourceCount / targetCount;

                IndexRange partitionRange = new IndexRange(start, end - 1);
                // 这里计算出来的子分区含义不同  范围变成了  0 ～ 分区*子分区
                IndexRange subpartitionRange =
                        computeConsumedSubpartitionRange(
                                index,
                                1,
                                () -> numOfSubpartitionsRetriever.apply(start),
                                isDynamicGraph,
                                false);
                executionVertexInputInfos.add(
                        new ExecutionVertexInputInfo(index, partitionRange, subpartitionRange));
            }
        } else {
            // 上游 < 下游
            for (int partitionNum = 0; partitionNum < sourceCount; partitionNum++) {

                // 转换成 target的范围
                int start = (partitionNum * targetCount + sourceCount - 1) / sourceCount;
                int end = ((partitionNum + 1) * targetCount + sourceCount - 1) / sourceCount;
                int numConsumers = end - start;

                IndexRange partitionRange = new IndexRange(partitionNum, partitionNum);
                // Variable used in lambda expression should be final or effectively final
                final int finalPartitionNum = partitionNum;

                // 在对应target的范围内检索
                for (int i = start; i < end; i++) {
                    // 得到子分区范围
                    IndexRange subpartitionRange =
                            computeConsumedSubpartitionRange(
                                    i,
                                    numConsumers,
                                    () -> numOfSubpartitionsRetriever.apply(finalPartitionNum),
                                    isDynamicGraph,
                                    false);
                    executionVertexInputInfos.add(
                            new ExecutionVertexInputInfo(i, partitionRange, subpartitionRange));
                }
            }
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    /**
     * Compute the {@link JobVertexInputInfo} for a {@link DistributionPattern#ALL_TO_ALL} edge.
     * This computation algorithm will evenly distribute subpartitions to downstream subtasks
     * according to the number of subpartitions. Different downstream subtasks consume roughly the
     * same number of subpartitions.
     *
     * @param sourceCount the parallelism of upstream  输入源的分区数
     * @param targetCount the parallelism of downstream  消费端的分区数
     * @param numOfSubpartitionsRetriever a retriever to get the number of subpartitions   查看某个分区有多少子分区
     * @param isDynamicGraph whether is dynamic graph
     * @param isBroadcast whether the edge is broadcast
     * @return the computed {@link JobVertexInputInfo}
     */
    static JobVertexInputInfo computeVertexInputInfoForAllToAll(
            int sourceCount,
            int targetCount,
            Function<Integer, Integer> numOfSubpartitionsRetriever,
            boolean isDynamicGraph,
            boolean isBroadcast) {
        final List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();

        // allToAll模式下 下游每个分区range都包含全部的分区
        IndexRange partitionRange = new IndexRange(0, sourceCount - 1);
        for (int i = 0; i < targetCount; ++i) {
            // 计算子分区范围
            IndexRange subpartitionRange =
                    computeConsumedSubpartitionRange(
                            i,
                            targetCount,
                            () -> numOfSubpartitionsRetriever.apply(0),
                            isDynamicGraph,
                            isBroadcast);
            executionVertexInputInfos.add(
                    new ExecutionVertexInputInfo(i, partitionRange, subpartitionRange));
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    /**
     * Compute the consumed subpartition range for a subtask. This computation algorithm will evenly
     * distribute subpartitions to downstream subtasks according to the number of subpartitions.
     * Different downstream subtasks consume roughly the same number of subpartitions.
     *
     * @param consumerSubtaskIndex the subtask index  子任务下标
     * @param numConsumers the total number of consumers  总计有多少子任务消费
     * @param numOfSubpartitionsSupplier a supplier to get the number of subpartitions  可以查询分区下子分区数
     * @param isDynamicGraph whether is dynamic graph  是否为动态图
     * @param isBroadcast whether the edge is broadcast  是否广播
     * @return the computed subpartition range
     * 计算该子任务如何消费上游数据
     */
    @VisibleForTesting
    static IndexRange computeConsumedSubpartitionRange(
            int consumerSubtaskIndex,
            int numConsumers,
            Supplier<Integer> numOfSubpartitionsSupplier,
            boolean isDynamicGraph,
            boolean isBroadcast) {
        int consumerIndex = consumerSubtaskIndex % numConsumers;
        if (!isDynamicGraph) {
            // 非动态 1比1消费  下游每个task对应上游所有分区的某个子分区
            return new IndexRange(consumerIndex, consumerIndex);
        } else {
            // 获取子分区数
            int numSubpartitions = numOfSubpartitionsSupplier.get();
            if (isBroadcast) {
                // broadcast results have only one subpartition, and be consumed multiple times.
                checkArgument(numSubpartitions == 1);
                // 广播模式特殊情况
                return new IndexRange(0, 0);
            } else {
                checkArgument(consumerIndex < numConsumers);
                checkArgument(numConsumers <= numSubpartitions);

                // 按照下游子任务占比 等比例计算子分区数
                int start = consumerIndex * numSubpartitions / numConsumers;
                int nextStart = (consumerIndex + 1) * numSubpartitions / numConsumers;

                return new IndexRange(start, nextStart - 1);
            }
        }
    }

    private static class IntermediateResultWrapper implements IntermediateResultInfo {
        private final IntermediateResult intermediateResult;

        IntermediateResultWrapper(IntermediateResult intermediateResult) {
            this.intermediateResult = checkNotNull(intermediateResult);
        }

        @Override
        public IntermediateDataSetID getResultId() {
            return intermediateResult.getId();
        }

        @Override
        public boolean isBroadcast() {
            return intermediateResult.isBroadcast();
        }

        @Override
        public boolean isPointwise() {
            return intermediateResult.getConsumingDistributionPattern()
                    == DistributionPattern.POINTWISE;
        }

        @Override
        public int getNumPartitions() {
            return intermediateResult.getNumberOfAssignedPartitions();
        }

        @Override
        public int getNumSubpartitions(int partitionIndex) {
            // Note that this method should only be called for dynamic graph.This method is used to
            // compute which sub-partitions a consumer vertex should consume, however, for
            // non-dynamic graph it is not needed, and the number of sub-partitions is not decided
            // at this stage, due to the execution edge are not created.
            checkState(
                    intermediateResult.getProducer().getGraph().isDynamic(),
                    "This method should only be called for dynamic graph.");
            return intermediateResult.getPartitions()[partitionIndex].getNumberOfSubpartitions();
        }
    }

    /** Private default constructor to avoid being instantiated. */
    private VertexInputInfoComputationUtils() {}
}
