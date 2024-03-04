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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.ParallelismAndInputInfos;
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Default implementation of {@link VertexParallelismAndInputInfosDecider}. This implementation will
 * decide parallelism and {@link JobVertexInputInfo}s as follows:
 *
 * <p>1. For job vertices whose inputs are all ALL_TO_ALL edges, evenly distribute data to
 * downstream subtasks, make different downstream subtasks consume roughly the same amount of data.
 *
 * <p>2. For other cases, evenly distribute subpartitions to downstream subtasks, make different
 * downstream subtasks consume roughly the same number of subpartitions.
 * 该对象用于描述顶点的并行度和输入数据信息
 */
public class DefaultVertexParallelismAndInputInfosDecider
        implements VertexParallelismAndInputInfosDecider {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultVertexParallelismAndInputInfosDecider.class);

    /**
     * The maximum number of subpartitions belonging to the same result that each task can consume.
     * We currently need this limitation to avoid too many channels in a downstream task leading to
     * poor performance.
     *
     * <p>TODO: Once we support one channel to consume multiple upstream subpartitions in the
     * future, we can remove this limitation
     */
    private static final int MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME = 32768;

    // 全局并行度
    private final int globalMaxParallelism;
    private final int globalMinParallelism;

    // 每个任务的数据量
    private final long dataVolumePerTask;
    private final int globalDefaultSourceParallelism;

    private DefaultVertexParallelismAndInputInfosDecider(
            int globalMaxParallelism,
            int globalMinParallelism,
            MemorySize dataVolumePerTask,
            int globalDefaultSourceParallelism) {

        checkArgument(globalMinParallelism > 0, "The minimum parallelism must be larger than 0.");
        checkArgument(
                globalMaxParallelism >= globalMinParallelism,
                "Maximum parallelism should be greater than or equal to the minimum parallelism.");
        checkArgument(
                globalDefaultSourceParallelism > 0,
                "The default source parallelism must be larger than 0.");
        checkNotNull(dataVolumePerTask);

        this.globalMaxParallelism = globalMaxParallelism;
        this.globalMinParallelism = globalMinParallelism;
        this.dataVolumePerTask = dataVolumePerTask.getBytes();
        this.globalDefaultSourceParallelism = globalDefaultSourceParallelism;
    }

    /**
     * @param jobVertexId The job vertex id  对标一个task
     * @param consumedResults The information of consumed blocking results   该对象记录了范围内的字节数
     * @param vertexInitialParallelism The initial parallelism of the job vertex. If it's a positive
     *     number, it will be respected. If it's not set(equals to {@link
     *     ExecutionConfig#PARALLELISM_DEFAULT}), a parallelism will be automatically decided for
     *     the vertex.
     * @param vertexMaxParallelism The max parallelism of the job vertex.
     * @return
     * 表示上游产生的数据在交给该task时 应当如何分配 使得task下各subtask消费的数据尽可能均匀
     */
    @Override
    public ParallelismAndInputInfos decideParallelismAndInputInfosForVertex(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,  // 该对象记录分区的数据量
            int vertexInitialParallelism,
            int vertexMaxParallelism) {
        checkArgument(
                vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                        || vertexInitialParallelism > 0);
        checkArgument(vertexMaxParallelism > 0 && vertexMaxParallelism >= vertexInitialParallelism);

        if (consumedResults.isEmpty()) {
            // source job vertex
            int parallelism =
                    vertexInitialParallelism > 0
                            ? vertexInitialParallelism
                            // 计算并行度
                            : computeSourceParallelism(jobVertexId, vertexMaxParallelism);
            // 因为是空数据  这里的计算很潦草
            return new ParallelismAndInputInfos(parallelism, Collections.emptyMap());
        } else {

            //
            int minParallelism = globalMinParallelism;
            int maxParallelism = globalMaxParallelism;

            // 调整默认并行度
            if (vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                    && vertexMaxParallelism < minParallelism) {
                LOG.info(
                        "The vertex maximum parallelism {} is smaller than the global minimum parallelism {}. "
                                + "Use {} as the lower bound to decide parallelism of job vertex {}.",
                        vertexMaxParallelism,
                        minParallelism,
                        vertexMaxParallelism,
                        jobVertexId);

                minParallelism = vertexMaxParallelism;
            }
            if (vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                    && vertexMaxParallelism < maxParallelism) {
                LOG.info(
                        "The vertex maximum parallelism {} is smaller than the global maximum parallelism {}. "
                                + "Use {} as the upper bound to decide parallelism of job vertex {}.",
                        vertexMaxParallelism,
                        maxParallelism,
                        vertexMaxParallelism,
                        jobVertexId);
                maxParallelism = vertexMaxParallelism;
            }
            checkState(maxParallelism >= minParallelism);

            if (vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                    && areAllInputsAllToAll(consumedResults)  // 非广播且都是allToAll
                    && !areAllInputsBroadcast(consumedResults)) {
                // 表示按照数据量来划分
                return decideParallelismAndEvenlyDistributeData(
                        jobVertexId,
                        consumedResults,
                        vertexInitialParallelism,
                        minParallelism,
                        maxParallelism);
            } else {
                // 其他情况  或者 decideParallelismAndEvenlyDistributeData无法计算出合适的并行度
                return decideParallelismAndEvenlyDistributeSubpartitions(
                        jobVertexId,
                        consumedResults,
                        vertexInitialParallelism,
                        minParallelism,
                        maxParallelism);
            }
        }
    }

    /**
     * 计算并行度
     * @param jobVertexId
     * @param maxParallelism
     * @return
     */
    private int computeSourceParallelism(JobVertexID jobVertexId, int maxParallelism) {
        if (globalDefaultSourceParallelism > maxParallelism) {
            LOG.info(
                    "The global default source parallelism {} is larger than the maximum parallelism {}. "
                            + "Use {} as the parallelism of source job vertex {}.",
                    globalDefaultSourceParallelism,
                    maxParallelism,
                    maxParallelism,
                    jobVertexId);
            return maxParallelism;
        } else {
            return globalDefaultSourceParallelism;
        }
    }

    /**
     * 表示所有都是  alltoall
     * @param consumedResults
     * @return
     */
    private static boolean areAllInputsAllToAll(List<BlockingResultInfo> consumedResults) {
        return consumedResults.stream().noneMatch(BlockingResultInfo::isPointwise);
    }

    private static boolean areAllInputsBroadcast(List<BlockingResultInfo> consumedResults) {
        return consumedResults.stream().allMatch(BlockingResultInfo::isBroadcast);
    }

    /**
     * Decide parallelism and input infos, which will make the subpartitions be evenly distributed
     * to downstream subtasks, such that different downstream subtasks consume roughly the same
     * number of subpartitions.
     *
     * @param jobVertexId The job vertex id
     * @param consumedResults The information of consumed blocking results
     * @param initialParallelism The initial parallelism of the job vertex
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @return the parallelism and vertex input infos
     * 均匀拆分子分区
     */
    private ParallelismAndInputInfos decideParallelismAndEvenlyDistributeSubpartitions(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int initialParallelism,  // 此时顶点的并行度
            int minParallelism,
            int maxParallelism) {
        checkArgument(!consumedResults.isEmpty());
        int parallelism =
                initialParallelism > 0  // 并行度有效直接使用
                        ? initialParallelism
                        // 按照数据量计算并行度
                        : decideParallelism(
                                jobVertexId, consumedResults, minParallelism, maxParallelism);
        return new ParallelismAndInputInfos(
                parallelism,
                // 根据指定的并行度 计算task的数据消费   这里是理想的每个子分区数据均匀的情况 如果出现数据倾斜 那么每个子任务的负载其实不一样
                VertexInputInfoComputationUtils.computeVertexInputInfos(
                        parallelism, consumedResults, true));
    }

    /**
     * 计算并行度
     * @param jobVertexId
     * @param consumedResults
     * @param minParallelism
     * @param maxParallelism
     * @return
     */
    int decideParallelism(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int minParallelism,
            int maxParallelism) {
        checkArgument(!consumedResults.isEmpty());

        // Considering that the sizes of broadcast results are usually very small, we compute the
        // parallelism only based on sizes of non-broadcast results
        final List<BlockingResultInfo> nonBroadcastResults =
                getNonBroadcastResultInfos(consumedResults);
        if (nonBroadcastResults.isEmpty()) {
            return minParallelism;
        }

        // 产生的总数据量
        long totalBytes =
                nonBroadcastResults.stream()
                        .mapToLong(BlockingResultInfo::getNumBytesProduced)
                        .sum();
        // 除以limit 直接得到并行度
        int parallelism = (int) Math.ceil((double) totalBytes / dataVolumePerTask);
        int minParallelismLimitedByMaxSubpartitions =
                (int)
                        Math.ceil(
                                (double) getMaxNumSubpartitions(nonBroadcastResults)
                                        / MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME);
        parallelism = Math.max(parallelism, minParallelismLimitedByMaxSubpartitions);

        LOG.debug(
                "The total size of non-broadcast data is {}, the initially decided parallelism of job vertex {} is {}.",
                new MemorySize(totalBytes),
                jobVertexId,
                parallelism);

        // 超范围直接修改
        if (parallelism < minParallelism) {
            LOG.info(
                    "The initially decided parallelism {} is smaller than the minimum parallelism {}. "
                            + "Use {} as the finally decided parallelism of job vertex {}.",
                    parallelism,
                    minParallelism,
                    minParallelism,
                    jobVertexId);
            parallelism = minParallelism;
        } else if (parallelism > maxParallelism) {
            LOG.info(
                    "The initially decided parallelism {} is larger than the maximum parallelism {}. "
                            + "Use {} as the finally decided parallelism of job vertex {}.",
                    parallelism,
                    maxParallelism,
                    maxParallelism,
                    jobVertexId);
            parallelism = maxParallelism;
        }

        return parallelism;
    }

    /**
     * Decide parallelism and input infos, which will make the data be evenly distributed to
     * downstream subtasks, such that different downstream subtasks consume roughly the same amount
     * of data.
     *
     * @param jobVertexId The job vertex id
     * @param consumedResults The information of consumed blocking results  这里记录了各分区的数据量
     * @param initialParallelism The initial parallelism of the job vertex
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @return the parallelism and vertex input infos
     * 均匀的分配数据
     */
    private ParallelismAndInputInfos decideParallelismAndEvenlyDistributeData(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int initialParallelism,
            int minParallelism,
            int maxParallelism) {

        // 看来这里期望一开始并行度还没有设定
        checkArgument(initialParallelism == ExecutionConfig.PARALLELISM_DEFAULT);
        checkArgument(!consumedResults.isEmpty());
        consumedResults.forEach(resultInfo -> checkState(!resultInfo.isPointwise()));

        // Considering that the sizes of broadcast results are usually very small, we compute the
        // parallelism and input infos only based on sizes of non-broadcast results

        // 过滤掉广播类型
        final List<BlockingResultInfo> nonBroadcastResults =
                getNonBroadcastResultInfos(consumedResults);

        // 获取子分区数
        int subpartitionNum = checkAndGetSubpartitionNum(nonBroadcastResults);

        // 按子分区分组
        long[] bytesBySubpartition = new long[subpartitionNum];
        Arrays.fill(bytesBySubpartition, 0L);

        // 这里再是将多个 BlockingResultInfo 信息进行累加
        for (BlockingResultInfo resultInfo : nonBroadcastResults) {

            // 按照 AllToAll 规则获得聚合后的结果
            List<Long> subpartitionBytes =
                    ((AllToAllBlockingResultInfo) resultInfo).getAggregatedSubpartitionBytes();
            for (int i = 0; i < subpartitionNum; ++i) {
                bytesBySubpartition[i] += subpartitionBytes.get(i);
            }
        }

        // 获取最大的分区数   虽然这些分区的子分区数一样 但是分区数有多有少
        int maxNumPartitions = getMaxNumPartitions(nonBroadcastResults);

        // 这个是子分区的跨度  一个range不能超过这个数
        // MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME 表示一个task最多消费多少子分区
        int maxRangeSize = MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME / maxNumPartitions;
        // compute subpartition ranges
        List<IndexRange> subpartitionRanges =
                computeSubpartitionRanges(bytesBySubpartition, dataVolumePerTask, maxRangeSize);

        // if the parallelism is not legal, adjust to a legal parallelism
        // subpartitionRanges 就成了并行度
        if (!isLegalParallelism(subpartitionRanges.size(), minParallelism, maxParallelism)) {

            // 并行度不合法时需要调整
            Optional<List<IndexRange>> adjustedSubpartitionRanges =
                    adjustToClosestLegalParallelism(
                            dataVolumePerTask,
                            subpartitionRanges.size(),
                            minParallelism,
                            maxParallelism,
                            Arrays.stream(bytesBySubpartition).min().getAsLong(),
                            Arrays.stream(bytesBySubpartition).sum(),
                            limit -> computeParallelism(bytesBySubpartition, limit, maxRangeSize),
                            limit ->
                                    computeSubpartitionRanges(
                                            bytesBySubpartition, limit, maxRangeSize));

            // 表示 无法计算出合适的并行度
            if (!adjustedSubpartitionRanges.isPresent()) {
                // can't find any legal parallelism, fall back to evenly distribute subpartitions
                LOG.info(
                        "Cannot find a legal parallelism to evenly distribute data for job vertex {}. "
                                + "Fall back to compute a parallelism that can evenly distribute subpartitions.",
                        jobVertexId);
                return decideParallelismAndEvenlyDistributeSubpartitions(
                        jobVertexId,
                        consumedResults,
                        initialParallelism,
                        minParallelism,
                        maxParallelism);
            }
            subpartitionRanges = adjustedSubpartitionRanges.get();
        }

        checkState(isLegalParallelism(subpartitionRanges.size(), minParallelism, maxParallelism));
        // 合法的情况下这就是结果了
        return createParallelismAndInputInfos(consumedResults, subpartitionRanges);
    }

    private static boolean isLegalParallelism(
            int parallelism, int minParallelism, int maxParallelism) {
        return parallelism >= minParallelism && parallelism <= maxParallelism;
    }

    /**
     * 获取子分区数量
     * @param consumedResults
     * @return
     */
    private static int checkAndGetSubpartitionNum(List<BlockingResultInfo> consumedResults) {
        // 每个分区的子分区数 应当一样
        final Set<Integer> subpartitionNumSet =
                consumedResults.stream()
                        .flatMap(
                                resultInfo ->
                                        IntStream.range(0, resultInfo.getNumPartitions())
                                                .boxed()
                                                .map(resultInfo::getNumSubpartitions))
                        .collect(Collectors.toSet());
        // all partitions have the same subpartition num
        checkState(subpartitionNumSet.size() == 1);
        return subpartitionNumSet.iterator().next();
    }

    /**
     * Adjust the parallelism to the closest legal parallelism and return the computed subpartition
     * ranges.
     *
     * @param currentDataVolumeLimit current data volume limit  当前每个并行值(task) 消费的字节数上限
     * @param currentParallelism current parallelism   当前并行度  (此时并行度不合法)
     * @param minParallelism the min parallelism   最小并行度
     * @param maxParallelism the max parallelism   最大并行度
     * @param minLimit the minimum data volume limit  单个分区消费的最小量
     * @param maxLimit the maximum data volume limit  消费最大量
     * @param parallelismComputer a function to compute the parallelism according to the data volume
     *     limit
     * @param subpartitionRangesComputer a function to compute the subpartition ranges according to
     *     the data volume limit
     * @return the computed subpartition ranges or {@link Optional#empty()} if we can't find any
     *     legal parallelism
     *     当并行度不合法时 要进行调整
     */
    private static Optional<List<IndexRange>> adjustToClosestLegalParallelism(
            long currentDataVolumeLimit,
            int currentParallelism,
            int minParallelism,
            int maxParallelism,
            long minLimit,
            long maxLimit,
            Function<Long, Integer> parallelismComputer,
            Function<Long, List<IndexRange>> subpartitionRangesComputer) {
        long adjustedDataVolumeLimit = currentDataVolumeLimit;

        // 表示并行度太小
        if (currentParallelism < minParallelism) {
            // Current parallelism is smaller than the user-specified lower-limit of parallelism ,
            // we need to adjust it to the closest/minimum possible legal parallelism. That is, we
            // need to find the maximum legal dataVolumeLimit.
            // 调整每个task的消费量  现在要降低
            adjustedDataVolumeLimit =
                    BisectionSearchUtils.findMaxLegalValue(
                            // 按照最小的子分区数据量计算产生的并行度
                            value -> parallelismComputer.apply(value) >= minParallelism,
                            minLimit,
                            currentDataVolumeLimit);

            // When we find the minimum possible legal parallelism, the dataVolumeLimit that can
            // lead to this parallelism may be a range, and we need to find the minimum value of
            // this range to make the data distribution as even as possible (the smaller the
            // dataVolumeLimit, the more even the distribution)

            // 调整后得到了一个变大的并行度 但是还是尽可能小  换句话每个任务的消费量尽可能大
            final long minPossibleLegalParallelism =
                    parallelismComputer.apply(adjustedDataVolumeLimit);


            // 计算出此时的数据消费量
            adjustedDataVolumeLimit =
                    BisectionSearchUtils.findMinLegalValue(
                            value ->
                                    parallelismComputer.apply(value) == minPossibleLegalParallelism,
                            minLimit,
                            adjustedDataVolumeLimit);

            // 计算出的并行度太多 往上调整消费量
        } else if (currentParallelism > maxParallelism) {
            // Current parallelism is larger than the user-specified upper-limit of parallelism ,
            // we need to adjust it to the closest/maximum possible legal parallelism. That is, we
            // need to find the minimum legal dataVolumeLimit.
            adjustedDataVolumeLimit =
                    BisectionSearchUtils.findMinLegalValue(
                            value -> parallelismComputer.apply(value) <= maxParallelism,
                            currentDataVolumeLimit,
                            maxLimit);
        }

        // 得到调整后的并行度
        int adjustedParallelism = parallelismComputer.apply(adjustedDataVolumeLimit);
        // 合法后产生结果
        if (isLegalParallelism(adjustedParallelism, minParallelism, maxParallelism)) {
            // 基于新的limit 生成range
            return Optional.of(subpartitionRangesComputer.apply(adjustedDataVolumeLimit));
        } else {
            return Optional.empty();
        }
    }

    /**
     * 创建输入的并行信息
     * @param consumedResults   这里记录了字节数
     * @param subpartitionRanges  已经划分好每个子任务 消费的子分区范围   注意这里的子分区数据以及是将各个分区相同子分区数据合并后的结果了
     * @return
     */
    private static ParallelismAndInputInfos createParallelismAndInputInfos(
            List<BlockingResultInfo> consumedResults, List<IndexRange> subpartitionRanges) {

        // 描述数据集会怎么拆分 用于被下游消费
        final Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        consumedResults.forEach(
                resultInfo -> {
                    // 这个是分区数量
                    int sourceParallelism = resultInfo.getNumPartitions();
                    IndexRange partitionRange = new IndexRange(0, sourceParallelism - 1);

                    List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
                    for (int i = 0; i < subpartitionRanges.size(); ++i) {
                        IndexRange subpartitionRange;
                        // 广播模式此时不应该考虑
                        if (resultInfo.isBroadcast()) {
                            subpartitionRange = new IndexRange(0, 0);
                        } else {
                            subpartitionRange = subpartitionRanges.get(i);
                        }

                        // 表示消费了所有分区下 这些子分区的数据
                        ExecutionVertexInputInfo executionVertexInputInfo =
                                new ExecutionVertexInputInfo(i, partitionRange, subpartitionRange);
                        executionVertexInputInfos.add(executionVertexInputInfo);
                    }

                    vertexInputInfos.put(
                            resultInfo.getResultId(),
                            new JobVertexInputInfo(executionVertexInputInfos));
                });
        return new ParallelismAndInputInfos(subpartitionRanges.size(), vertexInputInfos);
    }

    /**
     * 计算范围
     * @param nums   每个子分区的字节数
     * @param limit  range的字节量限制
     * @param maxRangeSize  表示range不能横跨超过多少子分区
     * @return
     */
    private static List<IndexRange> computeSubpartitionRanges(
            long[] nums, long limit, int maxRangeSize) {
        List<IndexRange> subpartitionRanges = new ArrayList<>();
        long tmpSum = 0;
        int startIndex = 0;
        for (int i = 0; i < nums.length; ++i) {

            // 得到某个子分区的字节数
            long num = nums[i];
            if (i == startIndex
                    || (tmpSum + num <= limit && (i - startIndex + 1) <= maxRangeSize)) {
                tmpSum += num;
            } else {
                // 计算出一个子分区下标范围
                subpartitionRanges.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                tmpSum = num;
            }
        }
        // 剩下的作为一个返回
        subpartitionRanges.add(new IndexRange(startIndex, nums.length - 1));
        return subpartitionRanges;
    }

    /**
     * 计算并行度
     * @param nums
     * @param limit
     * @param maxRangeSize
     * @return
     */
    private static int computeParallelism(long[] nums, long limit, int maxRangeSize) {
        long tmpSum = 0;
        int startIndex = 0;
        int count = 1;
        for (int i = 0; i < nums.length; ++i) {
            long num = nums[i];
            if (i == startIndex
                    || (tmpSum + num <= limit && (i - startIndex + 1) <= maxRangeSize)) {
                tmpSum += num;
            } else {
                startIndex = i;
                tmpSum = num;
                count += 1;
            }
        }
        return count;
    }

    /**
     * 获取最大分区数
     * @param consumedResults
     * @return
     */
    private static int getMaxNumPartitions(List<BlockingResultInfo> consumedResults) {
        checkArgument(!consumedResults.isEmpty());
        return consumedResults.stream()
                .mapToInt(BlockingResultInfo::getNumPartitions)
                .max()
                .getAsInt();
    }

    /**
     * 获取最大子分区数
     * @param consumedResults
     * @return
     */
    private static int getMaxNumSubpartitions(List<BlockingResultInfo> consumedResults) {
        checkArgument(!consumedResults.isEmpty());
        return consumedResults.stream()
                .mapToInt(
                        resultInfo ->
                                IntStream.range(0, resultInfo.getNumPartitions())
                                        .boxed()
                                        .mapToInt(resultInfo::getNumSubpartitions)
                                        .sum())
                .max()
                .getAsInt();
    }

    private static List<BlockingResultInfo> getNonBroadcastResultInfos(
            List<BlockingResultInfo> consumedResults) {
        return consumedResults.stream()
                .filter(resultInfo -> !resultInfo.isBroadcast())
                .collect(Collectors.toList());
    }

    static DefaultVertexParallelismAndInputInfosDecider from(
            int maxParallelism, Configuration configuration) {
        return new DefaultVertexParallelismAndInputInfosDecider(
                maxParallelism,
                configuration.getInteger(
                        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM),
                configuration.get(
                        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK),
                configuration.get(
                        BatchExecutionOptions
                                .ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM));
    }
}
