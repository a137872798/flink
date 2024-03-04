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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Default {@link ResourceManagerPartitionTracker} implementation.
 *
 * <p>Internal tracking info must only be updated upon reception of a {@link
 * ClusterPartitionReport}, as the task executor state is the source of truth.
 */
public class ResourceManagerPartitionTrackerImpl implements ResourceManagerPartitionTracker {

    private static final Logger LOG =
            LoggerFactory.getLogger(ResourceManagerPartitionTrackerImpl.class);

    // 资源id 与工作集 是多对多的关系

    /**
     * 一个资源id 对应一组中间结果集  每个中间结果集又可以关联多个分区  表示结果集落在多个分区
     */
    private final Map<ResourceID, Set<IntermediateDataSetID>> taskExecutorToDataSets =
            new HashMap<>();
    private final Map<IntermediateDataSetID, Map<ResourceID, Set<ResultPartitionID>>>
            dataSetToTaskExecutors = new HashMap<>();

    /**
     * 维护数据集信息
     */
    private final Map<IntermediateDataSetID, DataSetMetaInfo> dataSetMetaInfo = new HashMap<>();
    private final Map<IntermediateDataSetID, CompletableFuture<Void>>
            partitionReleaseCompletionFutures = new HashMap<>();

    /**
     * 该对象提供释放资源的接口
     */
    private final TaskExecutorClusterPartitionReleaser taskExecutorClusterPartitionReleaser;

    public ResourceManagerPartitionTrackerImpl(
            TaskExecutorClusterPartitionReleaser taskExecutorClusterPartitionReleaser) {
        this.taskExecutorClusterPartitionReleaser = taskExecutorClusterPartitionReleaser;
    }

    /**
     * 建立映射关系
     * @param taskExecutorId origin of the report
     * @param clusterPartitionReport partition report  报告对象 可以理解为bean
     */
    @Override
    public void processTaskExecutorClusterPartitionReport(
            ResourceID taskExecutorId, ClusterPartitionReport clusterPartitionReport) {
        Preconditions.checkNotNull(taskExecutorId);
        Preconditions.checkNotNull(clusterPartitionReport);
        LOG.debug(
                "Processing cluster partition report from task executor {}: {}.",
                taskExecutorId,
                clusterPartitionReport);

        internalProcessClusterPartitionReport(taskExecutorId, clusterPartitionReport);
    }

    /**
     * 通知某个TM下线了
     * @param taskExecutorId task executor that shut down
     */
    @Override
    public void processTaskExecutorShutdown(ResourceID taskExecutorId) {
        Preconditions.checkNotNull(taskExecutorId);
        LOG.debug("Processing shutdown of task executor {}.", taskExecutorId);

        internalProcessClusterPartitionReport(
                taskExecutorId, new ClusterPartitionReport(Collections.emptyList()));
    }


    /**
     * 释放某个数据集相关的数据
     * @param dataSetId data set to release
     * @return
     */
    @Override
    public CompletableFuture<Void> releaseClusterPartitions(IntermediateDataSetID dataSetId) {
        Preconditions.checkNotNull(dataSetId);
        if (!dataSetMetaInfo.containsKey(dataSetId)) {
            LOG.debug("Attempted released of unknown data set {}.", dataSetId);
            return CompletableFuture.completedFuture(null);
        }
        LOG.debug("Releasing cluster partitions for data set {}.", dataSetId);

        // 设置一个future对象 看来是异步操作
        CompletableFuture<Void> partitionReleaseCompletionFuture =
                partitionReleaseCompletionFutures.computeIfAbsent(
                        dataSetId, ignored -> new CompletableFuture<>());

        // 通过数据集反查相关的资源id   并借助TaskExecutorClusterPartitionReleaser 释放资源
        internalReleasePartitions(Collections.singleton(dataSetId));
        return partitionReleaseCompletionFuture;
    }

    @Override
    public List<ShuffleDescriptor> getClusterPartitionShuffleDescriptors(
            IntermediateDataSetID dataSetID) {
        final DataSetMetaInfo dataSetMetaInfo = this.dataSetMetaInfo.get(dataSetID);
        if (dataSetMetaInfo == null) {
            return Collections.emptyList();
        }

        // 获取内部的洗牌信息
        return new ArrayList<>(dataSetMetaInfo.getShuffleDescriptors().values());
    }

    /**
     * 记录某个TM 此时的分区信息
     * @param taskExecutorId
     * @param clusterPartitionReport
     */
    private void internalProcessClusterPartitionReport(
            ResourceID taskExecutorId, ClusterPartitionReport clusterPartitionReport) {

        // 处理丢失的分区数据
        final Set<IntermediateDataSetID> dataSetsWithLostPartitions =
                clusterPartitionReport.getEntries().isEmpty()
                        // 本次清空 所以之前的都是多出来的
                        ? processEmptyReport(taskExecutorId)
                        : setHostedDataSetsAndCheckCorruption(
                                taskExecutorId, clusterPartitionReport.getEntries());

        // 更新数据集元数据
        updateDataSetMetaData(clusterPartitionReport);

        // 移除相关数据
        checkForFullyLostDatasets(dataSetsWithLostPartitions);

        internalReleasePartitions(dataSetsWithLostPartitions);
    }

    /**
     * 移除某些数据集
     * @param dataSetsToRelease
     */
    private void internalReleasePartitions(Set<IntermediateDataSetID> dataSetsToRelease) {
        // 根据dataSetsToRelease信息  读取并建立releaseCalls   找到这些数据集相关的资源
        Map<ResourceID, Set<IntermediateDataSetID>> releaseCalls =
                prepareReleaseCalls(dataSetsToRelease);
        // 挨个释放资源
        releaseCalls.forEach(taskExecutorClusterPartitionReleaser::releaseClusterPartitions);
    }

    /**
     * 表示taskExecutorId 相关的报告数据为空
     * @param taskExecutorId
     * @return
     */
    private Set<IntermediateDataSetID> processEmptyReport(ResourceID taskExecutorId) {
        Set<IntermediateDataSetID> previouslyHostedDatasets =
                taskExecutorToDataSets.remove(taskExecutorId);
        if (previouslyHostedDatasets == null) {
            // default path for task executors that never have any cluster partitions
            previouslyHostedDatasets = Collections.emptySet();
        } else {
            // 删除数据
            previouslyHostedDatasets.forEach(
                    // 删除某个结果集相关的某个资源相关的一组分区id
                    dataSetId -> removeInnerKey(dataSetId, taskExecutorId, dataSetToTaskExecutors));
        }
        return previouslyHostedDatasets;
    }

    /**
     * Updates the data sets for which the given task executor is hosting partitions and returns
     * data sets that were corrupted due to a loss of partitions.
     *
     * @param taskExecutorId ID of the hosting TaskExecutor
     * @param reportEntries IDs of data sets for which partitions are hosted
     * @return corrupted data sets
     * 处理报告数据
     */
    private Set<IntermediateDataSetID> setHostedDataSetsAndCheckCorruption(
            ResourceID taskExecutorId,
            Collection<ClusterPartitionReport.ClusterPartitionReportEntry> reportEntries) {

        // 表示当前持有的结果集
        final Set<IntermediateDataSetID> currentlyHostedDatasets =
                reportEntries.stream()
                        .map(ClusterPartitionReport.ClusterPartitionReportEntry::getDataSetId)
                        .collect(Collectors.toSet());

        // 添加关联关系
        final Set<IntermediateDataSetID> previouslyHostedDataSets =
                taskExecutorToDataSets.put(taskExecutorId, currentlyHostedDatasets);

        // previously tracked data sets may be corrupted since we may be tracking less partitions
        // than before
        final Set<IntermediateDataSetID> potentiallyCorruptedDataSets =
                Optional.ofNullable(previouslyHostedDataSets).orElse(new HashSet<>(0));

        // update data set -> task executor mapping and find datasets for which lost a partition
        reportEntries.forEach(
                hostedPartition -> {
                    final Map<ResourceID, Set<ResultPartitionID>> taskExecutorHosts =
                            dataSetToTaskExecutors.computeIfAbsent(
                                    hostedPartition.getDataSetId(), ignored -> new HashMap<>());

                    // 添加映射关系
                    final Set<ResultPartitionID> previouslyHostedPartitions =
                            taskExecutorHosts.put(
                                    taskExecutorId, hostedPartition.getHostedPartitions());

                    // 当新数据完全覆盖了之前数据时  那么就没有数据丢失  少了某个分区就意味着数据丢失
                    final boolean noPartitionLost =
                            previouslyHostedPartitions == null
                                    || hostedPartition
                                            .getHostedPartitions()
                                            .containsAll(previouslyHostedPartitions);
                    if (noPartitionLost) {
                        // 没有丢失所以可以从 potentiallyCorruptedDataSets 移除
                        potentiallyCorruptedDataSets.remove(hostedPartition.getDataSetId());
                    }
                });

        // now only contains data sets for which a partition is no longer tracked
        // 本次不再携带的数据集信息 可以能已经损坏
        return potentiallyCorruptedDataSets;
    }

    /**
     * 更新数据集的元数据信息
     * @param clusterPartitionReport
     */
    private void updateDataSetMetaData(ClusterPartitionReport clusterPartitionReport) {
        // add meta info for new data sets
        clusterPartitionReport
                .getEntries()
                .forEach(
                        entry ->
                                dataSetMetaInfo.compute(
                                        entry.getDataSetId(),
                                        (dataSetID, dataSetMetaInfo) -> {
                                            // 表示之前不存在
                                            if (dataSetMetaInfo == null) {
                                                return DataSetMetaInfo
                                                        .withoutNumRegisteredPartitions(
                                                                entry.getNumTotalPartitions())
                                                        .addShuffleDescriptors(
                                                                entry.getShuffleDescriptors());
                                            } else {
                                                // double check that the meta data is consistent
                                                // 检验是否一致
                                                Preconditions.checkState(
                                                        dataSetMetaInfo.getNumTotalPartitions()
                                                                == entry.getNumTotalPartitions());
                                                return dataSetMetaInfo.addShuffleDescriptors(
                                                        entry.getShuffleDescriptors());
                                            }
                                        }));
    }

    /**
     * 这些数据集id在本次更新时丢失
     * @param dataSetsWithLostPartitions
     */
    private void checkForFullyLostDatasets(Set<IntermediateDataSetID> dataSetsWithLostPartitions) {
        dataSetsWithLostPartitions.forEach(
                dataSetId -> {
                    // 该数据集所在的TM 已经下线了
                    if (getHostingTaskExecutors(dataSetId).isEmpty()) {
                        LOG.debug(
                                "There are no longer partitions being tracked for dataset {}.",
                                dataSetId);
                        // 从相关容器移除    并为future设置结果
                        dataSetMetaInfo.remove(dataSetId);
                        Optional.ofNullable(partitionReleaseCompletionFutures.remove(dataSetId))
                                .map(future -> future.complete(null));
                    }
                });
    }

    /**
     * 准备移除某些数据集
     * @param dataSetsToRelease
     * @return
     */
    private Map<ResourceID, Set<IntermediateDataSetID>> prepareReleaseCalls(
            Set<IntermediateDataSetID> dataSetsToRelease) {
        final Map<ResourceID, Set<IntermediateDataSetID>> releaseCalls = new HashMap<>();
        dataSetsToRelease.forEach(
                dataSetToRelease -> {

                    // 每个数据集id 可以找到一组资源id
                    final Set<ResourceID> hostingTaskExecutors =
                            getHostingTaskExecutors(dataSetToRelease);
                    hostingTaskExecutors.forEach(
                            hostingTaskExecutor ->
                                    // 这里又建立反向映射
                                    insert(hostingTaskExecutor, dataSetToRelease, releaseCalls));
                });
        return releaseCalls;
    }

    /**
     *
     * @param dataSetId
     * @return
     */
    private Set<ResourceID> getHostingTaskExecutors(IntermediateDataSetID dataSetId) {
        Preconditions.checkNotNull(dataSetId);

        // 找到相关数据
        Map<ResourceID, Set<ResultPartitionID>> trackedPartitions =
                dataSetToTaskExecutors.get(dataSetId);
        if (trackedPartitions == null) {
            return Collections.emptySet();
        } else {
            return trackedPartitions.keySet();
        }
    }

    /**
     * 列举数据集信息
     * @return
     */
    @Override
    public Map<IntermediateDataSetID, DataSetMetaInfo> listDataSets() {
        return dataSetMetaInfo.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> {
                                    // 该数据集相关的信息
                                    final Map<ResourceID, Set<ResultPartitionID>>
                                            taskExecutorToPartitions =
                                                    dataSetToTaskExecutors.get(entry.getKey());
                                    Preconditions.checkState(
                                            taskExecutorToPartitions != null,
                                            "Have metadata entry for dataset %s, but no partition is tracked.",
                                            entry.getKey());

                                    // dataSetToTaskExecutors 信息是TM注册到RM上才会设置的
                                    // 有关该数据集每个TM相关的分区数和与中间结果集的总分区数是一样的
                                    // numTrackedPartitions 表示现在已经有多少注册上来了
                                    int numTrackedPartitions = 0;
                                    for (Set<ResultPartitionID> hostedPartitions :
                                            taskExecutorToPartitions.values()) {
                                        numTrackedPartitions += hostedPartitions.size();
                                    }

                                    // 基于最新的信息产生 DataSetMetaInfo
                                    return DataSetMetaInfo.withNumRegisteredPartitions(
                                            numTrackedPartitions,
                                            entry.getValue().getNumTotalPartitions());
                                }));
    }

    /**
     * Returns whether all maps are empty; used for checking for resource leaks in case entries
     * aren't properly removed.
     *
     * @return whether all contained maps are empty
     */
    @VisibleForTesting
    boolean areAllMapsEmpty() {
        return taskExecutorToDataSets.isEmpty()
                && dataSetToTaskExecutors.isEmpty()
                && dataSetMetaInfo.isEmpty()
                && partitionReleaseCompletionFutures.isEmpty();
    }

    private static <K, V> void insert(K key1, V value, Map<K, Set<V>> collection) {
        collection.compute(
                key1,
                (key, values) -> {
                    if (values == null) {
                        values = new HashSet<>();
                    }
                    values.add(value);
                    return values;
                });
    }

    /**
     * 删除数据
     * @param key1
     * @param value
     * @param collection
     * @param <K1>
     * @param <K2>
     * @param <V>
     */
    private static <K1, K2, V> void removeInnerKey(
            K1 key1, K2 value, Map<K1, Map<K2, V>> collection) {
        collection.computeIfPresent(
                key1,
                (key, values) -> {
                    values.remove(value);
                    if (values.isEmpty()) {
                        return null;
                    }
                    return values;
                });
    }
}
