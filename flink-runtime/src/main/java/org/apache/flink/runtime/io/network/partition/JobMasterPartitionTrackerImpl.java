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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Utility for tracking partitions and issuing release calls to task executors and shuffle masters.
 * 该对象基于ResourceID跟踪
 */
public class JobMasterPartitionTrackerImpl
        extends AbstractPartitionTracker<ResourceID, ResultPartitionDeploymentDescriptor>
        implements JobMasterPartitionTracker {

    // Besides below fields, JobMasterPartitionTrackerImpl inherits 'partitionTable' and
    // 'partitionInfos' from parent and tracks partitions from different dimensions:
    // 'partitionTable' tracks partitions which occupie local resource on TM;
    // 'partitionInfos' tracks all available partitions no matter they are accommodated
    // externally on remote or internally on TM;

    private final JobID jobId;

    /**
     * 洗牌对象
     */
    private final ShuffleMaster<?> shuffleMaster;

    /**
     * 通过该对象可以找到对应的TM网关  用于访问 TM
     */
    private final PartitionTrackerFactory.TaskExecutorGatewayLookup taskExecutorGatewayLookup;

    /**
     * 用于访问RM
     */
    private ResourceManagerGateway resourceManagerGateway;

    /**
     * 每个中间结果集会关联多个分区      每个分区关联一个ShuffleDescriptor
     */
    private final Map<IntermediateDataSetID, List<ShuffleDescriptor>>
            clusterPartitionShuffleDescriptors;

    public JobMasterPartitionTrackerImpl(
            JobID jobId,
            ShuffleMaster<?> shuffleMaster,
            PartitionTrackerFactory.TaskExecutorGatewayLookup taskExecutorGatewayLookup) {

        this.jobId = Preconditions.checkNotNull(jobId);
        this.shuffleMaster = Preconditions.checkNotNull(shuffleMaster);
        this.taskExecutorGatewayLookup = taskExecutorGatewayLookup;
        this.clusterPartitionShuffleDescriptors = new HashMap<>();
    }

    /**
     * 添加维护关系
     * @param producingTaskExecutorId ID of task executor on which the partition is produced  产生该分区数据的生产者
     * @param resultPartitionDeploymentDescriptor deployment descriptor of the partition   被产生的某个分区数据
     */
    @Override
    public void startTrackingPartition(
            ResourceID producingTaskExecutorId,
            ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
        Preconditions.checkNotNull(producingTaskExecutorId);
        Preconditions.checkNotNull(resultPartitionDeploymentDescriptor);

        // non-releaseByScheduler partitions don't require explicit partition release calls.
        // 只针对该类型有效
        if (!resultPartitionDeploymentDescriptor.getPartitionType().isReleaseByScheduler()) {
            return;
        }

        // 针对的分区id
        final ResultPartitionID resultPartitionId =
                resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID();

        startTrackingPartition(
                producingTaskExecutorId, resultPartitionId, resultPartitionDeploymentDescriptor);
    }

    /**
     * 添加关联关系
     * @param key
     * @param resultPartitionId
     * @param metaInfo
     */
    @Override
    void startTrackingPartition(
            ResourceID key,
            ResultPartitionID resultPartitionId,
            ResultPartitionDeploymentDescriptor metaInfo) {
        // A partition is registered into 'partitionTable' only when it occupies
        // resource on the corresponding TM;

        // TODO 满足某个条件时 才加入到 partitionTable
        if (metaInfo.getShuffleDescriptor().storesLocalResourcesOn().isPresent()) {
            partitionTable.startTrackingPartitions(
                    key, Collections.singletonList(resultPartitionId));
        }
        partitionInfos.put(resultPartitionId, new PartitionInfo<>(key, metaInfo));
    }

    /**
     * 移除一组分区数据
     * @param resultPartitionIds
     * @param releaseOnShuffleMaster
     */
    @Override
    public void stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> resultPartitionIds, boolean releaseOnShuffleMaster) {
        stopTrackingAndHandlePartitions(
                resultPartitionIds,
                (tmID, partitionDescs) ->  // TM的id 被分配到该TM上的一组分区结果
                        internalReleasePartitions(tmID, partitionDescs, releaseOnShuffleMaster));
    }

    /**
     * 停止追踪这些分区  并进行推进
     * @param resultPartitionIds ID of the partition containing both job partitions and cluster
     *     partitions.
     * @return
     */
    @Override
    public CompletableFuture<Void> stopTrackingAndPromotePartitions(
            Collection<ResultPartitionID> resultPartitionIds) {
        List<CompletableFuture<Acknowledge>> promoteFutures = new ArrayList<>();
        stopTrackingAndHandlePartitions(
                resultPartitionIds,
                (tmID, partitionDescs) ->
                        promoteFutures.add(
                                // 在TM上推进
                                internalPromotePartitionsOnTaskExecutor(tmID, partitionDescs)));
        return FutureUtils.completeAll(promoteFutures);
    }

    @Override
    public Collection<ResultPartitionDeploymentDescriptor> getAllTrackedPartitions() {
        return partitionInfos.values().stream().map(PartitionInfo::getMetaInfo).collect(toList());
    }

    @Override
    public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
        this.resourceManagerGateway = resourceManagerGateway;
    }

    /**
     * 从RM上获取洗牌信息
     * @param intermediateDataSetID
     * @return
     */
    @Override
    public List<ShuffleDescriptor> getClusterPartitionShuffleDescriptors(
            IntermediateDataSetID intermediateDataSetID) {
        return clusterPartitionShuffleDescriptors.computeIfAbsent(
                intermediateDataSetID, this::requestShuffleDescriptorsFromResourceManager);
    }

    private List<ShuffleDescriptor> requestShuffleDescriptorsFromResourceManager(
            IntermediateDataSetID intermediateDataSetID) {
        Preconditions.checkNotNull(
                resourceManagerGateway, "JobMaster is not connected to ResourceManager");
        try {
            return this.resourceManagerGateway
                    .getClusterPartitionsShuffleDescriptors(intermediateDataSetID)
                    .get();
        } catch (Throwable e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to get shuffle descriptors of intermediate dataset %s from ResourceManager",
                            intermediateDataSetID),
                    e);
        }
    }

    /**
     * 移除一组分区数据
     * @param resultPartitionIds
     * @param partitionHandler
     */
    private void stopTrackingAndHandlePartitions(
            Collection<ResultPartitionID> resultPartitionIds,
            BiConsumer<ResourceID, Collection<ResultPartitionDeploymentDescriptor>>
                    partitionHandler) {
        Preconditions.checkNotNull(resultPartitionIds);

        // stop tracking partitions to handle and group them by task executor ID
        Map<ResourceID, List<ResultPartitionDeploymentDescriptor>> partitionsToReleaseByResourceId =
                // 移除关联关系  并将分区按照TM分组
                stopTrackingPartitions(resultPartitionIds).stream()
                        .collect(
                                Collectors.groupingBy(
                                        PartitionTrackerEntry::getKey,
                                        Collectors.mapping(
                                                PartitionTrackerEntry::getMetaInfo, toList())));

        partitionsToReleaseByResourceId.forEach(partitionHandler);
    }

    /**
     * 释放分区信息
     * @param potentialPartitionLocation  这组分区所在的TM
     * @param partitionDeploymentDescriptors  这组分区信息
     * @param releaseOnShuffleMaster
     */
    private void internalReleasePartitions(
            ResourceID potentialPartitionLocation,
            Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors,
            boolean releaseOnShuffleMaster) {

        internalReleasePartitionsOnTaskExecutor(
                potentialPartitionLocation, partitionDeploymentDescriptors);
        if (releaseOnShuffleMaster) {
            internalReleasePartitionsOnShuffleMaster(partitionDeploymentDescriptors.stream());
        }
    }

    /**
     * 在TM上推进分区
     * @param potentialPartitionLocation
     * @param clusterPartitionDeploymentDescriptors
     * @return
     */
    private CompletableFuture<Acknowledge> internalPromotePartitionsOnTaskExecutor(
            ResourceID potentialPartitionLocation,
            Collection<ResultPartitionDeploymentDescriptor> clusterPartitionDeploymentDescriptors) {

        // 找到待处理的分区
        final Set<ResultPartitionID> partitionsRequiringRpcPromoteCalls =
                clusterPartitionDeploymentDescriptors.stream()
                        .filter(JobMasterPartitionTrackerImpl::isPartitionWithLocalResources)
                        .map(JobMasterPartitionTrackerImpl::getResultPartitionId)
                        .collect(Collectors.toSet());

        if (!partitionsRequiringRpcPromoteCalls.isEmpty()) {
            final TaskExecutorGateway taskExecutorGateway =
                    taskExecutorGatewayLookup.lookup(potentialPartitionLocation).orElse(null);

            if (taskExecutorGateway != null) {
                return taskExecutorGateway.promotePartitions(
                        jobId, partitionsRequiringRpcPromoteCalls);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 释放分区信息
     * @param potentialPartitionLocation
     * @param partitionDeploymentDescriptors
     */
    private void internalReleasePartitionsOnTaskExecutor(
            ResourceID potentialPartitionLocation,
            Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {

        final Set<ResultPartitionID> partitionsRequiringRpcReleaseCalls =
                partitionDeploymentDescriptors.stream()
                        // 表示这组分区有关联资源
                        .filter(JobMasterPartitionTrackerImpl::isPartitionWithLocalResources)
                        .map(JobMasterPartitionTrackerImpl::getResultPartitionId)
                        .collect(Collectors.toSet());

        if (!partitionsRequiringRpcReleaseCalls.isEmpty()) {
            taskExecutorGatewayLookup
                    .lookup(potentialPartitionLocation)
                    .ifPresent(
                            taskExecutorGateway ->
                                    // 手动调用释放分区 确保释放资源
                                    taskExecutorGateway.releasePartitions(
                                            jobId, partitionsRequiringRpcReleaseCalls));
        }
    }

    /**
     * 处理一组分区
     * @param partitionDeploymentDescriptors
     */
    private void internalReleasePartitionsOnShuffleMaster(
            Stream<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {
        partitionDeploymentDescriptors
                .map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)  // 获取洗牌信息 并释放外部资源
                .forEach(shuffleMaster::releasePartitionExternally);
    }

    private static boolean isPartitionWithLocalResources(
            ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
        return resultPartitionDeploymentDescriptor
                .getShuffleDescriptor()
                .storesLocalResourcesOn()
                .isPresent();
    }

    private static ResultPartitionID getResultPartitionId(
            ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
        return resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID();
    }
}
