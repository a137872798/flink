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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility for tracking partitions and issuing release calls to task executors and shuffle masters.
 * 这里确定了追踪的数据类型
 */
public class TaskExecutorPartitionTrackerImpl
        extends AbstractPartitionTracker<JobID, TaskExecutorPartitionInfo>
        implements TaskExecutorPartitionTracker {

    private static final Logger LOG =
            LoggerFactory.getLogger(TaskExecutorPartitionTrackerImpl.class);

    /**
     * 存放升级后的数据
     */
    private final Map<IntermediateDataSetID, DataSetEntry> clusterPartitions = new HashMap<>();

    /**
     * 用于洗牌的
     */
    private final ShuffleEnvironment<?, ?> shuffleEnvironment;

    public TaskExecutorPartitionTrackerImpl(ShuffleEnvironment<?, ?> shuffleEnvironment) {
        this.shuffleEnvironment = shuffleEnvironment;
    }

    /**
     * 追加追踪信息
     * @param producingJobId ID of job by which the partition is produced
     * @param partitionInfo information about the partition
     */
    @Override
    public void startTrackingPartition(
            JobID producingJobId, TaskExecutorPartitionInfo partitionInfo) {
        Preconditions.checkNotNull(producingJobId);
        Preconditions.checkNotNull(partitionInfo);

        startTrackingPartition(producingJobId, partitionInfo.getResultPartitionId(), partitionInfo);
    }

    /**
     * 解除维护关系
     * @param partitionsToRelease
     */
    @Override
    public void stopTrackingAndReleaseJobPartitions(
            Collection<ResultPartitionID> partitionsToRelease) {
        LOG.debug("Releasing Job Partitions {}", partitionsToRelease);
        if (partitionsToRelease.isEmpty()) {
            return;
        }

        stopTrackingPartitions(partitionsToRelease);
        // 当不再维护这些分区时  释放相关资源
        shuffleEnvironment.releasePartitionsLocally(partitionsToRelease);
    }

    @Override
    public void stopTrackingAndReleaseJobPartitionsFor(JobID producingJobId) {

        // 放弃某个job时 会得到相关的一组分区
        Collection<ResultPartitionID> partitionsForJob =
                CollectionUtil.project(
                        stopTrackingPartitionsFor(producingJobId),
                        PartitionTrackerEntry::getResultPartitionId);
        LOG.debug("Releasing Job Partitions {} for job {}", partitionsForJob, producingJobId);
        // 释放这些分区相关的本地资源
        shuffleEnvironment.releasePartitionsLocally(partitionsForJob);
    }

    @Override
    public void promoteJobPartitions(Collection<ResultPartitionID> partitionsToPromote) {
        LOG.debug("Promoting Job Partitions {}", partitionsToPromote);

        if (partitionsToPromote.isEmpty()) {
            return;
        }

        // 得到一组需要解除的对象
        final Collection<PartitionTrackerEntry<JobID, TaskExecutorPartitionInfo>>
                partitionTrackerEntries = stopTrackingPartitions(partitionsToPromote);

        for (PartitionTrackerEntry<JobID, TaskExecutorPartitionInfo> partitionTrackerEntry :
                partitionTrackerEntries) {
            final TaskExecutorPartitionInfo dataSetMetaInfo = partitionTrackerEntry.getMetaInfo();
            final DataSetEntry dataSetEntry =
                    clusterPartitions.computeIfAbsent(
                            dataSetMetaInfo.getIntermediateDataSetId(),
                            ignored -> new DataSetEntry(dataSetMetaInfo.getNumberOfPartitions()));
            dataSetEntry.addPartition(partitionTrackerEntry.getMetaInfo().getShuffleDescriptor());
        }
    }

    /**
     * 不再追踪某些集群数据
     * @param dataSetsToRelease data sets to release
     */
    @Override
    public void stopTrackingAndReleaseClusterPartitions(
            Collection<IntermediateDataSetID> dataSetsToRelease) {
        for (IntermediateDataSetID dataSetID : dataSetsToRelease) {
            final DataSetEntry dataSetEntry = clusterPartitions.remove(dataSetID);
            final Set<ResultPartitionID> partitionIds = dataSetEntry.getPartitionIds();
            // 释放资源
            shuffleEnvironment.releasePartitionsLocally(partitionIds);
        }
    }


    /**
     * 释放集群层面的所有数据
     */
    @Override
    public void stopTrackingAndReleaseAllClusterPartitions() {
        clusterPartitions.values().stream()
                .map(DataSetEntry::getPartitionIds)
                .forEach(shuffleEnvironment::releasePartitionsLocally);
        clusterPartitions.clear();
    }

    /**
     * 产生报告对象 就是一个bean
     * @return
     */
    @Override
    public ClusterPartitionReport createClusterPartitionReport() {
        List<ClusterPartitionReport.ClusterPartitionReportEntry> reportEntries =
                clusterPartitions.entrySet().stream()
                        .map(
                                entry ->
                                        new ClusterPartitionReport.ClusterPartitionReportEntry(
                                                entry.getKey(),
                                                entry.getValue().getTotalNumberOfPartitions(),
                                                entry.getValue().getShuffleDescriptors()))
                        .collect(Collectors.toList());

        return new ClusterPartitionReport(reportEntries);
    }

    /**
     * 对应一个中间数据集 数据集关联多个分区
     */
    private static class DataSetEntry {

        /**
         * 每个分区关联的洗牌对象
         */
        private final Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptors =
                new HashMap<>();

        /**
         * 表示有多少个分区     从元数据中获得的
         */
        private final int totalNumberOfPartitions;

        private DataSetEntry(int totalNumberOfPartitions) {
            this.totalNumberOfPartitions = totalNumberOfPartitions;
        }

        /**
         * 该分区相关的洗牌信息也要加入
         * @param shuffleDescriptor
         */
        void addPartition(ShuffleDescriptor shuffleDescriptor) {
            shuffleDescriptors.put(shuffleDescriptor.getResultPartitionID(), shuffleDescriptor);
        }

        public Set<ResultPartitionID> getPartitionIds() {
            return shuffleDescriptors.keySet();
        }

        public int getTotalNumberOfPartitions() {
            return totalNumberOfPartitions;
        }

        public Map<ResultPartitionID, ShuffleDescriptor> getShuffleDescriptors() {
            return shuffleDescriptors;
        }
    }
}
