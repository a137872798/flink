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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Represents a pending task manager in the {@link SlotManager}.
 * 表示正要申请的 TaskManager (worker)
 * */
public class PendingTaskManager {
    private final PendingTaskManagerId pendingTaskManagerId;

    /**
     * 需要的总资源描述
     */
    private final ResourceProfile totalResourceProfile;
    /**
     * 默认每个slot的需求量
     */
    private final ResourceProfile defaultSlotResourceProfile;
    /**
     * 一个TaskManager应该可以提供多个slot
     */
    private final int numSlots;

    /**
     * 表示资源剩余的部分
     */
    private ResourceProfile unusedResource;

    /**
     * 提前分配的资源
     */
    Map<JobID, ResourceCounter> pendingSlotAllocationRecords;

    public PendingTaskManager(ResourceProfile totalResourceProfile, int numSlots) {
        this.numSlots = numSlots;
        this.totalResourceProfile = Preconditions.checkNotNull(totalResourceProfile);
        this.defaultSlotResourceProfile =
                SlotManagerUtils.generateDefaultSlotResourceProfile(totalResourceProfile, numSlots);
        this.pendingTaskManagerId = PendingTaskManagerId.generate();
        this.unusedResource = totalResourceProfile;
        this.pendingSlotAllocationRecords = new HashMap<>();
    }

    public ResourceProfile getTotalResourceProfile() {
        return totalResourceProfile;
    }

    public ResourceProfile getDefaultSlotResourceProfile() {
        return defaultSlotResourceProfile;
    }

    public PendingTaskManagerId getPendingTaskManagerId() {
        return pendingTaskManagerId;
    }

    public int getNumSlots() {
        return numSlots;
    }

    public ResourceProfile getUnusedResource() {
        return unusedResource;
    }

    public Map<JobID, ResourceCounter> getPendingSlotAllocationRecords() {
        return pendingSlotAllocationRecords;
    }

    public void clearAllPendingAllocations() {
        pendingSlotAllocationRecords.clear();
        unusedResource = totalResourceProfile;
    }

    /**
     * 更换当前的资源分配信息
     * @param pendingSlotAllocations
     */
    public void replaceAllPendingAllocations(Map<JobID, ResourceCounter> pendingSlotAllocations) {
        pendingSlotAllocationRecords.clear();
        pendingSlotAllocationRecords.putAll(pendingSlotAllocations);
        unusedResource = calculateUnusedResourceProfile();
    }

    /**
     * 清理分配数据的同时 归还到unusedResource
     * @param jobId
     */
    public void clearPendingAllocationsOfJob(JobID jobId) {
        Optional.ofNullable(pendingSlotAllocationRecords.remove(jobId))
                .ifPresent(
                        resourceCounter ->
                                unusedResource =
                                        unusedResource.merge(resourceCounter.getTotalResource()));
    }

    /**
     * 将总资源减去分配的部分 得到剩余的部分
     * @return
     */
    private ResourceProfile calculateUnusedResourceProfile() {
        return totalResourceProfile.subtract(
                pendingSlotAllocationRecords.values().stream()
                        .map(ResourceCounter::getTotalResource)
                        .reduce(ResourceProfile.ZERO, ResourceProfile::merge));
    }

    @Override
    public String toString() {
        return String.format(
                "PendingTaskManager{id=%s, totalResourceProfile=%s, defaultSlotResourceProfile=%s}",
                pendingTaskManagerId, totalResourceProfile, defaultSlotResourceProfile);
    }
}
