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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A FineGrainedTaskManagerRegistration represents a TaskManager. It maintains states of the
 * TaskManager needed in {@link FineGrainedSlotManager}.
 * 描述一个TM的信息  细粒度对象
 */
public class FineGrainedTaskManagerRegistration implements TaskManagerInfo {

    /**
     * 通过该对象访问 TaskExecutor
     */
    private final TaskExecutorConnection taskManagerConnection;

    /**
     * FineGrainedTaskManagerSlot 就是包装了slot的基本信息
     */
    private final Map<AllocationID, FineGrainedTaskManagerSlot> slots;

    // 记录了TM的总资源和默认每个slot的分配资源

    private final ResourceProfile defaultSlotResourceProfile;

    private final ResourceProfile totalResource;

    /**
     * 默认有多少slot
     */
    private final int defaultNumSlots;

    private ResourceProfile unusedResource;

    /**
     * 表示正在分配的资源
     */
    private ResourceProfile pendingResource = ResourceProfile.ZERO;

    /** Timestamp when the last time becoming idle. Otherwise Long.MAX_VALUE.
     * 当没有分配出任何slot时  就认为该对象是空闲的
     * */
    private long idleSince;

    public FineGrainedTaskManagerRegistration(
            TaskExecutorConnection taskManagerConnection,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        this.taskManagerConnection = Preconditions.checkNotNull(taskManagerConnection);
        this.totalResource = Preconditions.checkNotNull(totalResourceProfile);
        this.defaultSlotResourceProfile = Preconditions.checkNotNull(defaultSlotResourceProfile);

        this.slots = new HashMap<>();

        this.defaultNumSlots =
                SlotManagerUtils.calculateDefaultNumSlots(
                        totalResourceProfile, defaultSlotResourceProfile);

        this.unusedResource = ResourceProfile.newBuilder(totalResourceProfile).build();

        idleSince = System.currentTimeMillis();
    }

    @Override
    public TaskExecutorConnection getTaskExecutorConnection() {
        return taskManagerConnection;
    }

    @Override
    public InstanceID getInstanceId() {
        return taskManagerConnection.getInstanceID();
    }

    @Override
    public Map<AllocationID, TaskManagerSlotInformation> getAllocatedSlots() {
        return Collections.unmodifiableMap(slots);
    }

    @Override
    public ResourceProfile getAvailableResource() {
        if (!unusedResource.allFieldsNoLessThan(pendingResource)) {
            return ResourceProfile.ZERO;
        }
        return unusedResource.subtract(pendingResource);
    }

    @Override
    public ResourceProfile getDefaultSlotResourceProfile() {
        return defaultSlotResourceProfile;
    }

    @Override
    public ResourceProfile getTotalResource() {
        return totalResource;
    }

    @Override
    public int getDefaultNumSlots() {
        return defaultNumSlots;
    }

    @Override
    public long getIdleSince() {
        return idleSince;
    }

    @Override
    public boolean isIdle() {
        return idleSince != Long.MAX_VALUE;
    }

    /**
     * 释放某个slot
     * @param allocationId
     */
    public void freeSlot(AllocationID allocationId) {
        Preconditions.checkNotNull(allocationId);
        FineGrainedTaskManagerSlot taskManagerSlot =
                Preconditions.checkNotNull(slots.remove(allocationId));

        if (taskManagerSlot.getState() == SlotState.PENDING) {
            pendingResource = pendingResource.subtract(taskManagerSlot.getResourceProfile());
        } else {
            unusedResource = unusedResource.merge(taskManagerSlot.getResourceProfile());
        }

        if (slots.isEmpty()) {
            idleSince = System.currentTimeMillis();
        }
    }

    /**
     * 通知某个slot分配完成了
     * @param allocationId
     */
    public void notifyAllocationComplete(AllocationID allocationId) {
        Preconditions.checkNotNull(allocationId);
        FineGrainedTaskManagerSlot slot = Preconditions.checkNotNull(slots.get(allocationId));
        Preconditions.checkState(slot.getState() == SlotState.PENDING);
        slot.completeAllocation();
        pendingResource = pendingResource.subtract(slot.getResourceProfile());
        unusedResource = unusedResource.subtract(slot.getResourceProfile());
    }

    /**
     * 直接插入一个slot
     * @param allocationId
     * @param taskManagerSlot
     */
    public void notifyAllocation(
            AllocationID allocationId, FineGrainedTaskManagerSlot taskManagerSlot) {
        Preconditions.checkNotNull(allocationId);
        Preconditions.checkNotNull(taskManagerSlot);
        switch (taskManagerSlot.getState()) {
            case PENDING:
                ResourceProfile newPendingResource =
                        pendingResource.merge(taskManagerSlot.getResourceProfile());
                Preconditions.checkState(totalResource.allFieldsNoLessThan(newPendingResource));
                pendingResource = newPendingResource;
                break;
            case ALLOCATED:
                // 不经过 pendingResource
                unusedResource = unusedResource.subtract(taskManagerSlot.getResourceProfile());
                break;
            default:
                throw new IllegalStateException(
                        "The slot stat should not be FREE under fine-grained resource management.");
        }
        slots.put(allocationId, taskManagerSlot);
        idleSince = Long.MAX_VALUE;
    }
}
