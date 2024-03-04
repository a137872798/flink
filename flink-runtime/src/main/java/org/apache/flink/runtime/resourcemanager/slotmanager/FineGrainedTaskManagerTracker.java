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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Implementation of {@link TaskManagerTracker} supporting fine-grained resource management.
 *
 * */
public class FineGrainedTaskManagerTracker implements TaskManagerTracker {
    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedTaskManagerTracker.class);

    /** Map for allocated and pending slots. */
    private final Map<AllocationID, FineGrainedTaskManagerSlot> slots;

    /** All currently registered task managers.
     * 记录了每个TM此时资源的分配情况
     * */
    private final Map<InstanceID, FineGrainedTaskManagerRegistration> taskManagerRegistrations;

    /** All unwanted task managers.
     * 表示不被需要的TM  Value对象就比较简单
     * */
    private final Map<InstanceID, WorkerResourceSpec> unWantedTaskManagers;

    /**
     * 维护待分配的资源
     */
    private final Map<PendingTaskManagerId, PendingTaskManager> pendingTaskManagers;

    private ResourceProfile totalRegisteredResource = ResourceProfile.ZERO;
    private ResourceProfile totalPendingResource = ResourceProfile.ZERO;

    /**
     * Pending task manager indexed by the tuple of total resource profile and default slot resource
     * profile.
     * 这个是用来匹配 PendingTaskManager 的
     */
    private final Map<Tuple2<ResourceProfile, ResourceProfile>, Set<PendingTaskManager>>
            totalAndDefaultSlotProfilesToPendingTaskManagers;

    public FineGrainedTaskManagerTracker() {
        slots = new HashMap<>();
        taskManagerRegistrations = new HashMap<>();
        unWantedTaskManagers = new HashMap<>();
        pendingTaskManagers = new HashMap<>();
        totalAndDefaultSlotProfilesToPendingTaskManagers = new HashMap<>();
    }

    /**
     * 更替所有 pending的信息
     * @param pendingSlotAllocations new pending slot allocations be recorded
     */
    @Override
    public void replaceAllPendingAllocations(
            Map<PendingTaskManagerId, Map<JobID, ResourceCounter>> pendingSlotAllocations) {
        Preconditions.checkNotNull(pendingSlotAllocations);
        LOG.trace("Record the pending allocations {}.", pendingSlotAllocations);
        pendingTaskManagers.values().forEach(PendingTaskManager::clearAllPendingAllocations);
        pendingSlotAllocations.forEach(
                (pendingTaskManagerId, jobIDResourceCounterMap) ->
                        // 找到对应TM
                        Preconditions.checkNotNull(pendingTaskManagers.get(pendingTaskManagerId))
                                // 重新计算 PendingTaskManager 中的未分配资源和分配中资源
                                .replaceAllPendingAllocations(jobIDResourceCounterMap));
    }

    /**
     * 清理有关这个job的资源分配
     * @param jobId of the given job
     */
    @Override
    public void clearPendingAllocationsOfJob(JobID jobId) {
        LOG.info("Clear all pending allocations for job {}.", jobId);
        pendingTaskManagers
                .values()
                .forEach(
                        pendingTaskManager ->
                                pendingTaskManager.clearPendingAllocationsOfJob(jobId));
    }

    /**
     * 追加一个TM
     * @param taskExecutorConnection of the new task manager   要添加的TM
     * @param totalResourceProfile of the new task manager
     * @param defaultSlotResourceProfile of the new task manager
     */
    @Override
    public void addTaskManager(
            TaskExecutorConnection taskExecutorConnection,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        Preconditions.checkNotNull(taskExecutorConnection);
        Preconditions.checkNotNull(totalResourceProfile);
        Preconditions.checkNotNull(defaultSlotResourceProfile);
        LOG.debug(
                "Add task manager {} with total resource {} and default slot resource {}.",
                taskExecutorConnection.getInstanceID(),
                totalResourceProfile,
                defaultSlotResourceProfile);

        // 根据描述信息产生 注册对象
        final FineGrainedTaskManagerRegistration taskManagerRegistration =
                new FineGrainedTaskManagerRegistration(
                        taskExecutorConnection, totalResourceProfile, defaultSlotResourceProfile);
        taskManagerRegistrations.put(
                taskExecutorConnection.getInstanceID(), taskManagerRegistration);

        // 记录总资源量
        totalRegisteredResource = totalRegisteredResource.merge(totalResourceProfile);
    }

    /**
     * 不再维护某个TM
     * @param instanceId of the task manager
     */
    @Override
    public void removeTaskManager(InstanceID instanceId) {
        Preconditions.checkNotNull(instanceId);
        unWantedTaskManagers.remove(instanceId);
        final FineGrainedTaskManagerRegistration taskManager =
                Preconditions.checkNotNull(taskManagerRegistrations.remove(instanceId));

        // 从总资源中去除
        totalRegisteredResource = totalRegisteredResource.subtract(taskManager.getTotalResource());
        LOG.debug("Remove task manager {}.", instanceId);
        // 不再维护该TM相关的slot
        for (AllocationID allocationId : taskManager.getAllocatedSlots().keySet()) {
            slots.remove(allocationId);
        }
    }

    /**
     * 将某个TM变成无用的状态   (加入一个额外的容器)
     * @param instanceId identifier of task manager.
     */
    @Override
    public void addUnWantedTaskManager(InstanceID instanceId) {
        final FineGrainedTaskManagerRegistration taskManager =
                taskManagerRegistrations.get(instanceId);
        if (taskManager != null) {
            unWantedTaskManagers.put(
                    instanceId,
                    WorkerResourceSpec.fromTotalResourceProfile(
                            taskManager.getTotalResource(),
                            SlotManagerUtils.calculateDefaultNumSlots(
                                    taskManager.getTotalResource(),
                                    taskManager.getDefaultSlotResourceProfile())));
        } else {
            LOG.debug("Unwanted task manager {} does not exists.", instanceId);
        }
    }

    @Override
    public Map<InstanceID, WorkerResourceSpec> getUnWantedTaskManager() {
        return unWantedTaskManagers;
    }

    /**
     * 添加一个申请中的TM
     * @param pendingTaskManager to be added
     */
    @Override
    public void addPendingTaskManager(PendingTaskManager pendingTaskManager) {
        Preconditions.checkNotNull(pendingTaskManager);
        LOG.debug("Add pending task manager {}.", pendingTaskManager);
        pendingTaskManagers.put(pendingTaskManager.getPendingTaskManagerId(), pendingTaskManager);
        totalPendingResource =
                totalPendingResource.merge(pendingTaskManager.getTotalResourceProfile());

        // 便于特定api检索
        totalAndDefaultSlotProfilesToPendingTaskManagers
                .computeIfAbsent(
                        Tuple2.of(
                                pendingTaskManager.getTotalResourceProfile(),
                                pendingTaskManager.getDefaultSlotResourceProfile()),
                        ignored -> new HashSet<>())
                .add(pendingTaskManager);
    }

    /**
     * 移除待处理的 TM
     * @param pendingTaskManagerId of the pending task manager
     * @return
     */
    @Override
    public Map<JobID, ResourceCounter> removePendingTaskManager(
            PendingTaskManagerId pendingTaskManagerId) {
        Preconditions.checkNotNull(pendingTaskManagerId);
        final PendingTaskManager pendingTaskManager =
                Preconditions.checkNotNull(pendingTaskManagers.remove(pendingTaskManagerId));
        totalPendingResource =
                totalPendingResource.subtract(pendingTaskManager.getTotalResourceProfile());
        LOG.debug("Remove pending task manager {}.", pendingTaskManagerId);
        totalAndDefaultSlotProfilesToPendingTaskManagers.compute(
                Tuple2.of(
                        pendingTaskManager.getTotalResourceProfile(),
                        pendingTaskManager.getDefaultSlotResourceProfile()),
                (ignored, pendingTMSet) -> {
                    Preconditions.checkNotNull(pendingTMSet).remove(pendingTaskManager);
                    return pendingTMSet.isEmpty() ? null : pendingTMSet;
                });
        return pendingTaskManager.getPendingSlotAllocationRecords();
    }

    @Override
    public Collection<TaskManagerInfo> getTaskManagersWithAllocatedSlotsForJob(JobID jobId) {
        return taskManagerRegistrations.values().stream()
                .filter(
                        taskManager ->
                                taskManager.getAllocatedSlots().values().stream()
                                        .anyMatch(slot -> jobId.equals(slot.getJobId())))
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------------------------------------
    // Core state transitions
    // ---------------------------------------------------------------------------------------------


    /**
     * 更新slot的状态
     * @param allocationId of the slot
     * @param jobId of the slot
     * @param instanceId of the slot
     * @param resourceProfile of the slot
     * @param slotState of the slot
     */
    @Override
    public void notifySlotStatus(
            AllocationID allocationId,
            JobID jobId,
            InstanceID instanceId,
            ResourceProfile resourceProfile,
            SlotState slotState) {
        Preconditions.checkNotNull(allocationId);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(instanceId);
        Preconditions.checkNotNull(resourceProfile);
        Preconditions.checkNotNull(slotState);
        switch (slotState) {
            case FREE:
                freeSlot(instanceId, allocationId);
                break;
            case ALLOCATED:
                addAllocatedSlot(allocationId, jobId, instanceId, resourceProfile);
                break;
            case PENDING:
                addPendingSlot(allocationId, jobId, instanceId, resourceProfile);
                break;
        }
    }

    /**
     * 通知TM 释放slot
     * @param instanceId
     * @param allocationId
     */
    private void freeSlot(InstanceID instanceId, AllocationID allocationId) {
        final FineGrainedTaskManagerRegistration taskManager =
                Preconditions.checkNotNull(taskManagerRegistrations.get(instanceId));
        Preconditions.checkNotNull(slots.remove(allocationId));
        LOG.debug("Free allocated slot with allocationId {}.", allocationId);
        taskManager.freeSlot(allocationId);
    }

    /**
     * 添加一个分配好的slot
     * @param allocationId
     * @param jobId
     * @param instanceId
     * @param resourceProfile
     */
    private void addAllocatedSlot(
            AllocationID allocationId,
            JobID jobId,
            InstanceID instanceId,
            ResourceProfile resourceProfile) {
        final FineGrainedTaskManagerRegistration taskManager =
                Preconditions.checkNotNull(taskManagerRegistrations.get(instanceId));
        if (slots.containsKey(allocationId)) {
            // Complete allocation of pending slot
            LOG.debug("Complete slot allocation with allocationId {}.", allocationId);
            taskManager.notifyAllocationComplete(allocationId);
        } else {
            // New allocated slot
            LOG.debug("Register new allocated slot with allocationId {}.", allocationId);
            final FineGrainedTaskManagerSlot slot =
                    new FineGrainedTaskManagerSlot(
                            allocationId,
                            jobId,
                            resourceProfile,
                            taskManager.getTaskExecutorConnection(),
                            SlotState.ALLOCATED);
            slots.put(allocationId, slot);
            // 表示没有经过pending阶段  直接添加
            taskManager.notifyAllocation(allocationId, slot);
        }
    }

    /**
     * 添加分配中的slot
     * @param allocationId
     * @param jobId
     * @param instanceId
     * @param resourceProfile
     */
    private void addPendingSlot(
            AllocationID allocationId,
            JobID jobId,
            InstanceID instanceId,
            ResourceProfile resourceProfile) {
        Preconditions.checkState(!slots.containsKey(allocationId));
        final FineGrainedTaskManagerRegistration taskManager =
                Preconditions.checkNotNull(taskManagerRegistrations.get(instanceId));
        LOG.debug("Add pending slot with allocationId {}.", allocationId);

        // 创建slot信息
        final FineGrainedTaskManagerSlot slot =
                new FineGrainedTaskManagerSlot(
                        allocationId,
                        jobId,
                        resourceProfile,
                        taskManager.getTaskExecutorConnection(),
                        SlotState.PENDING);
        taskManager.notifyAllocation(allocationId, slot);
        slots.put(allocationId, slot);
    }

    // ---------------------------------------------------------------------------------------------
    // Getters of internal state
    // ---------------------------------------------------------------------------------------------

    @Override
    public Collection<? extends TaskManagerInfo> getRegisteredTaskManagers() {
        return Collections.unmodifiableCollection(taskManagerRegistrations.values());
    }

    @Override
    public Optional<TaskManagerInfo> getRegisteredTaskManager(InstanceID instanceId) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceId));
    }

    @Override
    public Optional<TaskManagerSlotInformation> getAllocatedOrPendingSlot(
            AllocationID allocationId) {
        return Optional.ofNullable(slots.get(allocationId));
    }

    @Override
    public Collection<PendingTaskManager> getPendingTaskManagers() {
        return Collections.unmodifiableCollection(pendingTaskManagers.values());
    }

    @Override
    public Collection<PendingTaskManager>
            getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
                    ResourceProfile totalResourceProfile,
                    ResourceProfile defaultSlotResourceProfile) {
        return Collections.unmodifiableCollection(
                totalAndDefaultSlotProfilesToPendingTaskManagers.getOrDefault(
                        Tuple2.of(totalResourceProfile, defaultSlotResourceProfile),
                        Collections.emptySet()));
    }

    /**
     * 根据TM设置的slot数量 来计算整个集群能够感知到的slot数量
     * @return
     */
    @Override
    public int getNumberRegisteredSlots() {
        return taskManagerRegistrations.values().stream()
                .mapToInt(TaskManagerInfo::getDefaultNumSlots)
                .sum();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceId))
                .map(TaskManagerInfo::getDefaultNumSlots)
                .orElse(0);
    }

    @Override
    public int getNumberFreeSlots() {
        return taskManagerRegistrations.keySet().stream()
                .mapToInt(this::getNumberFreeSlotsOf)
                .sum();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceId))
                .map(
                        taskManager ->
                                Math.max(
                                        taskManager.getDefaultNumSlots()
                                                - taskManager.getAllocatedSlots().size(),
                                        0))
                .orElse(0);
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return totalRegisteredResource;
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceId))
                .map(TaskManagerInfo::getTotalResource)
                .orElse(ResourceProfile.ZERO);
    }

    @Override
    public ResourceProfile getFreeResource() {
        return taskManagerRegistrations.values().stream()
                .map(TaskManagerInfo::getAvailableResource)
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceId) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceId))
                .map(TaskManagerInfo::getAvailableResource)
                .orElse(ResourceProfile.ZERO);
    }

    @Override
    public ResourceProfile getPendingResource() {
        return totalPendingResource;
    }

    @Override
    public void clear() {
        slots.clear();
        taskManagerRegistrations.clear();
        totalRegisteredResource = ResourceProfile.ZERO;
        pendingTaskManagers.clear();
        totalPendingResource = ResourceProfile.ZERO;
        unWantedTaskManagers.clear();
    }
}
