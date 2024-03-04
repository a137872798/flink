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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.util.ResourceCounter;

import java.util.Collection;
import java.util.Map;

/** Tracks TaskManager's resource and slot status.
 * 该对象追踪每个 TaskManager的资源
 * */
interface TaskManagerTracker
        extends TaskManagerResourceInfoProvider,   // 提供TM信息
        ClusterResourceStatisticsProvider {  // 提供集群层面的信息

    // ---------------------------------------------------------------------------------------------
    // Add / Remove (pending) Resource
    // ---------------------------------------------------------------------------------------------

    /**
     * Register a new task manager.
     *
     * @param taskExecutorConnection of the new task manager
     * @param totalResourceProfile of the new task manager
     * @param defaultSlotResourceProfile of the new task manager
     *                                   追加一个TM信息
     */
    void addTaskManager(
            TaskExecutorConnection taskExecutorConnection,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile);

    /**
     * Unregister a task manager with the given instance id.
     *
     * @param instanceId of the task manager
     */
    void removeTaskManager(InstanceID instanceId);

    /**
     * Add a new pending task manager.
     *
     * @param pendingTaskManager to be added
     *                           追加一个申请中的TM
     */
    void addPendingTaskManager(PendingTaskManager pendingTaskManager);

    /**
     * Remove a pending task manager and it associated allocation records.
     *
     * @param pendingTaskManagerId of the pending task manager
     * @return the allocation records associated to the removed pending task manager
     * 移除的同时 返回该TM的已分配信息
     */
    Map<JobID, ResourceCounter> removePendingTaskManager(PendingTaskManagerId pendingTaskManagerId);

    /**
     * Add an unwanted task manager.
     *
     * @param instanceId identifier of task manager.
     *                   添加一个不需要的 TM
     */
    void addUnWantedTaskManager(InstanceID instanceId);

    /** Get unwanted task managers. */
    Map<InstanceID, WorkerResourceSpec> getUnWantedTaskManager();

    /**
     * Returns all task managers that have at least 1 allocation for the given job.
     *
     * @param jobId the job for which the task executors must have a slot
     * @return task managers with at least 1 slot for the job
     * 找到job id 可以找到分配给它的slot  然后找到相关的的TM
     */
    Collection<TaskManagerInfo> getTaskManagersWithAllocatedSlotsForJob(JobID jobId);

    // ---------------------------------------------------------------------------------------------
    // Slot status updates
    // ---------------------------------------------------------------------------------------------

    /**
     * Notifies the tracker about the slot status.
     *
     * @param allocationId of the slot
     * @param jobId of the slot
     * @param instanceId of the slot
     * @param resourceProfile of the slot
     * @param slotState of the slot
     *                  通知某个slot此时的状态
     */
    void notifySlotStatus(
            AllocationID allocationId,
            JobID jobId,
            InstanceID instanceId,
            ResourceProfile resourceProfile,
            SlotState slotState);

    /**
     * Clear all previous pending slot allocation records if any, and record new pending slot
     * allocations.
     *
     * @param pendingSlotAllocations new pending slot allocations be recorded
     *                               更新某些Pending slot 此时的状态
     */
    void replaceAllPendingAllocations(
            Map<PendingTaskManagerId, Map<JobID, ResourceCounter>> pendingSlotAllocations);

    /**
     * Clear all previous pending slot allocation records for the given job.
     *
     * @param jobId of the given job
     */
    void clearPendingAllocationsOfJob(JobID jobId);

    /** Removes all state from the tracker. */
    void clear();
}
