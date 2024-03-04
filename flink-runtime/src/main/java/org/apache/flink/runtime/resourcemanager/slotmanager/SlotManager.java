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
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.rest.messages.taskmanager.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import java.util.Collection;
import java.util.concurrent.Executor;

/**
 * The slot manager is responsible for maintaining a view on all registered task manager slots,
 * their allocation and all pending slot requests. Whenever a new slot is registered or an allocated
 * slot is freed, then it tries to fulfill another pending slot request. Whenever there are not
 * enough slots available the slot manager will notify the resource manager about it via {@link
 * ResourceAllocator#declareResourceNeeded}.
 *
 * <p>In order to free resources and avoid resource leaks, idling task managers (task managers whose
 * slots are currently not used) and pending slot requests time out triggering their release and
 * failure, respectively.
 * SlotManager 用于管理slot
 */
public interface SlotManager extends AutoCloseable {

    /**
     * 获取已经注册的slot总数
     * @return
     */
    int getNumberRegisteredSlots();

    /**
     * 某实例注册slot数
     * @param instanceId
     * @return
     */
    int getNumberRegisteredSlotsOf(InstanceID instanceId);

    /**
     * 空闲slot数
     * @return
     */
    int getNumberFreeSlots();

    /**
     * 某实例slot空闲数
     * @param instanceId
     * @return
     */
    int getNumberFreeSlotsOf(InstanceID instanceId);

    ResourceProfile getRegisteredResource();

    ResourceProfile getRegisteredResourceOf(InstanceID instanceID);

    ResourceProfile getFreeResource();

    ResourceProfile getFreeResourceOf(InstanceID instanceID);

    /**
     * 返回该实例相关的所有已分配slot
     * @param instanceID
     * @return
     */
    Collection<SlotInfo> getAllocatedSlotsOf(InstanceID instanceID);

    /**
     * Starts the slot manager with the given leader id and resource manager actions.
     *
     * @param newResourceManagerId to use for communication with the task managers   资源管理器的id
     * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
     * @param newResourceAllocator to use for resource (de-)allocations
     * @param resourceEventListener to use for notify resource not enough
     * @param newBlockedTaskManagerChecker to query whether a task manager is blocked
     *                                     启动slot管理器
     */
    void start(
            ResourceManagerId newResourceManagerId,
            Executor newMainThreadExecutor,
            ResourceAllocator newResourceAllocator,
            ResourceEventListener resourceEventListener,
            BlockedTaskManagerChecker newBlockedTaskManagerChecker);

    /** Suspends the component. This clears the internal state of the slot manager.
     * 暂停组件 会清理内部状态
     * */
    void suspend();

    /**
     * Notifies the slot manager that the resource requirements for the given job should be cleared.
     * The slot manager may assume that no further updates to the resource requirements will occur.
     *
     * @param jobId job for which to clear the requirements
     *              清理某个job需要的资源
     */
    void clearResourceRequirements(JobID jobId);

    /**
     * Notifies the slot manager about the resource requirements of a job.
     *
     * @param resourceRequirements resource requirements of a job
     *                             处理某个job申请资源的请求
     */
    void processResourceRequirements(ResourceRequirements resourceRequirements);

    /**
     * Registers a new task manager at the slot manager. This will make the task managers slots
     * known and, thus, available for allocation.
     *
     * @param taskExecutorConnection for the new task manager
     * @param initialSlotReport for the new task manager
     * @param totalResourceProfile for the new task manager
     * @param defaultSlotResourceProfile for the new task manager
     * @return The result of task manager registration
     * 注册任务管理器
     */
    RegistrationResult registerTaskManager(
            TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile);

    /**
     * Unregisters the task manager identified by the given instance id and its associated slots
     * from the slot manager.
     *
     * @param instanceId identifying the task manager to unregister
     * @param cause for unregistering the TaskManager
     * @return True if there existed a registered task manager with the given instance id
     * 注销任务管理器
     */
    boolean unregisterTaskManager(InstanceID instanceId, Exception cause);

    /**
     * Reports the current slot allocations for a task manager identified by the given instance id.
     *
     * @param instanceId identifying the task manager for which to report the slot status
     * @param slotReport containing the status for all of its slots
     * @return true if the slot status has been updated successfully, otherwise false
     */
    boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport);

    /**
     * Free the given slot from the given allocation. If the slot is still allocated by the given
     * allocation id, then the slot will be marked as free and will be subject to new slot requests.
     *
     * @param slotId identifying the slot to free
     * @param allocationId with which the slot is presumably allocated
     *                     释放slot
     */
    void freeSlot(SlotID slotId, AllocationID allocationId);

    void setFailUnfulfillableRequest(boolean failUnfulfillableRequest);

    /**
     * Trigger the resource requirement check. This method will be called when some slot statuses
     * changed.
     * 触发资源需求检查
     */
    void triggerResourceRequirementsCheck();

    /** The result of task manager registration. */
    enum RegistrationResult {
        SUCCESS, // task manager has not been registered before and is registered successfully
        IGNORED, // task manager has been registered before and is ignored
        REJECTED, // task manager is rejected and should be disconnected
    }
}
