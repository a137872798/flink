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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotStatus;

import javax.annotation.Nullable;

import java.util.Collection;

/** Tracks slots and their {@link SlotState}.
 *
 * */
interface SlotTracker {

    /**
     * Registers the given listener with this tracker.
     *
     * @param slotStatusUpdateListener listener to register
     *                                 注册监听器
     */
    void registerSlotStatusUpdateListener(SlotStatusUpdateListener slotStatusUpdateListener);

    /**
     * Adds the given slot to this tracker. The given slot may already be allocated for a job. This
     * method must be called before the tracker is notified of any state transition or slot status
     * notification.
     *
     * @param slotId ID of the slot
     * @param resourceProfile resource of the slot
     * @param taskManagerConnection connection to the hosting task executor
     * @param initialJob job that the slot is allocated for, or null if it is free
     *                   追加一个维护信息 表示某TaskExecutor拥有这个slot
     */
    void addSlot(
            SlotID slotId,
            ResourceProfile resourceProfile,
            TaskExecutorConnection taskManagerConnection,
            @Nullable JobID initialJob);

    /**
     * Removes the given set of slots from the slot manager. If a removed slot was not free at the
     * time of removal, then this method will automatically transition the slot to a free state.
     *
     * @param slotsToRemove identifying the slots to remove from the slot manager
     *                      不再维护这组slot
     */
    void removeSlots(Iterable<SlotID> slotsToRemove);

    /**
     * Notifies the tracker that the allocation for the given slot, for the given job, has started.
     *
     * @param slotId slot being allocated
     * @param jobId job for which the slot is being allocated
     *              通知开始将 slot 分配给job了
     */
    void notifyAllocationStart(SlotID slotId, JobID jobId);

    /**
     * Notifies the tracker that the allocation for the given slot, for the given job, has completed
     * successfully.
     *
     * @param slotId slot being allocated
     * @param jobId job for which the slot is being allocated
     *              通知分配结束
     */
    void notifyAllocationComplete(SlotID slotId, JobID jobId);

    /**
     * Notifies the tracker that the given slot was freed.
     *
     * @param slotId slot being freed
     *               通知释放slot
     */
    void notifyFree(SlotID slotId);

    /**
     * Notifies the tracker about the slot statuses.
     *
     * @param slotStatuses slot statues
     * @return whether any slot status has changed
     * 通知此时一组slot的状态
     */
    boolean notifySlotStatus(Iterable<SlotStatus> slotStatuses);

    /**
     * Returns a view over free slots. The returned collection cannot be modified directly, but
     * reflects changes to the set of free slots.
     *
     * @return free slots
     * 获取空闲的slot
     */
    Collection<TaskManagerSlotInformation> getFreeSlots();

    /**
     * Returns all task executors that have at least 1 pending/completed allocation for the given
     * job.
     *
     * @param jobId the job for which the task executors must have a slot
     * @return task executors with at least 1 slot for the job
     * 先通过job 找到分配给job的所有slot  然后再转变成查询slot相关的所有TaskExecutor连接
     */
    Collection<TaskExecutorConnection> getTaskExecutorsWithAllocatedSlotsForJob(JobID jobId);
}
