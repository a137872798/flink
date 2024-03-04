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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobmaster.SlotInfo;

import java.util.Collection;
import java.util.Set;

/** Track all free slots, support bookkeeping slot for {@link SlotSelectionStrategy}.
 * 通过该对象跟踪所有的slot
 * */
public interface FreeSlotInfoTracker {

    /**
     * Get allocation id of all available slots.
     *
     * @return allocation id of available slots
     * 获取所有可用的槽
     * 每个AllocationID 标识一个槽
     */
    Set<AllocationID> getAvailableSlots();

    /**
     * Get slot info by allocation id, this slot must exist.
     *
     * @param allocationId to get SlotInfo
     * @return slot info for the allocation id
     * 通过id查询某个槽的信息
     */
    SlotInfo getSlotInfo(AllocationID allocationId);

    /**
     * Returns a list of {@link AllocatedSlotPool.FreeSlotInfo} objects about all slots with slot
     * idle since that are currently available in the slot pool.
     *
     * @return a list of {@link AllocatedSlotPool.FreeSlotInfo} objects about all slots with slot
     *     idle since that are currently available in the slot pool.
     *     获取所有空闲的slot
     */
    Collection<AllocatedSlotPool.FreeSlotInfo> getFreeSlotsWithIdleSinceInformation();

    /**
     * Returns a list of {@link SlotInfo} objects about all slots that are currently available in
     * the slot pool.
     *
     * @return a list of {@link SlotInfo} objects about all slots that are currently available in
     *     the slot pool.
     *     获取所有空闲的slot
     */
    Collection<SlotInfo> getFreeSlotsInformation();

    /**
     * Get task executor utilization of this slot.
     *
     * @param slotInfo to get task executor utilization
     * @return task executor utilization of this slot
     */
    double getTaskExecutorUtilization(SlotInfo slotInfo);

    /**
     * Reserve free slot when it is used.
     *
     * @param allocationId to reserve
     *                     表示要占用这个slot
     */
    void reserveSlot(AllocationID allocationId);

    /**
     * Create a new free slot tracker without blocked slots.
     *
     * @param blockedSlots slots that should not be used
     * @return the new free slot tracker
     * 创建一个新的追踪对象 且不包含这些slot
     */
    FreeSlotInfoTracker createNewFreeSlotInfoTrackerWithoutBlockedSlots(
            Set<AllocationID> blockedSlots);
}
