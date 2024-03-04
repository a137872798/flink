/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Default {@link AllocatedSlotPool} implementation. */
public class DefaultAllocatedSlotPool implements AllocatedSlotPool {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultAllocatedSlotPool.class);

    /**
     * 借用slot不需要从该容器移除
     */
    private final Map<AllocationID, AllocatedSlot> registeredSlots;

    /** All free slots and since when they are free, index by TaskExecutor.
     * 该对象内部会维护未被占用的slot  以及他们的free时间
     * */
    private final FreeSlots freeSlots;

    /** Index containing a mapping between TaskExecutors and their slots.
     * 每个ResourceID 对应的对象可以占用一组slot
     * */
    private final Map<ResourceID, Set<AllocationID>> slotsPerTaskExecutor;

    public DefaultAllocatedSlotPool() {
        this.registeredSlots = new HashMap<>();
        this.slotsPerTaskExecutor = new HashMap<>();
        this.freeSlots = new FreeSlots();
    }

    @Override
    public void addSlots(Collection<AllocatedSlot> slots, long currentTime) {
        for (AllocatedSlot slot : slots) {
            addSlot(slot, currentTime);
        }
    }

    private void addSlot(AllocatedSlot slot, long currentTime) {
        // 避免重复添加
        Preconditions.checkState(
                !registeredSlots.containsKey(slot.getAllocationId()),
                "The slot pool already contains a slot with id %s",
                slot.getAllocationId());
        addSlotInternal(slot, currentTime);

        slotsPerTaskExecutor
                .computeIfAbsent(slot.getTaskManagerId(), resourceID -> new HashSet<>())
                .add(slot.getAllocationId());
    }

    private void addSlotInternal(AllocatedSlot slot, long currentTime) {
        registeredSlots.put(slot.getAllocationId(), slot);
        freeSlots.addFreeSlot(slot.getAllocationId(), slot.getTaskManagerId(), currentTime);
    }

    /**
     * 根据id从池中移除slot    移除不是借用
     * @param allocationId allocationId identifying the slot to remove from the slot pool
     * @return
     */
    @Override
    public Optional<AllocatedSlot> removeSlot(AllocationID allocationId) {
        final AllocatedSlot removedSlot = removeSlotInternal(allocationId);

        if (removedSlot != null) {
            final ResourceID owner = removedSlot.getTaskManagerId();

            // 更新 taskManager关联的slot数量
            slotsPerTaskExecutor.computeIfPresent(
                    owner,
                    (resourceID, allocationIds) -> {
                        allocationIds.remove(allocationId);

                        if (allocationIds.isEmpty()) {
                            return null;
                        }

                        return allocationIds;
                    });

            return Optional.of(removedSlot);
        } else {
            return Optional.empty();
        }
    }

    @Nullable
    private AllocatedSlot removeSlotInternal(AllocationID allocationId) {
        final AllocatedSlot removedSlot = registeredSlots.remove(allocationId);
        if (removedSlot != null) {
            freeSlots.removeFreeSlot(allocationId, removedSlot.getTaskManagerId());
        }
        return removedSlot;
    }

    @Override
    public AllocatedSlotsAndReservationStatus removeSlots(ResourceID owner) {
        final Set<AllocationID> slotsOfTaskExecutor = slotsPerTaskExecutor.remove(owner);

        if (slotsOfTaskExecutor != null) {
            final Collection<AllocatedSlot> removedSlots = new ArrayList<>();
            final Map<AllocationID, ReservationStatus> removedSlotsReservationStatus =
                    new HashMap<>();

            for (AllocationID allocationId : slotsOfTaskExecutor) {
                final ReservationStatus reservationStatus =
                        // 判断slot此时是否空闲  如果slot被借用 该方法会返回false
                        containsFreeSlot(allocationId)
                                ? ReservationStatus.FREE
                                : ReservationStatus.RESERVED;

                final AllocatedSlot removedSlot =
                        Preconditions.checkNotNull(removeSlotInternal(allocationId));
                removedSlots.add(removedSlot);
                removedSlotsReservationStatus.put(removedSlot.getAllocationId(), reservationStatus);
            }

            return new DefaultAllocatedSlotsAndReservationStatus(
                    removedSlots, removedSlotsReservationStatus);
        } else {
            return new DefaultAllocatedSlotsAndReservationStatus(
                    Collections.emptyList(), Collections.emptyMap());
        }
    }

    @Override
    public boolean containsSlots(ResourceID owner) {
        return slotsPerTaskExecutor.containsKey(owner);
    }

    @Override
    public boolean containsSlot(AllocationID allocationId) {
        return registeredSlots.containsKey(allocationId);
    }

    @Override
    public boolean containsFreeSlot(AllocationID allocationId) {
        return freeSlots.contains(allocationId);
    }

    @Override
    public AllocatedSlot reserveFreeSlot(AllocationID allocationId) {
        LOG.debug("Reserve free slot with allocation id {}.", allocationId);
        AllocatedSlot slot = registeredSlots.get(allocationId);
        Preconditions.checkNotNull(slot, "The slot with id %s was not exists.", allocationId);
        Preconditions.checkState(
                // 当借用时 会从freeSlots移除该slot
                freeSlots.removeFreeSlot(allocationId, slot.getTaskManagerId()) != null,
                "The slot with id %s was not free.",
                allocationId);
        return registeredSlots.get(allocationId);
    }

    /**
     * 归还借用的slot
     * @param allocationId identifying the reserved slot to freed
     * @param currentTime currentTime when the slot has been freed
     * @return
     */
    @Override
    public Optional<AllocatedSlot> freeReservedSlot(AllocationID allocationId, long currentTime) {
        // 需要先确保该slot 之前已经注册到pool中了   通过 addSlots
        final AllocatedSlot allocatedSlot = registeredSlots.get(allocationId);

        if (allocatedSlot != null && !freeSlots.contains(allocationId)) {
            freeSlots.addFreeSlot(allocationId, allocatedSlot.getTaskManagerId(), currentTime);
            return Optional.of(allocatedSlot);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<SlotInfo> getSlotInformation(AllocationID allocationID) {
        return Optional.ofNullable(registeredSlots.get(allocationID));
    }

    @Override
    public FreeSlotInfoTracker getFreeSlotInfoTracker() {
        return new DefaultFreeSlotInfoTracker(
                freeSlots.getFreeSlotsSince().keySet(),
                registeredSlots::get,
                this::getFreeSlotInfo,
                this::getTaskExecutorUtilization);
    }

    @Override
    public Collection<? extends SlotInfo> getAllSlotsInformation() {
        return registeredSlots.values();
    }

    /**
     * 计算slot的占用率
     * @param resourceId
     * @return
     */
    private double getTaskExecutorUtilization(ResourceID resourceId) {
        Set<AllocationID> slots = slotsPerTaskExecutor.get(resourceId);
        Preconditions.checkNotNull(slots, "There is no slots on %s", resourceId);

        return (double) (slots.size() - freeSlots.getFreeSlotsNumberOfTaskExecutor(resourceId))
                / slots.size();
    }

    private FreeSlotInfo getFreeSlotInfo(AllocationID allocationId) {
        final AllocatedSlot allocatedSlot =
                Preconditions.checkNotNull(registeredSlots.get(allocationId));
        final Long idleSince =
                Preconditions.checkNotNull(freeSlots.getFreeSlotsSince().get(allocationId));
        return DefaultFreeSlotInfo.create(allocatedSlot, idleSince);
    }

    /**
     * 该对象会记录所有slot被释放的时间
     */
    private static final class FreeSlots {
        /** Map containing all free slots and since when they are free. */
        private final Map<AllocationID, Long> freeSlotsSince = new HashMap<>();

        /** Index containing a mapping between TaskExecutors and their free slots number.
         * 记录同一个 ResourceID 关联的slot数量
         * */
        private final Map<ResourceID, Integer> freeSlotsNumberPerTaskExecutor = new HashMap<>();

        /**
         * 添加一个未被占用的slot
         * @param allocationId
         * @param resourceId
         * @param currentTime
         */
        public void addFreeSlot(
                AllocationID allocationId, ResourceID resourceId, long currentTime) {
            if (freeSlotsSince.put(allocationId, currentTime) == null) {
                // 表示首次添加
                freeSlotsNumberPerTaskExecutor.merge(resourceId, 1, Integer::sum);
            }
        }

        /**
         * 基本是 add的逆操作
         * @param allocationId
         * @param resourceId
         * @return
         */
        public Long removeFreeSlot(AllocationID allocationId, ResourceID resourceId) {
            Long freeSince = freeSlotsSince.remove(allocationId);
            if (freeSince != null) {
                freeSlotsNumberPerTaskExecutor.computeIfPresent(
                        resourceId,
                        (ignore, count) -> {
                            int newCount = count - 1;
                            return newCount == 0 ? null : newCount;
                        });
            }

            return freeSince;
        }

        public boolean contains(AllocationID allocationId) {
            return freeSlotsSince.containsKey(allocationId);
        }

        /**
         * 返回该资源关联的空闲slot数量
         * @param resourceId
         * @return
         */
        public int getFreeSlotsNumberOfTaskExecutor(ResourceID resourceId) {
            return freeSlotsNumberPerTaskExecutor.getOrDefault(resourceId, 0);
        }

        public Map<AllocationID, Long> getFreeSlotsSince() {
            return freeSlotsSince;
        }
    }

    /**
     * 表示一个空闲的slot
     */
    private static final class DefaultFreeSlotInfo implements AllocatedSlotPool.FreeSlotInfo {

        private final SlotInfo slotInfo;

        /**
         * 从该时刻开始空闲
         */
        private final long freeSince;

        private DefaultFreeSlotInfo(SlotInfo slotInfo, long freeSince) {
            this.slotInfo = slotInfo;
            this.freeSince = freeSince;
        }

        @Override
        public SlotInfo asSlotInfo() {
            return slotInfo;
        }

        @Override
        public long getFreeSince() {
            return freeSince;
        }

        private static DefaultFreeSlotInfo create(SlotInfo slotInfo, long idleSince) {
            return new DefaultFreeSlotInfo(Preconditions.checkNotNull(slotInfo), idleSince);
        }
    }

    /**
     * 当通过资源id移除slot时  被移除的slot会组成该对象
     */
    private static final class DefaultAllocatedSlotsAndReservationStatus
            implements AllocatedSlotsAndReservationStatus {

        /**
         * 原本被某个resourceId所占用
         */
        private final Collection<AllocatedSlot> slots;
        /**
         * 每个slot此时的状态
         */
        private final Map<AllocationID, ReservationStatus> reservationStatus;

        private DefaultAllocatedSlotsAndReservationStatus(
                Collection<AllocatedSlot> slots,
                Map<AllocationID, ReservationStatus> reservationStatus) {
            this.slots = slots;
            this.reservationStatus = reservationStatus;
        }

        @Override
        public boolean wasFree(AllocationID allocatedSlot) {
            return reservationStatus.get(allocatedSlot) == ReservationStatus.FREE;
        }

        @Override
        public Collection<AllocatedSlot> getAllocatedSlots() {
            return slots;
        }
    }

    private enum ReservationStatus {
        FREE,
        RESERVED  // 表示被占用
    }
}
