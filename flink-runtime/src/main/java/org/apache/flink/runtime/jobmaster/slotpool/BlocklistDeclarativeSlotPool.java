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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DeclarativeSlotPool} implementation that supports blocklist. This implementation will
 * avoid allocating slots that located on blocked nodes. The core idea is to keep the slot pool in
 * such a state: there is no slot in slot pool that is free (no task assigned) and located on
 * blocked nodes.
 */
public class BlocklistDeclarativeSlotPool extends DefaultDeclarativeSlotPool {

    /**
     * 该对象可以判断某个节点是否被阻塞
     */
    private final BlockedTaskManagerChecker blockedTaskManagerChecker;

    BlocklistDeclarativeSlotPool(
            JobID jobId,
            AllocatedSlotPool slotPool,
            Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements,
            BlockedTaskManagerChecker blockedTaskManagerChecker,
            Time idleSlotTimeout,
            Time rpcTimeout) {
        super(jobId, slotPool, notifyNewResourceRequirements, idleSlotTimeout, rpcTimeout);
        this.blockedTaskManagerChecker = checkNotNull(blockedTaskManagerChecker);
    }

    @Override
    public Collection<SlotOffer> offerSlots(
            Collection<? extends SlotOffer> offers,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime) {
        if (!isBlockedTaskManager(taskManagerLocation.getResourceID())) {
            // 未被阻塞就走正常逻辑
            return super.offerSlots(offers, taskManagerLocation, taskManagerGateway, currentTime);
        } else {
            return internalOfferSlotsFromBlockedTaskManager(offers, taskManagerLocation);
        }
    }

    @Override
    public Collection<SlotOffer> registerSlots(
            Collection<? extends SlotOffer> slots,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime) {
        if (!isBlockedTaskManager(taskManagerLocation.getResourceID())) {
            return super.registerSlots(slots, taskManagerLocation, taskManagerGateway, currentTime);
        } else {
            return internalOfferSlotsFromBlockedTaskManager(slots, taskManagerLocation);
        }
    }

    /**
     * 表示尝试使用的资源处于被阻塞状态
     * @param offers
     * @param taskManagerLocation
     * @return
     */
    private Collection<SlotOffer> internalOfferSlotsFromBlockedTaskManager(
            Collection<? extends SlotOffer> offers, TaskManagerLocation taskManagerLocation) {
        final Collection<SlotOffer> acceptedSlotOffers = new ArrayList<>();
        final Collection<SlotOffer> rejectedSlotOffers = new ArrayList<>();

        // we should accept a duplicate (already accepted) slot, even if it's from a currently
        // blocked task manager. Because the slot may already be assigned to an execution, rejecting
        // it will cause a task failover.
        // 只返回已经存在的slot    认为此时无法往pool中添加该资源相关的slot
        for (SlotOffer offer : offers) {
            if (slotPool.containsSlot(offer.getAllocationId())) {
                // we have already accepted this offer
                acceptedSlotOffers.add(offer);
            } else {
                rejectedSlotOffers.add(offer);
            }
        }

        log.debug(
                "Received {} slots from a blocked TaskManager {}, {} was accepted before: {}, {} was rejected: {}.",
                offers.size(),
                taskManagerLocation,
                acceptedSlotOffers.size(),
                acceptedSlotOffers,
                rejectedSlotOffers.size(),
                rejectedSlotOffers);

        return acceptedSlotOffers;
    }

    @Override
    public ResourceCounter freeReservedSlot(
            AllocationID allocationId, @Nullable Throwable cause, long currentTime) {
        Optional<SlotInfo> slotInfo = slotPool.getSlotInformation(allocationId);

        if (!slotInfo.isPresent()) {
            return ResourceCounter.empty();
        }

        ResourceID taskManagerId = slotInfo.get().getTaskManagerLocation().getResourceID();
        if (!isBlockedTaskManager(taskManagerId)) {
            return super.freeReservedSlot(allocationId, cause, currentTime);
        } else {
            log.debug("Free reserved slot {}.", allocationId);
            // 当被阻塞时 slot无法正常归还到pool中 而是直接释放
            return releaseSlot(
                    allocationId,
                    new FlinkRuntimeException(
                            String.format(
                                    "Free reserved slot %s on blocked task manager %s.",
                                    allocationId, taskManagerId.getStringWithMetadata())));
        }
    }

    private boolean isBlockedTaskManager(ResourceID resourceID) {
        return blockedTaskManagerChecker.isBlockedTaskManager(resourceID);
    }
}
