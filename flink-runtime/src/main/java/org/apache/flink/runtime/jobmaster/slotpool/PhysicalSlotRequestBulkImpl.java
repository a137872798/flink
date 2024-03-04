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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/** Represents a bulk of physical slot requests.
 * 维护一组请求
 * */
class PhysicalSlotRequestBulkImpl implements PhysicalSlotRequestBulk {

    /**
     * 描述待处理的请求 以及它们需要的资源
     */
    private final Map<SlotRequestId, ResourceProfile> pendingRequests;

    /**
     * 记录这些req被分配了什么slot
     */
    private final Map<SlotRequestId, AllocationID> fulfilledRequests = new HashMap<>();

    private final BiConsumer<SlotRequestId, Throwable> canceller;

    PhysicalSlotRequestBulkImpl(
            Map<SlotRequestId, ResourceProfile> physicalSlotRequests,
            BiConsumer<SlotRequestId, Throwable> canceller) {
        this.pendingRequests = new HashMap<>(physicalSlotRequests);
        this.canceller = canceller;
    }

    /**
     * 表示某个req被分配了slot
     * @param slotRequestId
     * @param allocationID
     */
    void markRequestFulfilled(final SlotRequestId slotRequestId, final AllocationID allocationID) {
        // 从待处理容器中移除
        pendingRequests.remove(slotRequestId);
        // 添加到处理好的容器中
        fulfilledRequests.put(slotRequestId, allocationID);
    }

    @Override
    public Collection<ResourceProfile> getPendingRequests() {
        return pendingRequests.values();
    }

    @Override
    public Set<AllocationID> getAllocationIdsOfFulfilledRequests() {
        return new HashSet<>(fulfilledRequests.values());
    }

    @Override
    public void cancel(Throwable cause) {
        // pending requests must be canceled first otherwise they might be fulfilled by
        // allocated slots released from this bulk

        // 使用异常处理所有请求
        for (SlotRequestId slotRequestId : pendingRequests.keySet()) {
            canceller.accept(slotRequestId, cause);
        }
        for (SlotRequestId slotRequestId : fulfilledRequests.keySet()) {
            canceller.accept(slotRequestId, cause);
        }
    }
}
