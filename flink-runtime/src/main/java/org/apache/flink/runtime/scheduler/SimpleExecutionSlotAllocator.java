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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.DualKeyLinkedMap;
import org.apache.flink.util.FlinkException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple implementation of {@link ExecutionSlotAllocator}. No support for slot sharing,
 * co-location, nor local recovery.
 * 这是一个简单的分配对象
 */
public class SimpleExecutionSlotAllocator implements ExecutionSlotAllocator {

    /**
     * 可以向该对象申请/提供 slot
     */
    private final PhysicalSlotProvider slotProvider;

    /**
     * 表示slot是否会被永久占用
     */
    private final boolean slotWillBeOccupiedIndefinitely;

    /**
     * 查看Execution需要的资源
     */
    private final Function<ExecutionAttemptID, ResourceProfile> resourceProfileRetriever;

    /**
     * 该对象以阻塞形式 得到一个Execution最合适分配的TMLocation
     */
    private final SyncPreferredLocationsRetriever preferredLocationsRetriever;

    /**
     * 可以使用2种key 来检索value
     */
    private final DualKeyLinkedMap<
                    ExecutionAttemptID, SlotRequestId, CompletableFuture<LogicalSlot>>
            requestedPhysicalSlots;

    SimpleExecutionSlotAllocator(
            PhysicalSlotProvider slotProvider,
            Function<ExecutionAttemptID, ResourceProfile> resourceProfileRetriever,
            SyncPreferredLocationsRetriever preferredLocationsRetriever,
            boolean slotWillBeOccupiedIndefinitely) {
        this.slotProvider = checkNotNull(slotProvider);
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.resourceProfileRetriever = checkNotNull(resourceProfileRetriever);
        this.preferredLocationsRetriever = checkNotNull(preferredLocationsRetriever);
        this.requestedPhysicalSlots = new DualKeyLinkedMap<>();
    }

    /**
     * 为这组Execution 分配slot
     * @param executionAttemptIds executions to allocate slots for
     * @return
     */
    @Override
    public Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
            List<ExecutionAttemptID> executionAttemptIds) {
        Map<ExecutionAttemptID, ExecutionSlotAssignment> result = new HashMap<>();

        Map<SlotRequestId, ExecutionAttemptID> remainingExecutionsToSlotRequest =
                new HashMap<>(executionAttemptIds.size());
        List<PhysicalSlotRequest> physicalSlotRequests =
                new ArrayList<>(executionAttemptIds.size());

        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
            if (requestedPhysicalSlots.containsKeyA(executionAttemptId)) {
                // 该Execution之前分配过  直接用结果
                result.put(
                        executionAttemptId,
                        new ExecutionSlotAssignment(
                                executionAttemptId,
                                requestedPhysicalSlots.getValueByKeyA(executionAttemptId)));
            } else {
                // 之前无结果 产生一个请求id
                final SlotRequestId slotRequestId = new SlotRequestId();
                // 找到需要的资源
                final ResourceProfile resourceProfile =
                        resourceProfileRetriever.apply(executionAttemptId);

                // 找到合适的位置
                Collection<TaskManagerLocation> preferredLocations =
                        preferredLocationsRetriever.getPreferredLocations(
                                executionAttemptId.getExecutionVertexId(), Collections.emptySet());
                final SlotProfile slotProfile =
                        SlotProfile.priorAllocation(
                                resourceProfile,
                                resourceProfile,
                                preferredLocations,
                                Collections.emptyList(),
                                Collections.emptySet());
                final PhysicalSlotRequest request =
                        new PhysicalSlotRequest(
                                slotRequestId, slotProfile, slotWillBeOccupiedIndefinitely);
                physicalSlotRequests.add(request);
                // 将请求id 和execution关联起来
                // 请求id又可以找到 PhysicalSlotRequest
                remainingExecutionsToSlotRequest.put(slotRequestId, executionAttemptId);
            }
        }

        result.putAll(
                allocatePhysicalSlotsFor(remainingExecutionsToSlotRequest, physicalSlotRequests));
        return result;
    }

    /**
     * 这里进行分配
     * @param executionAttemptIds
     * @param slotRequests
     * @return
     */
    private Map<ExecutionAttemptID, ExecutionSlotAssignment> allocatePhysicalSlotsFor(
            Map<SlotRequestId, ExecutionAttemptID> executionAttemptIds,
            List<PhysicalSlotRequest> slotRequests) {

        Map<ExecutionAttemptID, ExecutionSlotAssignment> allocatedSlots = new HashMap<>();

        // 这里已经得到结果了
        Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> slotFutures =
                slotProvider.allocatePhysicalSlots(slotRequests);

        slotFutures.forEach(
                (slotRequestId, slotRequestResultFuture) -> {
                    ExecutionAttemptID executionAttemptId = executionAttemptIds.get(slotRequestId);

                    final CompletableFuture<LogicalSlot> slotFuture =
                            slotRequestResultFuture.thenApply(
                                    physicalSlotRequest ->
                                            allocateLogicalSlotFromPhysicalSlot(
                                                    slotRequestId,
                                                    physicalSlotRequest.getPhysicalSlot(),  // 找到分配的slot
                                                    slotWillBeOccupiedIndefinitely));
                    slotFuture.exceptionally(
                            throwable -> {
                                this.requestedPhysicalSlots.removeKeyA(executionAttemptId);
                                this.slotProvider.cancelSlotRequest(slotRequestId, throwable);
                                return null;
                            });
                    // 设置分配结果
                    requestedPhysicalSlots.put(executionAttemptId, slotRequestId, slotFuture);
                    allocatedSlots.put(
                            executionAttemptId,
                            new ExecutionSlotAssignment(executionAttemptId, slotFuture));
                });
        return allocatedSlots;
    }

    @Override
    public void cancel(ExecutionAttemptID executionAttemptId) {
        final CompletableFuture<LogicalSlot> slotFuture =
                this.requestedPhysicalSlots.getValueByKeyA(executionAttemptId);
        if (slotFuture != null) {
            slotFuture.cancel(false);
        }
    }

    private void returnLogicalSlot(LogicalSlot slot) {
        releaseSlot(
                slot,
                new FlinkException("Slot is being returned from SimpleExecutionSlotAllocator."));
    }

    /**
     * 归还slot
     * @param slot
     * @param cause
     */
    private void releaseSlot(LogicalSlot slot, Throwable cause) {
        // 不再维护之前的分配结果
        requestedPhysicalSlots.removeKeyB(slot.getSlotRequestId());
        slotProvider.cancelSlotRequest(slot.getSlotRequestId(), cause);
    }

    /**
     * 将相关信息包装成 SingleLogicalSlot
     * @param slotRequestId
     * @param physicalSlot
     * @param slotWillBeOccupiedIndefinitely
     * @return
     */
    private LogicalSlot allocateLogicalSlotFromPhysicalSlot(
            final SlotRequestId slotRequestId,
            final PhysicalSlot physicalSlot,
            final boolean slotWillBeOccupiedIndefinitely) {

        final SingleLogicalSlot singleLogicalSlot =
                new SingleLogicalSlot(
                        slotRequestId,
                        physicalSlot,
                        Locality.UNKNOWN,
                        this::returnLogicalSlot,
                        slotWillBeOccupiedIndefinitely);

        final LogicalSlotHolder logicalSlotHolder = new LogicalSlotHolder(singleLogicalSlot);
        if (physicalSlot.tryAssignPayload(logicalSlotHolder)) {
            return singleLogicalSlot;
        } else {
            throw new IllegalStateException(
                    "BUG: Unexpected physical slot payload assignment failure!");
        }
    }

    /**
     * 该对象作为负载 挂在physicalSlot上
     */
    private class LogicalSlotHolder implements PhysicalSlot.Payload {
        private final SingleLogicalSlot logicalSlot;

        private LogicalSlotHolder(SingleLogicalSlot logicalSlot) {
            this.logicalSlot = checkNotNull(logicalSlot);
        }

        @Override
        public void release(Throwable cause) {
            logicalSlot.release(cause);
            releaseSlot(logicalSlot, new FlinkException("Physical slot releases its payload."));
        }

        @Override
        public boolean willOccupySlotIndefinitely() {
            return logicalSlot.willOccupySlotIndefinitely();
        }
    }

    /** Factory to instantiate a {@link SimpleExecutionSlotAllocator}. */
    public static class Factory implements ExecutionSlotAllocatorFactory {
        private final PhysicalSlotProvider slotProvider;

        private final boolean slotWillBeOccupiedIndefinitely;

        public Factory(PhysicalSlotProvider slotProvider, boolean slotWillBeOccupiedIndefinitely) {
            this.slotProvider = slotProvider;
            this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        }

        @Override
        public ExecutionSlotAllocator createInstance(ExecutionSlotAllocationContext context) {
            SyncPreferredLocationsRetriever preferredLocationsRetriever =
                    new DefaultSyncPreferredLocationsRetriever(
                            executionVertexId -> Optional.empty(), context);
            return new SimpleExecutionSlotAllocator(
                    slotProvider,
                    id -> context.getResourceProfile(id.getExecutionVertexId()),
                    preferredLocationsRetriever,
                    slotWillBeOccupiedIndefinitely);
        }
    }
}
