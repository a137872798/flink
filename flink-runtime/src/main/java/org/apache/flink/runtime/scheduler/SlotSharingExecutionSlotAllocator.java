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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Allocates {@link LogicalSlot}s from physical shared slots.
 *
 * <p>The allocator maintains a shared slot for each {@link ExecutionSlotSharingGroup}. It allocates
 * a physical slot for the shared slot and then allocates logical slots from it for scheduled tasks.
 * The physical slot is lazily allocated for a shared slot, upon any hosted subtask asking for the
 * shared slot. Each subsequent sharing subtask allocates a logical slot from the existing shared
 * slot. The shared/physical slot can be released only if all the requested logical slots are
 * released or canceled.
 * 该对象不同于SimpleExecutionSlotAllocator  slot会出现被共享的情况
 */
class SlotSharingExecutionSlotAllocator implements ExecutionSlotAllocator {
    private static final Logger LOG =
            LoggerFactory.getLogger(SlotSharingExecutionSlotAllocator.class);

    /**
     * 该对象提供slot
     */
    private final PhysicalSlotProvider slotProvider;

    private final boolean slotWillBeOccupiedIndefinitely;

    /**
     * 该对象可以找到某个 Execution所在的组
     */
    private final SlotSharingStrategy slotSharingStrategy;

    /**
     * 以组为单位分配slot
     * SharedSlot 可以继续分出很多逻辑slot
     */
    private final Map<ExecutionSlotSharingGroup, SharedSlot> sharedSlots;

    /**
     * 该对象可以产生slot的描述信息
     */
    private final SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory;

    /**
     * 该对象用于检查slot分配
     */
    private final PhysicalSlotRequestBulkChecker bulkChecker;

    private final Time allocationTimeout;

    /**
     * 获取Execution需要的资源
     */
    private final Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever;

    SlotSharingExecutionSlotAllocator(
            PhysicalSlotProvider slotProvider,
            boolean slotWillBeOccupiedIndefinitely,
            SlotSharingStrategy slotSharingStrategy,
            SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory,
            PhysicalSlotRequestBulkChecker bulkChecker,
            Time allocationTimeout,
            Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever) {
        this.slotProvider = checkNotNull(slotProvider);
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.slotSharingStrategy = checkNotNull(slotSharingStrategy);
        this.sharedSlotProfileRetrieverFactory = checkNotNull(sharedSlotProfileRetrieverFactory);
        this.bulkChecker = checkNotNull(bulkChecker);
        this.allocationTimeout = checkNotNull(allocationTimeout);
        this.resourceProfileRetriever = checkNotNull(resourceProfileRetriever);
        this.sharedSlots = new IdentityHashMap<>();

        // 禁用批相关的检查
        this.slotProvider.disableBatchSlotRequestTimeoutCheck();
    }

    @Override
    public Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
            List<ExecutionAttemptID> executionAttemptIds) {

        // ExecutionAttemptID 还会记录当前是第几次执行
        final Map<ExecutionVertexID, ExecutionAttemptID> vertexIdToExecutionId = new HashMap<>();
        executionAttemptIds.forEach(
                executionId ->
                        vertexIdToExecutionId.put(executionId.getExecutionVertexId(), executionId));

        checkState(
                vertexIdToExecutionId.size() == executionAttemptIds.size(),
                "SlotSharingExecutionSlotAllocator does not support one execution vertex to have multiple concurrent executions");

        final List<ExecutionVertexID> vertexIds =
                executionAttemptIds.stream()
                        .map(ExecutionAttemptID::getExecutionVertexId)
                        .collect(Collectors.toList());

        // 得到分配结果
        return allocateSlotsForVertices(vertexIds).stream()
                .collect(
                        Collectors.toMap(
                                vertexAssignment ->
                                        vertexIdToExecutionId.get(
                                                vertexAssignment.getExecutionVertexId()),
                                vertexAssignment ->
                                        new ExecutionSlotAssignment(
                                                vertexIdToExecutionId.get(
                                                        vertexAssignment.getExecutionVertexId()),
                                                vertexAssignment.getLogicalSlotFuture())));
    }

    /**
     * Creates logical {@link SlotExecutionVertexAssignment}s from physical shared slots.
     *
     * <p>The allocation has the following steps:
     *
     * <ol>
     *   <li>Map the executions to {@link ExecutionSlotSharingGroup}s using {@link
     *       SlotSharingStrategy}
     *   <li>Check which {@link ExecutionSlotSharingGroup}s already have shared slot
     *   <li>For all involved {@link ExecutionSlotSharingGroup}s which do not have a shared slot
     *       yet:
     *   <li>Create a {@link SlotProfile} future using {@link SharedSlotProfileRetriever} and then
     *   <li>Allocate a physical slot from the {@link PhysicalSlotProvider}
     *   <li>Create a shared slot based on the returned physical slot futures
     *   <li>Allocate logical slot futures for the executions from all corresponding shared slots.
     *   <li>If a physical slot request fails, associated logical slot requests are canceled within
     *       the shared slot
     *   <li>Generate {@link SlotExecutionVertexAssignment}s based on the logical slot futures and
     *       returns the results.
     * </ol>
     *
     * @param executionVertexIds Execution vertices to allocate slots for
     *                           为这组execution分配 slot
     */
    private List<SlotExecutionVertexAssignment> allocateSlotsForVertices(
            List<ExecutionVertexID> executionVertexIds) {

        // 产生检索对象
        SharedSlotProfileRetriever sharedSlotProfileRetriever =
                sharedSlotProfileRetrieverFactory.createFromBulk(new HashSet<>(executionVertexIds));

        // 尝试将execution 分到不同的组里
        Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executionsByGroup =
                executionVertexIds.stream()
                        .collect(
                                Collectors.groupingBy(
                                        slotSharingStrategy::getExecutionSlotSharingGroup));

        // 同一组共享一个SharedSlot
        Map<ExecutionSlotSharingGroup, SharedSlot> slots = new HashMap<>(executionsByGroup.size());
        Set<ExecutionSlotSharingGroup> groupsToAssign = new HashSet<>(executionsByGroup.keySet());

        // 找到已经分配的组
        Map<ExecutionSlotSharingGroup, SharedSlot> assignedSlots =
                tryAssignExistingSharedSlots(groupsToAssign);
        // 存储分配结果
        slots.putAll(assignedSlots);
        groupsToAssign.removeAll(assignedSlots.keySet());

        // 表示还有需要分配的组
        if (!groupsToAssign.isEmpty()) {

            // 这里已经分配好了
            Map<ExecutionSlotSharingGroup, SharedSlot> allocatedSlots =
                    allocateSharedSlots(groupsToAssign, sharedSlotProfileRetriever);
            slots.putAll(allocatedSlots);
            groupsToAssign.removeAll(allocatedSlots.keySet());
            Preconditions.checkState(groupsToAssign.isEmpty());
        }

        // 当以组为单位分配好sharedSlot后  再以execution为单位分配slot
        Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments =
                allocateLogicalSlotsFromSharedSlots(slots, executionsByGroup);

        // we need to pass the slots map to the createBulk method instead of using the allocator's
        // 'sharedSlots'
        // because if any physical slots have already failed, their shared slots have been removed
        // from the allocator's 'sharedSlots' by failed logical slots.
        // 产生请求对象
        SharingPhysicalSlotRequestBulk bulk = createBulk(slots, executionsByGroup);

        // 超时时 触发bulk.cancel
        bulkChecker.schedulePendingRequestBulkTimeoutCheck(bulk, allocationTimeout);

        return executionVertexIds.stream().map(assignments::get).collect(Collectors.toList());
    }

    @Override
    public void cancel(ExecutionAttemptID executionAttemptId) {
        cancelLogicalSlotRequest(executionAttemptId.getExecutionVertexId(), null);
    }

    /**
     * 当分配slot超时时 触发该方法
     * @param executionVertexId
     * @param cause
     */
    private void cancelLogicalSlotRequest(ExecutionVertexID executionVertexId, Throwable cause) {
        ExecutionSlotSharingGroup executionSlotSharingGroup =
                slotSharingStrategy.getExecutionSlotSharingGroup(executionVertexId);
        checkNotNull(
                executionSlotSharingGroup,
                "There is no ExecutionSlotSharingGroup for ExecutionVertexID " + executionVertexId);
        SharedSlot slot = sharedSlots.get(executionSlotSharingGroup);
        if (slot != null) {
            // 取消分配
            slot.cancelLogicalSlotRequest(executionVertexId, cause);
        } else {
            LOG.debug(
                    "There is no SharedSlot for ExecutionSlotSharingGroup of ExecutionVertexID {}",
                    executionVertexId);
        }
    }

    /**
     * 为每个execution 分配slot
     * @param slots  组的分配结果
     * @param executionsByGroup  组下面的execution
     * @return
     */
    private static Map<ExecutionVertexID, SlotExecutionVertexAssignment>
            allocateLogicalSlotsFromSharedSlots(
                    Map<ExecutionSlotSharingGroup, SharedSlot> slots,
                    Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executionsByGroup) {

        Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments = new HashMap<>();

        for (Map.Entry<ExecutionSlotSharingGroup, List<ExecutionVertexID>> entry :
                executionsByGroup.entrySet()) {
            ExecutionSlotSharingGroup group = entry.getKey();
            List<ExecutionVertexID> executionIds = entry.getValue();

            // 遍历execution
            for (ExecutionVertexID executionId : executionIds) {
                // 产生逻辑slot
                CompletableFuture<LogicalSlot> logicalSlotFuture =
                        slots.get(group).allocateLogicalSlot(executionId);
                SlotExecutionVertexAssignment assignment =
                        new SlotExecutionVertexAssignment(executionId, logicalSlotFuture);
                // 添加分配结果
                assignments.put(executionId, assignment);
            }
        }

        return assignments;
    }

    /**
     * 找到已经分配的组
     * @param executionSlotSharingGroups
     * @return
     */
    private Map<ExecutionSlotSharingGroup, SharedSlot> tryAssignExistingSharedSlots(
            Set<ExecutionSlotSharingGroup> executionSlotSharingGroups) {
        Map<ExecutionSlotSharingGroup, SharedSlot> assignedSlots =
                new HashMap<>(executionSlotSharingGroups.size());
        for (ExecutionSlotSharingGroup group : executionSlotSharingGroups) {
            SharedSlot sharedSlot = sharedSlots.get(group);
            if (sharedSlot != null) {
                assignedSlots.put(group, sharedSlot);
            }
        }
        return assignedSlots;
    }

    /**
     * 为这些组分配slot
     * @param executionSlotSharingGroups 待分配的group
     * @param sharedSlotProfileRetriever 该对象用于产生slot描述信息
     * @return
     */
    private Map<ExecutionSlotSharingGroup, SharedSlot> allocateSharedSlots(
            Set<ExecutionSlotSharingGroup> executionSlotSharingGroups,
            SharedSlotProfileRetriever sharedSlotProfileRetriever) {

        List<PhysicalSlotRequest> slotRequests = new ArrayList<>();
        Map<ExecutionSlotSharingGroup, SharedSlot> allocatedSlots = new HashMap<>();

        Map<SlotRequestId, ExecutionSlotSharingGroup> requestToGroup = new HashMap<>();
        Map<SlotRequestId, ResourceProfile> requestToPhysicalResources = new HashMap<>();

        // 遍历每个需要分配的组
        for (ExecutionSlotSharingGroup group : executionSlotSharingGroups) {
            SlotRequestId physicalSlotRequestId = new SlotRequestId();

            // 获得需要的资源
            ResourceProfile physicalSlotResourceProfile = getPhysicalSlotResourceProfile(group);

            // 产生描述对象
            SlotProfile slotProfile =
                    sharedSlotProfileRetriever.getSlotProfile(group, physicalSlotResourceProfile);

            // 包装成请求对象
            PhysicalSlotRequest request =
                    new PhysicalSlotRequest(
                            physicalSlotRequestId, slotProfile, slotWillBeOccupiedIndefinitely);

            // 管理关联关系
            slotRequests.add(request);
            requestToGroup.put(physicalSlotRequestId, group);
            requestToPhysicalResources.put(physicalSlotRequestId, physicalSlotResourceProfile);
        }

        // 得到分配结果
        Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocateResult =
                slotProvider.allocatePhysicalSlots(slotRequests);

        allocateResult.forEach(
                (slotRequestId, resultCompletableFuture) -> {
                    ExecutionSlotSharingGroup group = requestToGroup.get(slotRequestId);
                    CompletableFuture<PhysicalSlot> physicalSlotFuture =
                            resultCompletableFuture.thenApply(
                                    PhysicalSlotRequest.Result::getPhysicalSlot);

                    // 将相关信息包装起来 变成一个共享slot
                    SharedSlot slot =
                            new SharedSlot(
                                    slotRequestId,
                                    requestToPhysicalResources.get(slotRequestId),
                                    group,
                                    physicalSlotFuture,
                                    slotWillBeOccupiedIndefinitely,
                                    this::releaseSharedSlot);
                    allocatedSlots.put(group, slot);
                    Preconditions.checkState(!sharedSlots.containsKey(group));
                    sharedSlots.put(group, slot);
                });
        return allocatedSlots;
    }

    private void releaseSharedSlot(ExecutionSlotSharingGroup executionSlotSharingGroup) {
        SharedSlot slot = sharedSlots.remove(executionSlotSharingGroup);
        Preconditions.checkNotNull(slot);
        Preconditions.checkState(
                slot.isEmpty(),
                "Trying to remove a shared slot with physical request id %s which has assigned logical slots",
                slot.getPhysicalSlotRequestId());
        slotProvider.cancelSlotRequest(
                slot.getPhysicalSlotRequestId(),
                new FlinkException(
                        "Slot is being returned from SlotSharingExecutionSlotAllocator."));
    }

    /**
     * 获取该组的资源描述信息
     * @param executionSlotSharingGroup
     * @return
     */
    private ResourceProfile getPhysicalSlotResourceProfile(
            ExecutionSlotSharingGroup executionSlotSharingGroup) {
        // 表示有有效值
        if (!executionSlotSharingGroup.getResourceProfile().equals(ResourceProfile.UNKNOWN)) {
            return executionSlotSharingGroup.getResourceProfile();
        } else {
            return executionSlotSharingGroup.getExecutionVertexIds().stream()
                    .reduce(
                            // 虽然是共享对象 但是资源需求其实是这些 execution累加的结果
                            ResourceProfile.ZERO,
                            (r, e) -> r.merge(resourceProfileRetriever.apply(e)),
                            ResourceProfile::merge);
        }
    }

    /**
     *
     * @param slots  维护已经分配好的结果
     * @param executions
     * @return
     */
    private SharingPhysicalSlotRequestBulk createBulk(
            Map<ExecutionSlotSharingGroup, SharedSlot> slots,
            Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executions) {
        Map<ExecutionSlotSharingGroup, ResourceProfile> pendingRequests =
                executions.keySet().stream()
                        .collect(
                                Collectors.toMap(
                                        group -> group,
                                        group ->
                                                slots.get(group).getPhysicalSlotResourceProfile()));

        // 产生请求对象
        SharingPhysicalSlotRequestBulk bulk =
                new SharingPhysicalSlotRequestBulk(
                        executions, pendingRequests, this::cancelLogicalSlotRequest);
        // 挨个标记为分配完成
        registerPhysicalSlotRequestBulkCallbacks(slots, executions.keySet(), bulk);
        return bulk;
    }

    /**
     *
     * @param slots
     * @param executions
     * @param bulk
     */
    private static void registerPhysicalSlotRequestBulkCallbacks(
            Map<ExecutionSlotSharingGroup, SharedSlot> slots,
            Iterable<ExecutionSlotSharingGroup> executions,
            SharingPhysicalSlotRequestBulk bulk) {
        for (ExecutionSlotSharingGroup group : executions) {
            CompletableFuture<PhysicalSlot> slotContextFuture =
                    slots.get(group).getSlotContextFuture();
            // 当完成时 触发标记
            slotContextFuture.thenAccept(
                    physicalSlot -> bulk.markFulfilled(group, physicalSlot.getAllocationId()));
            slotContextFuture.exceptionally(
                    t -> {
                        // clear the bulk to stop the fulfillability check
                        bulk.clearPendingRequests();
                        return null;
                    });
        }
    }

    /** The slot assignment for an {@link ExecutionVertex}. */
    private static class SlotExecutionVertexAssignment {

        private final ExecutionVertexID executionVertexId;

        /**
         * 注意这里是逻辑slot  也就是先不考虑group
         */
        private final CompletableFuture<LogicalSlot> logicalSlotFuture;

        SlotExecutionVertexAssignment(
                ExecutionVertexID executionVertexId,
                CompletableFuture<LogicalSlot> logicalSlotFuture) {
            this.executionVertexId = checkNotNull(executionVertexId);
            this.logicalSlotFuture = checkNotNull(logicalSlotFuture);
        }

        ExecutionVertexID getExecutionVertexId() {
            return executionVertexId;
        }

        CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
            return logicalSlotFuture;
        }
    }
}
