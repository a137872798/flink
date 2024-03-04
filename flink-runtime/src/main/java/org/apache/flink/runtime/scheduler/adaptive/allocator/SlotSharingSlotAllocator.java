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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.util.ResourceCounter;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/** {@link SlotAllocator} implementation that supports slot sharing. */
public class SlotSharingSlotAllocator implements SlotAllocator {

    /**
     * 通过该对象可以申请 slot
     */
    private final ReserveSlotFunction reserveSlotFunction;
    /**
     * 用于释放slot
     */
    private final FreeSlotFunction freeSlotFunction;
    /**
     * 判断某个slot是否空闲
     */
    private final IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction;

    private SlotSharingSlotAllocator(
            ReserveSlotFunction reserveSlot,
            FreeSlotFunction freeSlotFunction,
            IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction) {
        this.reserveSlotFunction = reserveSlot;
        this.freeSlotFunction = freeSlotFunction;
        this.isSlotAvailableAndFreeFunction = isSlotAvailableAndFreeFunction;
    }

    public static SlotSharingSlotAllocator createSlotSharingSlotAllocator(
            ReserveSlotFunction reserveSlot,
            FreeSlotFunction freeSlotFunction,
            IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction) {
        return new SlotSharingSlotAllocator(
                reserveSlot, freeSlotFunction, isSlotAvailableAndFreeFunction);
    }

    @Override
    public ResourceCounter calculateRequiredSlots(
            Iterable<JobInformation.VertexInformation> vertices) {
        int numTotalRequiredSlots = 0;

        // 把每个顶点变成了一个meta
        for (SlotSharingGroupMetaInfo slotSharingGroupMetaInfo :
                SlotSharingGroupMetaInfo.from(vertices).values()) {
            // 根据并行度来判断需要的slot数量
            numTotalRequiredSlots += slotSharingGroupMetaInfo.getMaxUpperBound();
        }
        // 表示不确定资源的开销  但是要求slot的数量为确定的
        return ResourceCounter.withResource(ResourceProfile.UNKNOWN, numTotalRequiredSlots);
    }

    @Override
    public Optional<VertexParallelism> determineParallelism(
            JobInformation jobInformation, Collection<? extends SlotInfo> freeSlots) {

        // 将顶点转换成 以同一共享组为key value 为每个group需要的slot数量
        final Map<SlotSharingGroupId, SlotSharingGroupMetaInfo> slotSharingGroupMetaInfo =
                SlotSharingGroupMetaInfo.from(jobInformation.getVertices());

        // 这是最小的需求量
        final int minimumRequiredSlots =
                slotSharingGroupMetaInfo.values().stream()
                        .map(SlotSharingGroupMetaInfo::getMinLowerBound)
                        .reduce(0, Integer::sum);

        // 表示slot数量不够
        if (minimumRequiredSlots > freeSlots.size()) {
            return Optional.empty();
        }

        // 将freeSlots超出minimumRequiredSlots的部分 分配到各组 并更新 Map<SlotSharingGroupId, Integer>
        final Map<SlotSharingGroupId, Integer> slotSharingGroupParallelism =
                determineSlotsPerSharingGroup(
                        jobInformation,
                        freeSlots.size(),
                        minimumRequiredSlots,
                        slotSharingGroupMetaInfo);

        final Map<JobVertexID, Integer> allVertexParallelism = new HashMap<>();

        // 遍历每个组
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            // 获得顶点信息
            final List<JobInformation.VertexInformation> containedJobVertices =
                    slotSharingGroup.getJobVertexIds().stream()
                            .map(jobInformation::getVertexInformation)
                            .collect(Collectors.toList());

            // 刚才是为每个共享组争取slot  现在看每个顶点能使用多少slot (slot是被多个顶点共享的)
            final Map<JobVertexID, Integer> vertexParallelism =
                    determineVertexParallelism(
                            containedJobVertices,
                            slotSharingGroupParallelism.get(
                                    slotSharingGroup.getSlotSharingGroupId()));
            allVertexParallelism.putAll(vertexParallelism);
        }

        // 得到每个顶点预期分配的slot数量
        return Optional.of(new VertexParallelism(allVertexParallelism));
    }

    /**
     * 产生调度计划
     * @param jobInformation
     * @param slots
     * @param jobAllocationsInformation
     * @return
     */
    @Override
    public Optional<JobSchedulingPlan> determineParallelismAndCalculateAssignment(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> slots,
            JobAllocationsInformation jobAllocationsInformation) {
        return determineParallelism(jobInformation, slots)
                .map(
                        // 这是每个顶点的需求量
                        parallelism -> {
                            SlotAssigner slotAssigner =
                                    jobAllocationsInformation.isEmpty()
                                            ? new DefaultSlotAssigner()
                                            : new StateLocalitySlotAssigner();
                            return new JobSchedulingPlan(
                                    parallelism,
                                    // 得到分配结果
                                    slotAssigner.assignSlots(
                                            jobInformation,
                                            slots,
                                            parallelism,
                                            jobAllocationsInformation));
                        });
    }

    /**
     * Distributes free slots across the slot-sharing groups of the job. Slots are distributed as
     * evenly as possible. If a group requires less than an even share of slots the remainder is
     * distributed over the remaining groups.
     *
     * @param freeSlots 目前给予的空闲slot数量
     * @param minRequiredSlots 这个是最小的需求量
     * @param slotSharingGroupMetaInfo 这是以组为单位存储 每个组的slot需求   每个顶点会关联一个共享组  多个顶点可能对应同一个组
     *
     *                                 这里是将超过最小slot需求量的部分尽可能均匀的分到不同的组
     */
    private static Map<SlotSharingGroupId, Integer> determineSlotsPerSharingGroup(
            JobInformation jobInformation,
            int freeSlots,
            int minRequiredSlots,
            Map<SlotSharingGroupId, SlotSharingGroupMetaInfo> slotSharingGroupMetaInfo) {

        int numUnassignedSlots = freeSlots;
        // 表示会出现的多个共享组
        int numUnassignedSlotSharingGroups = jobInformation.getSlotSharingGroups().size();
        int numMinSlotsRequiredByRemainingGroups = minRequiredSlots;

        final Map<SlotSharingGroupId, Integer> slotSharingGroupParallelism = new HashMap<>();

        // 按照range排序后遍历
        for (SlotSharingGroupId slotSharingGroup :
                sortSlotSharingGroupsByHighestParallelismRange(slotSharingGroupMetaInfo)) {
            // 获取该group的最小并行度 也可以立即为最小的slot需求量
            final int minParallelism =
                    slotSharingGroupMetaInfo.get(slotSharingGroup).getMinLowerBound();

            // if we reached this point we know we have more slots than we need to fulfill the
            // minimum requirements for each slot sharing group.
            // this means that a certain number of slots are already implicitly reserved (to fulfill
            // the minimum requirement of other groups); so we only need to distribute the remaining
            // "optional" slots while only accounting for the requirements beyond the minimum

            // the number of slots this group can use beyond the minimum

            // 这个是差值 也可以理解为这些slot是可选的
            final int maxOptionalSlots =
                    slotSharingGroupMetaInfo.get(slotSharingGroup).getMaxUpperBound()
                            - minParallelism;
            // the number of slots that are not implicitly reserved for minimum requirements
            // 当未分配的 - 最小需求量 就得到空闲的量
            final int freeOptionalSlots = numUnassignedSlots - numMinSlotsRequiredByRemainingGroups;
            // the number of slots this group is allowed to use beyond the minimum requirements
            // 表示每个组可以分到的slot数量
            final int optionalSlotShare = freeOptionalSlots / numUnassignedSlotSharingGroups;

            // 得到最后的并行度
            final int groupParallelism =
                    minParallelism + Math.min(maxOptionalSlots, optionalSlotShare);

            // 更新并行度
            slotSharingGroupParallelism.put(slotSharingGroup, groupParallelism);

            // 修正影响
            numMinSlotsRequiredByRemainingGroups -= minParallelism;
            numUnassignedSlots -= groupParallelism;
            numUnassignedSlotSharingGroups--;
        }

        return slotSharingGroupParallelism;
    }

    /**
     * 按照上下限差距的大小来排序
     * @param slotSharingGroupMetaInfo  这是以共享组为单位 描述每个组需要的slot数量的  数量跟属于该组的顶点的并行度有关
     * @return
     */
    private static List<SlotSharingGroupId> sortSlotSharingGroupsByHighestParallelismRange(
            Map<SlotSharingGroupId, SlotSharingGroupMetaInfo> slotSharingGroupMetaInfo) {

        return slotSharingGroupMetaInfo.entrySet().stream()
                .sorted(
                        Comparator.comparingInt(
                                entry -> entry.getValue().getMaxLowerUpperBoundRange()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    /**
     * 确定每个顶点分配到的slot数量
     * @param containedJobVertices
     * @param availableSlots 可分配数量
     * @return
     */
    private static Map<JobVertexID, Integer> determineVertexParallelism(
            Collection<JobInformation.VertexInformation> containedJobVertices, int availableSlots) {
        final Map<JobVertexID, Integer> vertexParallelism = new HashMap<>();

        // 遍历每个顶点
        for (JobInformation.VertexInformation jobVertex : containedJobVertices) {
            // 按照并行度来分配   其实这些slot是被共用了 可以看出来 因为availableSlots没有减少
            final int parallelism = Math.min(jobVertex.getParallelism(), availableSlots);

            vertexParallelism.put(jobVertex.getJobVertexID(), parallelism);
        }

        return vertexParallelism;
    }

    /**
     * 尝试申请资源
     * @param jobSchedulingPlan information on how slots should be assigned to the slots
     * @return
     */
    @Override
    public Optional<ReservedSlots> tryReserveResources(JobSchedulingPlan jobSchedulingPlan) {
        // 获取被分配的slot id
        final Collection<AllocationID> expectedSlots =
                calculateExpectedSlots(jobSchedulingPlan.getSlotAssignments());

        if (areAllExpectedSlotsAvailableAndFree(expectedSlots)) {
            final Map<ExecutionVertexID, LogicalSlot> assignedSlots = new HashMap<>();

            for (SlotAssignment assignment : jobSchedulingPlan.getSlotAssignments()) {
                final SharedSlot sharedSlot = reserveSharedSlot(assignment.getSlotInfo());
                // 各顶点相同subtaskIndex的 子任务 会共用同一个slot
                for (ExecutionVertexID executionVertexId :
                        assignment
                                .getTargetAs(ExecutionSlotSharingGroup.class)
                                .getContainedExecutionVertices()) {
                    assignedSlots.put(executionVertexId, sharedSlot.allocateLogicalSlot());
                }
            }

            return Optional.of(ReservedSlots.create(assignedSlots));
        } else {
            // 某些slot此时被占用 无法分配
            return Optional.empty();
        }
    }

    @Nonnull
    private Collection<AllocationID> calculateExpectedSlots(Iterable<SlotAssignment> assignments) {
        final Collection<AllocationID> requiredSlots = new ArrayList<>();

        for (SlotAssignment assignment : assignments) {
            requiredSlots.add(assignment.getSlotInfo().getAllocationId());
        }
        return requiredSlots;
    }

    private boolean areAllExpectedSlotsAvailableAndFree(
            Iterable<? extends AllocationID> requiredSlots) {
        for (AllocationID requiredSlot : requiredSlots) {
            if (!isSlotAvailableAndFreeFunction.isSlotAvailableAndFree(requiredSlot)) {
                return false;
            }
        }

        return true;
    }

    /**
     * 申请一个slot
     * @param slotInfo
     * @return
     */
    private SharedSlot reserveSharedSlot(SlotInfo slotInfo) {
        final PhysicalSlot physicalSlot =
                reserveSlotFunction.reserveSlot(
                        slotInfo.getAllocationId(), ResourceProfile.UNKNOWN);

        return new SharedSlot(
                new SlotRequestId(),
                physicalSlot,
                slotInfo.willBeOccupiedIndefinitely(),
                () ->
                        // 这是回收方法
                        freeSlotFunction.freeSlot(
                                slotInfo.getAllocationId(), null, System.currentTimeMillis()));
    }

    /**
     * 表示被共享的资源组  默认情况下 containedExecutionVertices 内部的顶点 subtaskIndex是一样的
     */
    static class ExecutionSlotSharingGroup {
        private final String id;
        private final Set<ExecutionVertexID> containedExecutionVertices;

        public ExecutionSlotSharingGroup(Set<ExecutionVertexID> containedExecutionVertices) {
            this(containedExecutionVertices, UUID.randomUUID().toString());
        }

        public ExecutionSlotSharingGroup(
                Set<ExecutionVertexID> containedExecutionVertices, String id) {
            this.containedExecutionVertices = containedExecutionVertices;
            this.id = id;
        }

        public String getId() {
            return id;
        }

        public Collection<ExecutionVertexID> getContainedExecutionVertices() {
            return containedExecutionVertices;
        }
    }

    /**
     * 某个共享组的元数据
     */
    private static class SlotSharingGroupMetaInfo {

        private final int minLowerBound;
        private final int maxUpperBound;

        /**
         * 表示  maxUpperBound - minLowerBound
         */
        private final int maxLowerUpperBoundRange;

        private SlotSharingGroupMetaInfo(
                int minLowerBound, int maxUpperBound, int maxLowerUpperBoundRange) {
            this.minLowerBound = minLowerBound;
            this.maxUpperBound = maxUpperBound;
            this.maxLowerUpperBoundRange = maxLowerUpperBoundRange;
        }

        public int getMinLowerBound() {
            return minLowerBound;
        }

        public int getMaxUpperBound() {
            return maxUpperBound;
        }

        public int getMaxLowerUpperBoundRange() {
            return maxLowerUpperBoundRange;
        }

        /**
         * 将一组顶点的分配信息转换成map
         * @param vertices
         * @return
         */
        public static Map<SlotSharingGroupId, SlotSharingGroupMetaInfo> from(
                Iterable<JobInformation.VertexInformation> vertices) {

            // 简单看就是填充容器
            return getPerSlotSharingGroups(
                    vertices,
                    // 映射逻辑
                    vertexInformation ->
                            new SlotSharingGroupMetaInfo(
                                    vertexInformation.getMinParallelism(),
                                    vertexInformation.getParallelism(),
                                    vertexInformation.getParallelism()
                                            - vertexInformation.getMinParallelism()),

                    // 累加逻辑
                    (metaInfo1, metaInfo2) ->
                            new SlotSharingGroupMetaInfo(
                                    Math.min(metaInfo1.getMinLowerBound(), metaInfo2.minLowerBound),
                                    Math.max(
                                            metaInfo1.getMaxUpperBound(),
                                            metaInfo2.getMaxUpperBound()),
                                    Math.max(  // 这个计算是不是有问题啊
                                            metaInfo1.getMaxLowerUpperBoundRange(),
                                            metaInfo2.getMaxLowerUpperBoundRange())));
        }

        /**
         *
         * @param vertices  传入的顶点
         * @param mapper
         * @param reducer
         * @param <T>
         * @return
         */
        private static <T> Map<SlotSharingGroupId, T> getPerSlotSharingGroups(
                Iterable<JobInformation.VertexInformation> vertices,
                Function<JobInformation.VertexInformation, T> mapper,
                BiFunction<T, T, T> reducer) {
            final Map<SlotSharingGroupId, T> extractedPerSlotSharingGroups = new HashMap<>();

            // 先遍历每个顶点
            for (JobInformation.VertexInformation vertex : vertices) {
                extractedPerSlotSharingGroups.compute(
                        vertex.getSlotSharingGroup().getSlotSharingGroupId(),
                        (slotSharingGroupId, currentData) ->
                                currentData == null
                                        ? mapper.apply(vertex)  // 映射产生数据
                                        : reducer.apply(currentData, mapper.apply(vertex)));  // 累加数据
            }
            return extractedPerSlotSharingGroups;
        }
    }
}
