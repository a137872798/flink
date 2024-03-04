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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Simple {@link SlotAssigner} that treats all slots and slot sharing groups equally.
 * 默认的slot分配对象   slot表示资源
 * */
public class DefaultSlotAssigner implements SlotAssigner {

    /**
     * 将一组slot分配给job
     * @param jobInformation
     * @param freeSlots  表示此时还空闲的slot
     * @param vertexParallelism    该job下各顶点的并行度
     * @param previousAllocations
     * @return
     */
    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            JobAllocationsInformation previousAllocations) {

        // 将共享资源的组 按照subtaskIndex 进一步划分
        List<ExecutionSlotSharingGroup> allGroups = new ArrayList<>();
        // 该job相关的多个slot共享组
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            allGroups.addAll(createExecutionSlotSharingGroups(vertexParallelism, slotSharingGroup));
        }

        Iterator<? extends SlotInfo> iterator = freeSlots.iterator();

        // 记录slot的分配结果
        Collection<SlotAssignment> assignments = new ArrayList<>();

        // 把slot平均分配给每个group  默认情况下最多只会分配与group等量的slot
        for (ExecutionSlotSharingGroup group : allGroups) {
            assignments.add(new SlotAssignment(iterator.next(), group));
        }
        return assignments;
    }

    /**
     *
     * @param vertexParallelism  记录多个顶点的并行度    并行度决定了该task会有多少subtask
     * @param slotSharingGroup  记录共享该资源的一组顶点 或者说task
     * @return
     */
    static List<ExecutionSlotSharingGroup> createExecutionSlotSharingGroups(
            VertexParallelism vertexParallelism, SlotSharingGroup slotSharingGroup) {

        // 以子任务下标为key value存储相关的顶点
        final Map<Integer, Set<ExecutionVertexID>> sharedSlotToVertexAssignment = new HashMap<>();
        slotSharingGroup
                .getJobVertexIds()
                .forEach(
                        jobVertexId -> {
                            // 获取该顶点的并行度
                            int parallelism = vertexParallelism.getParallelism(jobVertexId);

                            // 按照并行度 遍历子任务
                            for (int subtaskIdx = 0; subtaskIdx < parallelism; subtaskIdx++) {
                                sharedSlotToVertexAssignment
                                        .computeIfAbsent(subtaskIdx, ignored -> new HashSet<>())
                                        .add(new ExecutionVertexID(jobVertexId, subtaskIdx));
                            }
                        });
        return sharedSlotToVertexAssignment.values().stream()
                .map(ExecutionSlotSharingGroup::new)
                .collect(Collectors.toList());
    }
}
