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

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionTopology;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This strategy tries to reduce remote data exchanges. Execution vertices, which are connected and
 * belong to the same SlotSharingGroup, tend to be put in the same ExecutionSlotSharingGroup.
 * Co-location constraints will be respected.
 * 该对象还会监听拓扑图的变化
 */
class LocalInputPreferredSlotSharingStrategy
        implements SlotSharingStrategy, SchedulingTopologyListener {

    /**
     * 记录每个Execution所在的组
     */
    private final Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroupMap;

    /**
     * 这也是共享组
     */
    private final Set<SlotSharingGroup> logicalSlotSharingGroups;

    /**
     * 这个是位置组
     */
    private final Set<CoLocationGroup> coLocationGroups;

    /**
     *
     * @param topology  使用拓扑图初始化
     * @param logicalSlotSharingGroups  初始化的时候已经传了组
     * @param coLocationGroups
     */
    LocalInputPreferredSlotSharingStrategy(
            final SchedulingTopology topology,
            final Set<SlotSharingGroup> logicalSlotSharingGroups,
            final Set<CoLocationGroup> coLocationGroups) {

        this.logicalSlotSharingGroups = checkNotNull(logicalSlotSharingGroups);
        this.coLocationGroups = checkNotNull(coLocationGroups);

        this.executionSlotSharingGroupMap =
                new ExecutionSlotSharingGroupBuilder(
                                topology, logicalSlotSharingGroups, coLocationGroups)
                        .build();
        // 监听拓扑图变化
        topology.registerSchedulingTopologyListener(this);
    }

    @Override
    public ExecutionSlotSharingGroup getExecutionSlotSharingGroup(
            final ExecutionVertexID executionVertexId) {
        return executionSlotSharingGroupMap.get(executionVertexId);
    }

    @Override
    public Set<ExecutionSlotSharingGroup> getExecutionSlotSharingGroups() {
        return new HashSet<>(executionSlotSharingGroupMap.values());
    }

    /**
     * 通知拓扑图变化
     * @param schedulingTopology the scheduling topology which is just updated
     * @param newExecutionVertices the newly added execution vertices.
     *
     */
    @Override
    public void notifySchedulingTopologyUpdated(
            SchedulingTopology schedulingTopology, List<ExecutionVertexID> newExecutionVertices) {

        // 重新生成map
        final Map<ExecutionVertexID, ExecutionSlotSharingGroup> newMap =
                new LocalInputPreferredSlotSharingStrategy.ExecutionSlotSharingGroupBuilder(
                                schedulingTopology, logicalSlotSharingGroups, coLocationGroups)
                        .build();

        for (ExecutionVertexID vertexId : newMap.keySet()) {
            final ExecutionSlotSharingGroup newEssg = newMap.get(vertexId);
            final ExecutionSlotSharingGroup oldEssg = executionSlotSharingGroupMap.get(vertexId);
            if (oldEssg == null) {
                executionSlotSharingGroupMap.put(vertexId, newEssg);
            } else {
                // ensures that existing slot sharing groups are not changed
                // 做了检测
                checkState(
                        oldEssg.getExecutionVertexIds().equals(newEssg.getExecutionVertexIds()),
                        "Existing ExecutionSlotSharingGroups are changed after topology update");
            }
        }
    }

    static class Factory implements SlotSharingStrategy.Factory {

        public LocalInputPreferredSlotSharingStrategy create(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> logicalSlotSharingGroups,
                final Set<CoLocationGroup> coLocationGroups) {

            return new LocalInputPreferredSlotSharingStrategy(
                    topology, logicalSlotSharingGroups, coLocationGroups);
        }
    }

    /**
     * 通过该对象可以挖掘共享组
     */
    private static class ExecutionSlotSharingGroupBuilder {
        private final SchedulingTopology topology;

        /**
         * 通过 task级别id 可以检索到组
         */
        private final Map<JobVertexID, SlotSharingGroup> slotSharingGroupMap;

        /**
         * 检索位置组
         */
        private final Map<JobVertexID, CoLocationGroup> coLocationGroupMap;

        private final Map<ExecutionVertexID, ExecutionSlotSharingGroup>
                executionSlotSharingGroupMap;

        private final Map<CoLocationConstraint, ExecutionSlotSharingGroup>
                constraintToExecutionSlotSharingGroupMap;

        /**
         * A JobVertex only belongs to one {@link SlotSharingGroup}. A SlotSharingGroup is
         * corresponding to a set of {@link ExecutionSlotSharingGroup}s. We can maintain available
         * ExecutionSlotSharingGroups for each JobVertex.
         *
         * <p>Once an ExecutionSlotSharingGroup is created, it becomes available for all JobVertices
         * in the corresponding SlotSharingGroup in the beginning.
         *
         * <p>Once a SchedulingExecutionVertex is added to the ExecutionSlotSharingGroup, the group
         * is no longer available for other SchedulingExecutionVertices with the same JobVertexID.
         *
         * <p>Here we use {@link LinkedHashSet} to reserve the order the same as the
         * SchedulingVertices are traversed.
         * 同一个task(JobVertexID) 下的 execution 不能属于同一个ExecutionSlotSharingGroup
         * 然后因为属于同一个组的JobVertexID 会有相同的ExecutionSlotSharingGroup 也就是允许他们有一个subtask属于同一ExecutionSlotSharingGroup
         */
        private final Map<JobVertexID, LinkedHashSet<ExecutionSlotSharingGroup>>
                availableGroupsForJobVertex;

        /**
         * Maintains the candidate {@link ExecutionSlotSharingGroup}s for every {@link
         * ConsumedPartitionGroup}. The ConsumedPartitionGroup represents a group of partitions that
         * is consumed by the same ExecutionVertices. These ExecutionVertices belong to one consumer
         * JobVertex. Thus, we can say, a ConsumedPartitionGroup is corresponding to one consumer
         * JobVertex.
         *
         * <p>This mapping is used to find an available producer ExecutionSlotSharingGroup for the
         * consumer vertex. If a candidate group is available for this consumer vertex, it will be
         * assigned to this vertex.
         *
         * <p>The candidate groups are computed in {@link
         * #computeAllCandidateGroupsForConsumedPartitionGroup} when the ConsumedPartitionGroup is
         * traversed for the first time.
         *
         * <p>Here we use {@link LinkedHashSet} to reserve the order the same as the
         * SchedulingVertices are traversed.
         */
        private final Map<ConsumedPartitionGroup, LinkedHashSet<ExecutionSlotSharingGroup>>
                candidateGroupsForConsumedPartitionGroup;

        /**
         * 使用2个组初始化
         * @param topology
         * @param logicalSlotSharingGroups
         * @param coLocationGroups
         */
        private ExecutionSlotSharingGroupBuilder(
                final SchedulingTopology topology,
                final Set<SlotSharingGroup> logicalSlotSharingGroups,
                final Set<CoLocationGroup> coLocationGroups) {

            this.topology = checkNotNull(topology);

            this.slotSharingGroupMap = new HashMap<>();
            // 建立索引信息
            for (SlotSharingGroup slotSharingGroup : logicalSlotSharingGroups) {
                for (JobVertexID jobVertexId : slotSharingGroup.getJobVertexIds()) {
                    slotSharingGroupMap.put(jobVertexId, slotSharingGroup);
                }
            }

            // 建立索引信息
            this.coLocationGroupMap = new HashMap<>();
            for (CoLocationGroup coLocationGroup : coLocationGroups) {
                for (JobVertexID jobVertexId : coLocationGroup.getVertexIds()) {
                    coLocationGroupMap.put(jobVertexId, coLocationGroup);
                }
            }

            executionSlotSharingGroupMap = new HashMap<>();
            constraintToExecutionSlotSharingGroupMap = new HashMap<>();
            availableGroupsForJobVertex = new HashMap<>();
            candidateGroupsForConsumedPartitionGroup = new IdentityHashMap<>();
        }

        /**
         * Build ExecutionSlotSharingGroups for all vertices in the topology. The
         * ExecutionSlotSharingGroup of a vertex is determined in order below:
         *
         * <p>1. try finding an existing group of the corresponding co-location constraint.
         *
         * <p>2. try finding an available group of its producer vertex if the producer is in the
         * same slot sharing group.
         *
         * <p>3. try finding any available group.
         *
         * <p>4. create a new group.
         * 基于初始化的组信息 为Execution分配组
         */
        private Map<ExecutionVertexID, ExecutionSlotSharingGroup> build() {

            // 产生 <Task,Subtask> 映射
            final LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> allVertices =
                    getExecutionVertices();

            // loop on job vertices so that an execution vertex will not be added into a group
            // if that group better fits another execution vertex
            for (List<SchedulingExecutionVertex> executionVertices : allVertices.values()) {
                // 遍历每个task下的所有subtask
                final List<SchedulingExecutionVertex> remaining =
                        tryFindOptimalAvailableExecutionSlotSharingGroupFor(executionVertices);

                // 先创建组 remaining 记录了还没相关组的execution
                findAvailableOrCreateNewExecutionSlotSharingGroupFor(remaining);

                // 更新Constraint
                updateConstraintToExecutionSlotSharingGroupMap(executionVertices);
            }

            return executionSlotSharingGroupMap;
        }

        /**
         * The vertices are topologically sorted since {@link DefaultExecutionTopology#getVertices}
         * are topologically sorted.
         */
        private LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> getExecutionVertices() {
            final LinkedHashMap<JobVertexID, List<SchedulingExecutionVertex>> vertices =
                    new LinkedHashMap<>();

            // 对标的是 subtask
            for (SchedulingExecutionVertex executionVertex : topology.getVertices()) {
                final List<SchedulingExecutionVertex> executionVertexGroup =
                        vertices.computeIfAbsent(
                                executionVertex.getId().getJobVertexId(), k -> new ArrayList<>());
                executionVertexGroup.add(executionVertex);
            }
            return vertices;
        }

        /**
         *
         * @param executionVertices  属于同一个task的所有subtask
         * @return
         */
        private List<SchedulingExecutionVertex> tryFindOptimalAvailableExecutionSlotSharingGroupFor(
                final List<SchedulingExecutionVertex> executionVertices) {

            final List<SchedulingExecutionVertex> remaining = new ArrayList<>();
            // 遍历每个subtask
            for (SchedulingExecutionVertex executionVertex : executionVertices) {

                // 尝试找到匹配的组 找到就直接触发 addVertexToExecutionSlotSharingGroup
                // (优先按照位置限制来分组)
                ExecutionSlotSharingGroup group =
                        tryFindAvailableCoLocatedExecutionSlotSharingGroupFor(executionVertex);

                // 2种情况  第一种 constraintToExecutionSlotSharingGroupMap 信息还未填充 无法快速找到共享同一constraint 且 subtaskIndex一样的组
                // 第二种 就是 subtaskIndex不同 或者 job没有共享 constraint
                if (group == null) {
                    group = tryFindAvailableProducerExecutionSlotSharingGroupFor(executionVertex);
                }

                if (group == null) {
                    remaining.add(executionVertex);
                } else {
                    addVertexToExecutionSlotSharingGroup(executionVertex, group);
                }
            }

            return remaining;
        }

        /**
         * 为该子任务 寻找一个组
         * @param executionVertex
         * @return
         */
        private ExecutionSlotSharingGroup tryFindAvailableCoLocatedExecutionSlotSharingGroupFor(
                final SchedulingExecutionVertex executionVertex) {

            final ExecutionVertexID executionVertexId = executionVertex.getId();
            // 先找到task相关的位置组
            final CoLocationGroup coLocationGroup =
                    coLocationGroupMap.get(executionVertexId.getJobVertexId());
            if (coLocationGroup != null) {
                final CoLocationConstraint constraint =
                        coLocationGroup.getLocationConstraint(executionVertexId.getSubtaskIndex());

                // 表示发现共享同一CoLocationGroup 的其他task的相同 subtaskIndex的 Execution 已经先创建了 ExecutionSlotSharingGroup
                // 本对象也加入同一个组
                return constraintToExecutionSlotSharingGroupMap.get(constraint);
            } else {
                return null;
            }
        }

        /**
         * 为子任务找到一个组
         * @param executionVertex
         * @return
         */
        private ExecutionSlotSharingGroup tryFindAvailableProducerExecutionSlotSharingGroupFor(
                final SchedulingExecutionVertex executionVertex) {

            final ExecutionVertexID executionVertexId = executionVertex.getId();

            // 由本节点消费的数据
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    executionVertex.getConsumedPartitionGroups()) {

                // 当发现上游的task与本Execution的task属于同一个组 先获取上游subtask所在的共享组   这些共享组就是本对象的候选
                Set<ExecutionSlotSharingGroup> candidateGroups =
                        candidateGroupsForConsumedPartitionGroup.computeIfAbsent(
                                consumedPartitionGroup,
                                group ->
                                        computeAllCandidateGroupsForConsumedPartitionGroup(
                                                executionVertexId.getJobVertexId(), group));

                Iterator<ExecutionSlotSharingGroup> candidateIterator = candidateGroups.iterator();

                while (candidateIterator.hasNext()) {
                    ExecutionSlotSharingGroup candidateGroup = candidateIterator.next();
                    // There are two cases for this candidate group:
                    //
                    // 1. The group is available for this vertex, and it will be assigned to this
                    // vertex;
                    // 2. The group is not available for this vertex, because it's already assigned
                    // to another vertex with the same JobVertexID.
                    //
                    // No matter what case it is, the candidate group is no longer a candidate and
                    // should be removed.
                    candidateIterator.remove();
                    // 因为task下每个subtask只能属于一个 Execution共享组 所以要先确保该组在该task下还未有别的subtask占用
                    if (isExecutionSlotSharingGroupAvailableForVertex(
                            candidateGroup, executionVertexId)) {
                        return candidateGroup;
                    }
                }
            }

            return null;
        }

        /**
         * @param executionSlotSharingGroup  一组候选组
         * @param vertexId
         * @return
         */
        private boolean isExecutionSlotSharingGroupAvailableForVertex(
                ExecutionSlotSharingGroup executionSlotSharingGroup, ExecutionVertexID vertexId) {

            // task下各 subtask 所属的组
            Set<ExecutionSlotSharingGroup> availableGroupsForCurrentVertex =
                    availableGroupsForJobVertex.get(vertexId.getJobVertexId());

            // 确保该task下的该组还未被subtask占用
            return availableGroupsForCurrentVertex != null
                    && availableGroupsForCurrentVertex.contains(executionSlotSharingGroup);
        }

        /**
         * 一个是产生数据的task 一个是消费数据的task
         * @param jobVertexId1
         * @param jobVertexId2
         * @return
         */
        private boolean inSameLogicalSlotSharingGroup(
                final JobVertexID jobVertexId1, final JobVertexID jobVertexId2) {

            return Objects.equals(
                    getSlotSharingGroup(jobVertexId1).getSlotSharingGroupId(),
                    getSlotSharingGroup(jobVertexId2).getSlotSharingGroupId());
        }

        private SlotSharingGroup getSlotSharingGroup(final JobVertexID jobVertexId) {
            // slot sharing group of a vertex would never be null in production
            return checkNotNull(slotSharingGroupMap.get(jobVertexId));
        }

        /**
         * 添加映射关系
         * 此时该执行对象被认为属于这个组
         * @param vertex
         * @param group
         */
        private void addVertexToExecutionSlotSharingGroup(
                final SchedulingExecutionVertex vertex, final ExecutionSlotSharingGroup group) {

            ExecutionVertexID executionVertexId = vertex.getId();
            group.addVertex(executionVertexId);
            // 添加映射关系
            executionSlotSharingGroupMap.put(executionVertexId, group);

            // The ExecutionSlotSharingGroup is no longer available for the JobVertex
            // 表示该task下的其他subtask无法加入该组了 已经被移除了
            Set<ExecutionSlotSharingGroup> availableExecutionSlotSharingGroups =
                    availableGroupsForJobVertex.get(executionVertexId.getJobVertexId());
            if (availableExecutionSlotSharingGroups != null) {
                availableExecutionSlotSharingGroups.remove(group);
            }
        }

        /**
         * 按照其他规则找组
         * @param executionVertices
         */
        private void findAvailableOrCreateNewExecutionSlotSharingGroupFor(
                final List<SchedulingExecutionVertex> executionVertices) {

            for (SchedulingExecutionVertex executionVertex : executionVertices) {

                ExecutionSlotSharingGroup group =
                        tryFindAvailableExecutionSlotSharingGroupFor(executionVertex);

                if (group == null) {
                    // 表示该job此时没有可用的组了 进行添加 并立即分配给executionVertex
                    group = createNewExecutionSlotSharingGroup(executionVertex.getId());
                }

                addVertexToExecutionSlotSharingGroup(executionVertex, group);
            }
        }

        /**
         * @param executionVertex
         * @return
         */
        private ExecutionSlotSharingGroup tryFindAvailableExecutionSlotSharingGroupFor(
                SchedulingExecutionVertex executionVertex) {

            Set<ExecutionSlotSharingGroup> availableGroupsForCurrentVertex =
                    availableGroupsForJobVertex.get(executionVertex.getId().getJobVertexId());

            // 找一个还未被占用的组分配出去
            if (availableGroupsForCurrentVertex != null
                    && !availableGroupsForCurrentVertex.isEmpty()) {
                return availableGroupsForCurrentVertex.iterator().next();
            }

            return null;
        }

        /**
         * 这里是最开始
         * @param executionVertexId  注意这时executionVertexId还没有加入组
         * @return
         */
        private ExecutionSlotSharingGroup createNewExecutionSlotSharingGroup(
                ExecutionVertexID executionVertexId) {
            // 第一步 先找到task相关的组
            final SlotSharingGroup slotSharingGroup =
                    getSlotSharingGroup(executionVertexId.getJobVertexId());

            // 初始化一个针对Execution的组
            final ExecutionSlotSharingGroup newGroup = new ExecutionSlotSharingGroup();
            // 设置资源消耗
            newGroup.setResourceProfile(slotSharingGroup.getResourceProfile());

            // Once a new ExecutionSlotSharingGroup is created, it's available for all JobVertices
            // in this SlotSharingGroup
            // 这个操作配合remove 意味着 属于同一组的task 他们的每个subtask有加入一个组的机会 加入后ExecutionSlotSharingGroup就会remove 下次就访问不到了
            for (JobVertexID jobVertexId : slotSharingGroup.getJobVertexIds()) {
                Set<ExecutionSlotSharingGroup> availableExecutionSlotSharingGroups =
                        availableGroupsForJobVertex.computeIfAbsent(
                                jobVertexId, ignore -> new LinkedHashSet<>());
                availableExecutionSlotSharingGroups.add(newGroup);
            }

            return newGroup;
        }

        /**
         *
         * @param executionVertices
         */
        private void updateConstraintToExecutionSlotSharingGroupMap(
                final List<SchedulingExecutionVertex> executionVertices) {

            for (SchedulingExecutionVertex executionVertex : executionVertices) {
                final ExecutionVertexID executionVertexId = executionVertex.getId();
                // 找到相关的组
                final CoLocationGroup coLocationGroup =
                        coLocationGroupMap.get(executionVertexId.getJobVertexId());
                if (coLocationGroup != null) {
                    final CoLocationConstraint constraint =
                            coLocationGroup.getLocationConstraint(
                                    executionVertexId.getSubtaskIndex());

                    // 这样子的操作会使得 处于共享组的所有task下的 下标相同的 subtask 会归到一个ExecutionSlotSharingGroup里
                    constraintToExecutionSlotSharingGroupMap.put(
                            constraint, executionSlotSharingGroupMap.get(executionVertexId));
                }
            }
        }

        /**
         *
         * @param consumerJobVertexId 某个顶点
         * @param consumedPartitionGroup 会去消费的数据
         * @return
         */
        private LinkedHashSet<ExecutionSlotSharingGroup>
                computeAllCandidateGroupsForConsumedPartitionGroup(
                        JobVertexID consumerJobVertexId,
                        ConsumedPartitionGroup consumedPartitionGroup) {

            // We tend to reserve the order of ExecutionSlotSharingGroups as they are traversed
            // topologically
            final LinkedHashSet<ExecutionSlotSharingGroup> candidateExecutionSlotSharingGroups =
                    new LinkedHashSet<>();

            // 找到产生中间结果集的某个task
            JobVertexID producerJobVertexId =
                    topology.getResultPartition(consumedPartitionGroup.getFirst())
                            .getProducer()
                            .getId()
                            .getJobVertexId();

            // Check if the producer JobVertex and the consumer JobVertex are in the same
            // SlotSharingGroup
            // 表示产生数据和消费数据的task在同一个组
            if (inSameLogicalSlotSharingGroup(producerJobVertexId, consumerJobVertexId)) {

                // Iterate over the producer ExecutionVertices of all the partitions in the
                // ConsumedPartitionGroup
                for (IntermediateResultPartitionID consumedPartition : consumedPartitionGroup) {

                    // 获取产生数据的子任务id
                    ExecutionVertexID producerExecutionVertexId =
                            topology.getResultPartition(consumedPartition).getProducer().getId();

                    // 找到对应的组
                    ExecutionSlotSharingGroup assignedGroupForProducerExecutionVertex =
                            executionSlotSharingGroupMap.get(producerExecutionVertexId);
                    checkNotNull(assignedGroupForProducerExecutionVertex);

                    // 把上游已经存在的组返回
                    candidateExecutionSlotSharingGroups.add(
                            assignedGroupForProducerExecutionVertex);
                }
            }

            return candidateExecutionSlotSharingGroups;
        }
    }
}
