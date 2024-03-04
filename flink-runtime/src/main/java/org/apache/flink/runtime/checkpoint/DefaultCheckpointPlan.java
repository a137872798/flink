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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.OperatorStateHandle;

import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The default implementation of he {@link CheckpointPlan}. */
public class DefaultCheckpointPlan implements CheckpointPlan {

    /**
     * 表示一组需要执行的任务
     */
    private final List<Execution> tasksToTrigger;

    /**
     * 一组等待的任务
     */
    private final List<Execution> tasksToWaitFor;

    /**
     * 一组需要提交的任务
     */
    private final List<ExecutionVertex> tasksToCommitTo;

    /**
     * 一组已经完成的任务
     */
    private final List<Execution> finishedTasks;

    private final boolean mayHaveFinishedTasks;

    private final Map<JobVertexID, ExecutionJobVertex> fullyFinishedOrFinishedOnRestoreVertices;

    private final IdentityHashMap<ExecutionJobVertex, Integer> vertexOperatorsFinishedTasksCount;

    /**
     * 检查点计划 维护着各种execution
     * @param tasksToTrigger
     * @param tasksToWaitFor
     * @param tasksToCommitTo
     * @param finishedTasks
     * @param fullyFinishedJobVertex
     * @param mayHaveFinishedTasks
     */
    DefaultCheckpointPlan(
            List<Execution> tasksToTrigger,
            List<Execution> tasksToWaitFor,
            List<ExecutionVertex> tasksToCommitTo,
            List<Execution> finishedTasks,
            List<ExecutionJobVertex> fullyFinishedJobVertex,
            boolean mayHaveFinishedTasks) {

        this.tasksToTrigger = checkNotNull(tasksToTrigger);
        this.tasksToWaitFor = checkNotNull(tasksToWaitFor);
        this.tasksToCommitTo = checkNotNull(tasksToCommitTo);
        this.finishedTasks = checkNotNull(finishedTasks);
        this.mayHaveFinishedTasks = mayHaveFinishedTasks;

        this.fullyFinishedOrFinishedOnRestoreVertices = new HashMap<>();

        // 把这组对象填充到 fullyFinishedOrFinishedOnRestoreVertices 内
        fullyFinishedJobVertex.forEach(
                jobVertex ->
                        fullyFinishedOrFinishedOnRestoreVertices.put(
                                jobVertex.getJobVertexId(), jobVertex));

        this.vertexOperatorsFinishedTasksCount = new IdentityHashMap<>();
    }

    @Override
    public List<Execution> getTasksToTrigger() {
        return tasksToTrigger;
    }

    @Override
    public List<Execution> getTasksToWaitFor() {
        return tasksToWaitFor;
    }

    @Override
    public List<ExecutionVertex> getTasksToCommitTo() {
        return tasksToCommitTo;
    }

    @Override
    public List<Execution> getFinishedTasks() {
        return finishedTasks;
    }

    @Override
    public Collection<ExecutionJobVertex> getFullyFinishedJobVertex() {
        return fullyFinishedOrFinishedOnRestoreVertices.values();
    }

    @Override
    public boolean mayHaveFinishedTasks() {
        return mayHaveFinishedTasks;
    }

    /**
     * 当某个task报告已经完成恢复 就加入对应的容器
     * @param task
     */
    @Override
    public void reportTaskFinishedOnRestore(ExecutionVertex task) {
        fullyFinishedOrFinishedOnRestoreVertices.putIfAbsent(
                task.getJobvertexId(), task.getJobVertex());
    }

    /**
     * 增加次数
     * @param task
     */
    @Override
    public void reportTaskHasFinishedOperators(ExecutionVertex task) {
        vertexOperatorsFinishedTasksCount.compute(
                task.getJobVertex(), (k, v) -> v == null ? 1 : v + 1);
    }

    /**
     *
     * @param operatorStates  相当于用本对象内部的finished数据来填充 operatorStates
     */
    @Override
    public void fulfillFinishedTaskStatus(Map<OperatorID, OperatorState> operatorStates) {
        // 此时还没有完成的任务  直接返回
        if (!mayHaveFinishedTasks) {
            return;
        }

        Map<JobVertexID, ExecutionJobVertex> partlyFinishedVertex = new HashMap<>();
        // 遍历所有已经完成的task
        for (Execution task : finishedTasks) {
            JobVertexID jobVertexId = task.getVertex().getJobvertexId();
            // 不在完全结束的容器中  就加入到部分结束的容器
            if (!fullyFinishedOrFinishedOnRestoreVertices.containsKey(jobVertexId)) {
                partlyFinishedVertex.put(jobVertexId, task.getVertex().getJobVertex());
            }
        }

        // 这2个方法都是检测是否有union
        checkNoPartlyFinishedVertexUsedUnionListState(partlyFinishedVertex, operatorStates);
        checkNoPartlyOperatorsFinishedVertexUsedUnionListState(
                partlyFinishedVertex, operatorStates);

        // 根据fullyFinishedOrFinishedOnRestoreVertices的信息 将operatorStates中匹配的状态更新成fully
        fulfillFullyFinishedOrFinishedOnRestoreOperatorStates(operatorStates);
        fulfillSubtaskStateForPartiallyFinishedOperators(operatorStates);
    }

    /**
     * If a job vertex using {@code UnionListState} has part of tasks FINISHED where others are
     * still in RUNNING state, the checkpoint would be aborted since it might cause incomplete
     * {@code UnionListState}.
     */
    private void checkNoPartlyFinishedVertexUsedUnionListState(
            Map<JobVertexID, ExecutionJobVertex> partlyFinishedVertex,
            Map<OperatorID, OperatorState> operatorStates) {
        for (ExecutionJobVertex vertex : partlyFinishedVertex.values()) {
            // 如果job与入参有交集 并且mode为union 抛出异常
            if (hasUsedUnionListState(vertex, operatorStates)) {
                throw new PartialFinishingNotSupportedByStateException(
                        String.format(
                                "The vertex %s (id = %s) has used"
                                        + " UnionListState, but part of its tasks are FINISHED.",
                                vertex.getName(), vertex.getJobVertexId()));
            }
        }
    }

    /**
     * If a job vertex using {@code UnionListState} has all the tasks in RUNNING state, but part of
     * the tasks have reported that the operators are finished, the checkpoint would be aborted.
     * This is to force the fast tasks wait for the slow tasks so that their final checkpoints would
     * be the same one, otherwise if the fast tasks finished, the slow tasks would be blocked
     * forever since all the following checkpoints would be aborted.
     */
    private void checkNoPartlyOperatorsFinishedVertexUsedUnionListState(
            Map<JobVertexID, ExecutionJobVertex> partlyFinishedVertex,
            Map<OperatorID, OperatorState> operatorStates) {
        for (Map.Entry<ExecutionJobVertex, Integer> entry :
                vertexOperatorsFinishedTasksCount.entrySet()) {
            ExecutionJobVertex vertex = entry.getKey();

            // If the vertex is partly finished, then it must not used UnionListState
            // due to it passed the previous check.
            // 表示在checkNoPartlyFinishedVertexUsedUnionListState已经完成检查了 跳过
            if (partlyFinishedVertex.containsKey(vertex.getJobVertexId())) {
                continue;
            }

            if (entry.getValue() != vertex.getParallelism()  // 表示并不是所有子任务都已经完成  也就是部分完成
                    && hasUsedUnionListState(vertex, operatorStates)) {  // 部分完成才需要检测
                throw new PartialFinishingNotSupportedByStateException(
                        String.format(
                                "The vertex %s (id = %s) has used"
                                        + " UnionListState, but part of its tasks has called operators' finish method.",
                                vertex.getName(), vertex.getJobVertexId()));
            }
        }
    }

    private boolean hasUsedUnionListState(
            ExecutionJobVertex vertex, Map<OperatorID, OperatorState> operatorStates) {

        // 遍历涉及到的所有operator
        for (OperatorIDPair operatorIDPair : vertex.getOperatorIDs()) {
            OperatorState operatorState =
                    operatorStates.get(operatorIDPair.getGeneratedOperatorID());
            // 该算子与入参无关   跳过
            if (operatorState == null) {
                continue;
            }

            // 获取关联的所有子状态
            for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {
                boolean hasUnionListState =
                        Stream.concat(
                                        operatorSubtaskState.getManagedOperatorState().stream(),
                                        operatorSubtaskState.getRawOperatorState().stream())
                                .filter(Objects::nonNull)
                                .flatMap(
                                        operatorStateHandle ->
                                                operatorStateHandle.getStateNameToPartitionOffsets()
                                                        .values().stream())
                                // 找到任一模式为union的
                                .anyMatch(
                                        stateMetaInfo ->
                                                stateMetaInfo.getDistributionMode()
                                                        == OperatorStateHandle.Mode.UNION);

                if (hasUnionListState) {
                    return true;
                }
            }
        }

        return false;
    }

    private void fulfillFullyFinishedOrFinishedOnRestoreOperatorStates(
            Map<OperatorID, OperatorState> operatorStates) {
        // Completes the operator state for the fully finished operators
        for (ExecutionJobVertex jobVertex : fullyFinishedOrFinishedOnRestoreVertices.values()) {
            for (OperatorIDPair operatorID : jobVertex.getOperatorIDs()) {

                // 获取相关的状态
                OperatorState operatorState =
                        operatorStates.get(operatorID.getGeneratedOperatorID());
                checkState(
                        operatorState == null || !operatorState.hasSubtaskStates(),
                        "There should be no states or only coordinator state reported for fully finished operators");

                // 将operatorStates内的状态更新成fullyFinished
                operatorState =
                        new FullyFinishedOperatorState(
                                operatorID.getGeneratedOperatorID(),
                                jobVertex.getParallelism(),
                                jobVertex.getMaxParallelism());
                operatorStates.put(operatorID.getGeneratedOperatorID(), operatorState);
            }
        }
    }

    private void fulfillSubtaskStateForPartiallyFinishedOperators(
            Map<OperatorID, OperatorState> operatorStates) {
        for (Execution finishedTask : finishedTasks) {
            ExecutionJobVertex jobVertex = finishedTask.getVertex().getJobVertex();
            for (OperatorIDPair operatorIDPair : jobVertex.getOperatorIDs()) {
                // 从operatorStates中找到对应的状态
                OperatorState operatorState =
                        operatorStates.get(operatorIDPair.getGeneratedOperatorID());

                // 该状态已经完成 就不需要处理了
                if (operatorState != null && operatorState.isFullyFinished()) {
                    continue;
                }

                if (operatorState == null) {
                    operatorState =
                            new OperatorState(
                                    operatorIDPair.getGeneratedOperatorID(),
                                    jobVertex.getParallelism(),
                                    jobVertex.getMaxParallelism());
                    // 初始化状态 并加入容器
                    operatorStates.put(operatorIDPair.getGeneratedOperatorID(), operatorState);
                }

                // 因为本task已经处于finished状态了 所以加入operatorState的是一个FinishedOperatorSubtaskState
                operatorState.putState(
                        finishedTask.getParallelSubtaskIndex(),
                        FinishedOperatorSubtaskState.INSTANCE);
            }
        }
    }
}
