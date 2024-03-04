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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor.MappingType;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.CollectionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Used by {@link StateAssignmentOperation} to store temporal information while creating {@link
 * OperatorSubtaskState}.
 * 表示任务的分配信息
 * TODO
 */
class TaskStateAssignment {
    private static final Logger LOG = LoggerFactory.getLogger(TaskStateAssignment.class);

    /**
     * 看作一个job
     */
    final ExecutionJobVertex executionJobVertex;

    /**
     * 维护各个算子的状态  每个OperatorState下有多个子任务
     */
    final Map<OperatorID, OperatorState> oldState;
    final boolean hasNonFinishedState;
    final boolean isFullyFinished;
    final boolean hasInputState;
    final boolean hasOutputState;
    final int newParallelism;
    final OperatorID inputOperatorID;
    final OperatorID outputOperatorID;

    /**
     * OperatorInstanceID 对应到某个子任务实例  operatorId + subtask
     * List<OperatorStateHandle> 表示相关的一组状态     分为4类状态
     */
    final Map<OperatorInstanceID, List<OperatorStateHandle>> subManagedOperatorState;
    final Map<OperatorInstanceID, List<OperatorStateHandle>> subRawOperatorState;
    final Map<OperatorInstanceID, List<KeyedStateHandle>> subManagedKeyedState;
    final Map<OperatorInstanceID, List<KeyedStateHandle>> subRawKeyedState;

    // 对应输入输出状态
    final Map<OperatorInstanceID, List<InputChannelStateHandle>> inputChannelStates;
    final Map<OperatorInstanceID, List<ResultSubpartitionStateHandle>> resultSubpartitionStates;

    // 子任务会关联重映射对象

    /** The subtask mapping when the output operator was rescaled. */
    private final Map<Integer, SubtasksRescaleMapping> outputSubtaskMappings = new HashMap<>();
    /** The subtask mapping when the input operator was rescaled. */
    private final Map<Integer, SubtasksRescaleMapping> inputSubtaskMappings = new HashMap<>();

    // 关联的上下游
    @Nullable private TaskStateAssignment[] downstreamAssignments;
    @Nullable private TaskStateAssignment[] upstreamAssignments;

    // 描述是否有上游/下游
    @Nullable private Boolean hasUpstreamOutputStates;
    @Nullable private Boolean hasDownstreamInputStates;


    private final Map<IntermediateDataSetID, TaskStateAssignment> consumerAssignment;
    private final Map<ExecutionJobVertex, TaskStateAssignment> vertexAssignments;

    /**
     * @param executionJobVertex
     * @param oldState
     * @param consumerAssignment  对应downstream
     * @param vertexAssignments   对应upstream
     */
    public TaskStateAssignment(
            ExecutionJobVertex executionJobVertex,
            Map<OperatorID, OperatorState> oldState,
            Map<IntermediateDataSetID, TaskStateAssignment> consumerAssignment,
            Map<ExecutionJobVertex, TaskStateAssignment> vertexAssignments) {

        this.executionJobVertex = executionJobVertex;
        this.oldState = oldState;

        // 只要有子任务数量 > 0 代表包含未完成
        // FullyFinishedOperatorState 中数量就是0
        this.hasNonFinishedState =
                oldState.values().stream()
                        .anyMatch(operatorState -> operatorState.getNumberCollectedStates() > 0);
        // 要求所有状态的所有子任务 都完成了
        this.isFullyFinished = oldState.values().stream().anyMatch(OperatorState::isFullyFinished);
        if (isFullyFinished) {
            checkState(
                    oldState.values().stream().allMatch(OperatorState::isFullyFinished),
                    "JobVertex could not have mixed finished and unfinished operators");
        }

        newParallelism = executionJobVertex.getParallelism();
        this.consumerAssignment = checkNotNull(consumerAssignment);
        this.vertexAssignments = checkNotNull(vertexAssignments);

        // 预期出现的子任务总数
        final int expectedNumberOfSubtasks = newParallelism * oldState.size();

        subManagedOperatorState =
                CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        subRawOperatorState = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        inputChannelStates = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        resultSubpartitionStates =
                CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        subManagedKeyedState = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);
        subRawKeyedState = CollectionUtil.newHashMapWithExpectedSize(expectedNumberOfSubtasks);

        final List<OperatorIDPair> operatorIDs = executionJobVertex.getOperatorIDs();
        // 第一个id是输出id  最后一个是输入id
        outputOperatorID = operatorIDs.get(0).getGeneratedOperatorID();
        inputOperatorID = operatorIDs.get(operatorIDs.size() - 1).getGeneratedOperatorID();

        // 根据子状态是否有输入/输出 设置标识
        hasInputState =
                oldState.get(inputOperatorID).getStates().stream()
                        .anyMatch(subState -> !subState.getInputChannelState().isEmpty());
        hasOutputState =
                oldState.get(outputOperatorID).getStates().stream()
                        .anyMatch(subState -> !subState.getResultSubpartitionState().isEmpty());
    }

    public TaskStateAssignment[] getDownstreamAssignments() {
        if (downstreamAssignments == null) {
            downstreamAssignments =
                    Arrays.stream(executionJobVertex.getProducedDataSets())
                            .map(result -> consumerAssignment.get(result.getId())) // consumerAssignment是作为入参传入的
                            .toArray(TaskStateAssignment[]::new);
        }
        return downstreamAssignments;
    }

    private static int getAssignmentIndex(
            TaskStateAssignment[] assignments, TaskStateAssignment assignment) {
        return Arrays.asList(assignments).indexOf(assignment);
    }

    public TaskStateAssignment[] getUpstreamAssignments() {
        if (upstreamAssignments == null) {
            upstreamAssignments =
                    executionJobVertex.getInputs().stream()
                            .map(result -> vertexAssignments.get(result.getProducer()))  // 跟getDownstreamAssignments类似
                            .toArray(TaskStateAssignment[]::new);
        }
        return upstreamAssignments;
    }

    /**
     * 查询子状态
     * @param instanceID
     * @return
     */
    public OperatorSubtaskState getSubtaskState(OperatorInstanceID instanceID) {
        checkState(
                subManagedKeyedState.containsKey(instanceID)
                        || !subRawKeyedState.containsKey(instanceID),
                "If an operator has no managed key state, it should also not have a raw keyed state.");

        final StateObjectCollection<InputChannelStateHandle> inputState =
                getState(instanceID, inputChannelStates);
        final StateObjectCollection<ResultSubpartitionStateHandle> outputState =
                getState(instanceID, resultSubpartitionStates);

        // 按照实例id 从各容器加载数据 并产生OperatorSubtaskState
        return OperatorSubtaskState.builder()
                .setManagedOperatorState(getState(instanceID, subManagedOperatorState))
                .setRawOperatorState(getState(instanceID, subRawOperatorState))
                .setManagedKeyedState(getState(instanceID, subManagedKeyedState))
                .setRawKeyedState(getState(instanceID, subRawKeyedState))
                .setInputChannelState(inputState)
                .setResultSubpartitionState(outputState)
                // 设置输入和输出的重调节描述信息
                .setInputRescalingDescriptor(
                        // TODO
                        createRescalingDescriptor(
                                instanceID,
                                inputOperatorID,
                                getUpstreamAssignments(),
                                (assignment, recompute) -> {
                                    // 获取本对象在下游assignment的下标
                                    int assignmentIndex =
                                            getAssignmentIndex(
                                                    assignment.getDownstreamAssignments(), this);
                                    return assignment.getOutputMapping(assignmentIndex, recompute);
                                },
                                inputSubtaskMappings,
                                this::getInputMapping))
                .setOutputRescalingDescriptor(
                        createRescalingDescriptor(
                                instanceID,
                                outputOperatorID,
                                getDownstreamAssignments(),
                                (assignment, recompute) -> {
                                    int assignmentIndex =
                                            getAssignmentIndex(
                                                    assignment.getUpstreamAssignments(), this);
                                    return assignment.getInputMapping(assignmentIndex, recompute);
                                },
                                outputSubtaskMappings,
                                this::getOutputMapping))
                .build();
    }

    public boolean hasUpstreamOutputStates() {
        if (hasUpstreamOutputStates == null) {
            hasUpstreamOutputStates =
                    Arrays.stream(getUpstreamAssignments())
                            .anyMatch(assignment -> assignment.hasOutputState);
        }
        return hasUpstreamOutputStates;
    }

    public boolean hasDownstreamInputStates() {
        if (hasDownstreamInputStates == null) {
            hasDownstreamInputStates =
                    Arrays.stream(getDownstreamAssignments())
                            .anyMatch(assignment -> assignment.hasInputState);
        }
        return hasDownstreamInputStates;
    }

    private InflightDataGateOrPartitionRescalingDescriptor log(
            InflightDataGateOrPartitionRescalingDescriptor descriptor, int subtask, int partition) {
        LOG.debug(
                "created {} for task={} subtask={} partition={}",
                descriptor,
                executionJobVertex.getName(),
                subtask,
                partition);
        return descriptor;
    }

    private InflightDataRescalingDescriptor log(
            InflightDataRescalingDescriptor descriptor, int subtask) {
        LOG.debug(
                "created {} for task={} subtask={}",
                descriptor,
                executionJobVertex.getName(),
                subtask);
        return descriptor;
    }

    /**
     * 产生重调节的描述信息
     * @param instanceID
     * @param expectedOperatorID
     * @param connectedAssignments
     * @param mappingRetriever
     * @param subtaskGateOrPartitionMappings
     * @param subtaskMappingCalculator
     * @return
     */
    private InflightDataRescalingDescriptor createRescalingDescriptor(
            OperatorInstanceID instanceID,
            OperatorID expectedOperatorID,
            TaskStateAssignment[] connectedAssignments,
            BiFunction<TaskStateAssignment, Boolean, SubtasksRescaleMapping> mappingRetriever,
            Map<Integer, SubtasksRescaleMapping> subtaskGateOrPartitionMappings,
            Function<Integer, SubtasksRescaleMapping> subtaskMappingCalculator) {

        // id不匹配
        if (!expectedOperatorID.equals(instanceID.getOperatorId())) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }

        // 这里会找到一个匹配的重映射对象
        SubtasksRescaleMapping[] rescaledChannelsMappings =
                Arrays.stream(connectedAssignments)
                        .map(assignment -> mappingRetriever.apply(assignment, false))
                        .toArray(SubtasksRescaleMapping[]::new);

        // no state on input and output, especially for any aligned checkpoint
        if (subtaskGateOrPartitionMappings.isEmpty()
                && Arrays.stream(rescaledChannelsMappings).allMatch(Objects::isNull)) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }

        // 产生描述对象
        InflightDataGateOrPartitionRescalingDescriptor[] gateOrPartitionDescriptors =
                createGateOrPartitionRescalingDescriptors(
                        instanceID,
                        connectedAssignments,
                        assignment -> mappingRetriever.apply(assignment, true),
                        subtaskGateOrPartitionMappings,
                        subtaskMappingCalculator,
                        rescaledChannelsMappings);

        if (Arrays.stream(gateOrPartitionDescriptors)
                .allMatch(InflightDataGateOrPartitionRescalingDescriptor::isIdentity)) {
            return log(InflightDataRescalingDescriptor.NO_RESCALE, instanceID.getSubtaskId());
        } else {
            return log(
                    new InflightDataRescalingDescriptor(gateOrPartitionDescriptors),
                    instanceID.getSubtaskId());
        }
    }

    private InflightDataGateOrPartitionRescalingDescriptor[]
            createGateOrPartitionRescalingDescriptors(
                    OperatorInstanceID instanceID,
                    TaskStateAssignment[] connectedAssignments,
                    Function<TaskStateAssignment, SubtasksRescaleMapping> mappingCalculator,
                    Map<Integer, SubtasksRescaleMapping> subtaskGateOrPartitionMappings,
                    Function<Integer, SubtasksRescaleMapping> subtaskMappingCalculator,
                    SubtasksRescaleMapping[] rescaledChannelsMappings) {
        return IntStream.range(0, rescaledChannelsMappings.length)
                .mapToObj(
                        partition -> {
                            TaskStateAssignment connectedAssignment =
                                    connectedAssignments[partition];
                            SubtasksRescaleMapping rescaleMapping =
                                    Optional.ofNullable(rescaledChannelsMappings[partition])
                                            .orElseGet(
                                                    () ->
                                                            mappingCalculator.apply(
                                                                    connectedAssignment));
                            SubtasksRescaleMapping subtaskMapping =
                                    Optional.ofNullable(
                                                    subtaskGateOrPartitionMappings.get(partition))
                                            .orElseGet(
                                                    () ->
                                                            subtaskMappingCalculator.apply(
                                                                    partition));
                            return getInflightDataGateOrPartitionRescalingDescriptor(
                                    instanceID, partition, rescaleMapping, subtaskMapping);
                        })
                .toArray(InflightDataGateOrPartitionRescalingDescriptor[]::new);
    }

    private InflightDataGateOrPartitionRescalingDescriptor
            getInflightDataGateOrPartitionRescalingDescriptor(
                    OperatorInstanceID instanceID,
                    int partition,
                    SubtasksRescaleMapping rescaleMapping,
                    SubtasksRescaleMapping subtaskMapping) {

        int[] oldSubtaskInstances =
                subtaskMapping.rescaleMappings.getMappedIndexes(instanceID.getSubtaskId());

        // no scaling or simple scale-up without the need of virtual
        // channels.
        boolean isIdentity =
                (subtaskMapping.rescaleMappings.isIdentity()
                                && rescaleMapping.getRescaleMappings().isIdentity())
                        || oldSubtaskInstances.length == 0;

        final Set<Integer> ambiguousSubtasks =
                subtaskMapping.mayHaveAmbiguousSubtasks
                        ? subtaskMapping.rescaleMappings.getAmbiguousTargets()
                        : emptySet();
        return log(
                new InflightDataGateOrPartitionRescalingDescriptor(
                        oldSubtaskInstances,
                        rescaleMapping.getRescaleMappings(),
                        ambiguousSubtasks,
                        isIdentity ? MappingType.IDENTITY : MappingType.RESCALING),
                instanceID.getSubtaskId(),
                partition);
    }

    private <T extends StateObject> StateObjectCollection<T> getState(
            OperatorInstanceID instanceID,
            Map<OperatorInstanceID, List<T>> subManagedOperatorState) {
        List<T> value = subManagedOperatorState.get(instanceID);
        return value != null ? new StateObjectCollection<>(value) : StateObjectCollection.empty();
    }

    /**
     *
     * @param assignmentIndex  这是本对象的下标
     * @param recompute
     * @return
     */
    private SubtasksRescaleMapping getOutputMapping(int assignmentIndex, boolean recompute) {
        SubtasksRescaleMapping mapping = outputSubtaskMappings.get(assignmentIndex);
        if (recompute && mapping == null) {
            return getOutputMapping(assignmentIndex);
        } else {
            return mapping;
        }
    }

    private SubtasksRescaleMapping getInputMapping(int assignmentIndex, boolean recompute) {
        SubtasksRescaleMapping mapping = inputSubtaskMappings.get(assignmentIndex);
        if (recompute && mapping == null) {
            return getInputMapping(assignmentIndex);
        } else {
            return mapping;
        }
    }

    public SubtasksRescaleMapping getOutputMapping(int partitionIndex) {
        final TaskStateAssignment downstreamAssignment = getDownstreamAssignments()[partitionIndex];
        final IntermediateResult output = executionJobVertex.getProducedDataSets()[partitionIndex];
        final int gateIndex = downstreamAssignment.executionJobVertex.getInputs().indexOf(output);

        final SubtaskStateMapper mapper =
                checkNotNull(
                        downstreamAssignment
                                .executionJobVertex
                                .getJobVertex()
                                .getInputs()
                                .get(gateIndex)
                                .getUpstreamSubtaskStateMapper(),
                        "No channel rescaler found during rescaling of channel state");
        final RescaleMappings mapping =
                mapper.getNewToOldSubtasksMapping(
                        oldState.get(outputOperatorID).getParallelism(), newParallelism);
        return outputSubtaskMappings.compute(
                partitionIndex,
                (idx, oldMapping) ->
                        checkSubtaskMapping(oldMapping, mapping, mapper.isAmbiguous()));
    }

    public SubtasksRescaleMapping getInputMapping(int gateIndex) {
        final SubtaskStateMapper mapper =
                checkNotNull(
                        executionJobVertex
                                .getJobVertex()
                                .getInputs()
                                .get(gateIndex)
                                .getDownstreamSubtaskStateMapper(),
                        "No channel rescaler found during rescaling of channel state");
        final RescaleMappings mapping =
                mapper.getNewToOldSubtasksMapping(
                        oldState.get(inputOperatorID).getParallelism(), newParallelism);

        return inputSubtaskMappings.compute(
                gateIndex,
                (idx, oldMapping) ->
                        checkSubtaskMapping(oldMapping, mapping, mapper.isAmbiguous()));
    }

    @Override
    public String toString() {
        return "TaskStateAssignment for " + executionJobVertex.getName();
    }

    private static @Nonnull SubtasksRescaleMapping checkSubtaskMapping(
            @Nullable SubtasksRescaleMapping oldMapping,
            RescaleMappings mapping,
            boolean mayHaveAmbiguousSubtasks) {
        if (oldMapping == null) {
            return new SubtasksRescaleMapping(mapping, mayHaveAmbiguousSubtasks);
        }
        if (!oldMapping.rescaleMappings.equals(mapping)) {
            throw new IllegalStateException(
                    "Incompatible subtask mappings: are multiple operators "
                            + "ingesting/producing intermediate results with varying degrees of parallelism?"
                            + "Found "
                            + oldMapping
                            + " and "
                            + mapping
                            + ".");
        }
        return new SubtasksRescaleMapping(
                mapping, oldMapping.mayHaveAmbiguousSubtasks || mayHaveAmbiguousSubtasks);
    }

    /**
     * 该对象对分区进行重映射
     */
    static class SubtasksRescaleMapping {
        private final RescaleMappings rescaleMappings;
        /**
         * If channel data cannot be safely divided into subtasks (several new subtask indexes are
         * associated with the same old subtask index). Mostly used for range partitioners.
         */
        private final boolean mayHaveAmbiguousSubtasks;

        private SubtasksRescaleMapping(
                RescaleMappings rescaleMappings, boolean mayHaveAmbiguousSubtasks) {
            this.rescaleMappings = rescaleMappings;
            this.mayHaveAmbiguousSubtasks = mayHaveAmbiguousSubtasks;
        }

        public RescaleMappings getRescaleMappings() {
            return rescaleMappings;
        }

        public boolean isMayHaveAmbiguousSubtasks() {
            return mayHaveAmbiguousSubtasks;
        }
    }
}
