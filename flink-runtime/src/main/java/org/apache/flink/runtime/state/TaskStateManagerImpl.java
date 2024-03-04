/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReaderImpl;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageView;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * This class is the default implementation of {@link TaskStateManager} and collaborates with the
 * job manager through {@link CheckpointResponder}) as well as a task-manager-local state store.
 * Like this, client code does not have to deal with the differences between remote or local state
 * on recovery because this class handles both cases transparently.
 *
 * <p>Reported state is tagged by clients so that this class can properly forward to the right
 * receiver for the checkpointed state.
 *
 * 对应某个subtask状态
 */
public class TaskStateManagerImpl implements TaskStateManager {

    /** The logger for this class. */
    private static final Logger LOG = LoggerFactory.getLogger(TaskStateManagerImpl.class);

    /** The id of the job for which this manager was created, can report, and recover.
     * 所属的job
     * */
    private final JobID jobId;

    /** The execution attempt id that this manager reports for. */
    private final ExecutionAttemptID executionAttemptID;

    /**
     * The data given by the job manager to restore the job. This is null for a new job without
     * previous state.
     * 内部包含该subtask某次检查点的快照数据
     */
    @Nullable private final JobManagerTaskRestore jobManagerTaskRestore;

    /** The local state store to which this manager reports local state snapshots.
     * 存储本地状态的容器
     * */
    private final TaskLocalStateStore localStateStore;

    /** The changelog storage where the manager reads and writes the changelog
     * 存储状态的变更数据
     * */
    @Nullable private final StateChangelogStorage<?> stateChangelogStorage;

    /**
     * 维护每个job的changelog
     */
    private final TaskExecutorStateChangelogStoragesManager changelogStoragesManager;

    /** The checkpoint responder through which this manager can report to the job manager.
     * 用于与远端的总控对象交互
     * */
    private final CheckpointResponder checkpointResponder;

    /**
     * 用于读取数据
     */
    private final SequentialChannelStateReader sequentialChannelStateReader;

    public TaskStateManagerImpl(
            @Nonnull JobID jobId,
            @Nonnull ExecutionAttemptID executionAttemptID,
            @Nonnull TaskLocalStateStore localStateStore,
            @Nullable StateChangelogStorage<?> stateChangelogStorage,
            @Nonnull TaskExecutorStateChangelogStoragesManager changelogStoragesManager,
            @Nullable JobManagerTaskRestore jobManagerTaskRestore,
            @Nonnull CheckpointResponder checkpointResponder) {
        this(
                jobId,
                executionAttemptID,
                localStateStore,
                stateChangelogStorage,
                changelogStoragesManager,
                jobManagerTaskRestore,
                checkpointResponder,
                new SequentialChannelStateReaderImpl(
                        jobManagerTaskRestore == null
                                ? new TaskStateSnapshot()
                                : jobManagerTaskRestore.getTaskStateSnapshot()));
    }

    public TaskStateManagerImpl(
            @Nonnull JobID jobId,
            @Nonnull ExecutionAttemptID executionAttemptID,
            @Nonnull TaskLocalStateStore localStateStore,
            @Nullable StateChangelogStorage<?> stateChangelogStorage,
            @Nonnull TaskExecutorStateChangelogStoragesManager changelogStoragesManager,
            @Nullable JobManagerTaskRestore jobManagerTaskRestore,
            @Nonnull CheckpointResponder checkpointResponder,
            @Nonnull SequentialChannelStateReaderImpl sequentialChannelStateReader) {
        this.jobId = jobId;
        this.localStateStore = localStateStore;
        this.stateChangelogStorage = stateChangelogStorage;
        this.changelogStoragesManager = changelogStoragesManager;
        this.jobManagerTaskRestore = jobManagerTaskRestore;
        this.executionAttemptID = executionAttemptID;
        this.checkpointResponder = checkpointResponder;
        this.sequentialChannelStateReader = sequentialChannelStateReader;
    }

    /**
     * 报告某个任务快照已经完成
     * @param checkpointMetaData meta data from the checkpoint request.
     * @param checkpointMetrics task level metrics for the checkpoint.
     * @param acknowledgedState the reported states to acknowledge to the job manager.  表示上报给jobManager的状态
     * @param localState the reported states for local recovery.  本地的状态
     */
    @Override
    public void reportTaskStateSnapshots(
            @Nonnull CheckpointMetaData checkpointMetaData,
            @Nonnull CheckpointMetrics checkpointMetrics,
            @Nullable TaskStateSnapshot acknowledgedState,
            @Nullable TaskStateSnapshot localState) {

        long checkpointId = checkpointMetaData.getCheckpointId();

        // 存储快照数据
        localStateStore.storeLocalState(checkpointId, localState);

        // 通知到远端
        checkpointResponder.acknowledgeCheckpoint(
                jobId, executionAttemptID, checkpointId, checkpointMetrics, acknowledgedState);
    }

    @Override
    public void reportIncompleteTaskStateSnapshots(
            CheckpointMetaData checkpointMetaData, CheckpointMetrics checkpointMetrics) {

        checkpointResponder.reportCheckpointMetrics(
                jobId, executionAttemptID, checkpointMetaData.getCheckpointId(), checkpointMetrics);
    }

    /**
     * 获取描述信息
     * @return
     */
    @Override
    public InflightDataRescalingDescriptor getInputRescalingDescriptor() {
        if (jobManagerTaskRestore == null) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }
        return jobManagerTaskRestore.getTaskStateSnapshot().getInputRescalingDescriptor();
    }

    @Override
    public InflightDataRescalingDescriptor getOutputRescalingDescriptor() {
        if (jobManagerTaskRestore == null) {
            return InflightDataRescalingDescriptor.NO_RESCALE;
        }
        return jobManagerTaskRestore.getTaskStateSnapshot().getOutputRescalingDescriptor();
    }

    public boolean isTaskDeployedAsFinished() {
        if (jobManagerTaskRestore == null) {
            return false;
        }

        return jobManagerTaskRestore.getTaskStateSnapshot().isTaskDeployedAsFinished();
    }

    @Override
    public Optional<Long> getRestoreCheckpointId() {
        if (jobManagerTaskRestore == null) {
            // This happens only if no checkpoint to restore.
            return Optional.empty();
        }

        return Optional.of(jobManagerTaskRestore.getRestoreCheckpointId());
    }

    /**
     * 仅保留指定的operator状态
     * @param operatorID the id of the operator for which we request state.
     * @return
     */
    @Override
    public PrioritizedOperatorSubtaskState prioritizedOperatorState(OperatorID operatorID) {

        if (jobManagerTaskRestore == null) {
            return PrioritizedOperatorSubtaskState.emptyNotRestored();
        }

        TaskStateSnapshot jobManagerStateSnapshot = jobManagerTaskRestore.getTaskStateSnapshot();

        OperatorSubtaskState jobManagerSubtaskState =
                jobManagerStateSnapshot.getSubtaskStateByOperatorID(operatorID);

        if (jobManagerSubtaskState == null) {
            return PrioritizedOperatorSubtaskState.empty(
                    jobManagerTaskRestore.getRestoreCheckpointId());
        }

        long restoreCheckpointId = jobManagerTaskRestore.getRestoreCheckpointId();

        // 找到之前的快照
        TaskStateSnapshot localStateSnapshot =
                localStateStore.retrieveLocalState(restoreCheckpointId);

        // 删除其他检查点
        localStateStore.pruneMatchingCheckpoints(
                (long checkpointId) -> checkpointId != restoreCheckpointId);

        List<OperatorSubtaskState> alternativesByPriority = Collections.emptyList();

        if (localStateSnapshot != null) {
            OperatorSubtaskState localSubtaskState =
                    localStateSnapshot.getSubtaskStateByOperatorID(operatorID);

            if (localSubtaskState != null) {
                alternativesByPriority = Collections.singletonList(localSubtaskState);
            }
        }

        LOG.debug(
                "Operator {} has remote state {} from job manager and local state alternatives {} from local "
                        + "state store {}.",
                operatorID,
                jobManagerSubtaskState,
                alternativesByPriority,
                localStateStore);

        // 只保留该operatorSubtaskState
        PrioritizedOperatorSubtaskState.Builder builder =
                new PrioritizedOperatorSubtaskState.Builder(
                        jobManagerSubtaskState,
                        alternativesByPriority,
                        jobManagerTaskRestore.getRestoreCheckpointId());

        return builder.build();
    }

    @Nonnull
    @Override
    public LocalRecoveryConfig createLocalRecoveryConfig() {
        return localStateStore.getLocalRecoveryConfig();
    }

    @Override
    public SequentialChannelStateReader getSequentialChannelStateReader() {
        return sequentialChannelStateReader;
    }

    @Nullable
    @Override
    public StateChangelogStorage<?> getStateChangelogStorage() {
        return stateChangelogStorage;
    }

    @Nullable
    @Override
    public StateChangelogStorageView<?> getStateChangelogStorageView(
            Configuration configuration, ChangelogStateHandle changelogStateHandle) {
        StateChangelogStorageView<?> storageView = null;
        try {
            storageView =
                    changelogStoragesManager.stateChangelogStorageViewForJob(
                            jobId, configuration, changelogStateHandle);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        return storageView;
    }

    /** Tracking when local state can be confirmed and disposed. */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        localStateStore.confirmCheckpoint(checkpointId);
    }

    /** Tracking when some local state can be disposed. */
    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        localStateStore.abortCheckpoint(checkpointId);
    }

    @Override
    public void close() throws Exception {
        sequentialChannelStateReader.close();
    }
}
