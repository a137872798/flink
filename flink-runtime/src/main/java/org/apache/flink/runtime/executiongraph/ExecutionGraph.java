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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.scheduler.InternalFailuresListener;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.util.OptionalFailure;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The execution graph is the central data structure that coordinates the distributed execution of a
 * data flow. It keeps representations of each parallel task, each intermediate stream, and the
 * communication between them.
 *
 * <p>The execution graph consists of the following constructs:
 *
 * <ul>
 *   <li>The {@link ExecutionJobVertex} represents one vertex from the JobGraph (usually one
 *       operation like "map" or "join") during execution. It holds the aggregated state of all
 *       parallel subtasks. The ExecutionJobVertex is identified inside the graph by the {@link
 *       JobVertexID}, which it takes from the JobGraph's corresponding JobVertex.
 *   <li>The {@link ExecutionVertex} represents one parallel subtask. For each ExecutionJobVertex,
 *       there are as many ExecutionVertices as the parallelism. The ExecutionVertex is identified
 *       by the ExecutionJobVertex and the index of the parallel subtask
 *   <li>The {@link Execution} is one attempt to execute a ExecutionVertex. There may be multiple
 *       Executions for the ExecutionVertex, in case of a failure, or in the case where some data
 *       needs to be recomputed because it is no longer available when requested by later
 *       operations. An Execution is always identified by an {@link ExecutionAttemptID}. All
 *       messages between the JobManager and the TaskManager about deployment of tasks and updates
 *       in the task status always use the ExecutionAttemptID to address the message receiver.
 * </ul>
 *
 * AccessExecutionGraph 提供了一些访问性质的接口
 */
public interface ExecutionGraph extends AccessExecutionGraph {

    /**
     * 使用指定的执行器启动图对象
     * @param jobMasterMainThreadExecutor
     */
    void start(@Nonnull ComponentMainThreadExecutor jobMasterMainThreadExecutor);

    /**
     * 获取拓扑信息
     * @return
     */
    SchedulingTopology getSchedulingTopology();

    /**
     * 传入检查点相关的一系列组件 并开启检查点
     * @param chkConfig
     * @param masterHooks
     * @param checkpointIDCounter
     * @param checkpointStore
     * @param checkpointStateBackend
     * @param checkpointStorage
     * @param statsTracker
     * @param checkpointsCleaner
     * @param changelogStorage
     */
    void enableCheckpointing(
            CheckpointCoordinatorConfiguration chkConfig,
            List<MasterTriggerRestoreHook<?>> masterHooks,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore checkpointStore,
            StateBackend checkpointStateBackend,
            CheckpointStorage checkpointStorage,
            CheckpointStatsTracker statsTracker,
            CheckpointsCleaner checkpointsCleaner,
            String changelogStorage);

    /**
     * 可以从执行图中拿到 检查点协调者
     * @return
     */
    @Nullable
    CheckpointCoordinator getCheckpointCoordinator();

    /**
     * 借助该对象可以查询kvState的位置
     * @return
     */
    KvStateLocationRegistry getKvStateLocationRegistry();

    /**
     * 设置计划信息
     * @param jsonPlan
     */
    void setJsonPlan(String jsonPlan);

    Configuration getJobConfiguration();

    /**
     * 获取执行失败的错误信息
     * @return
     */
    Throwable getFailureCause();

    /**
     * graph 对应的其实就是拓扑图  这里获取可拓扑的所有顶点 (job级别)
     * @return
     */
    @Override
    Iterable<ExecutionJobVertex> getVerticesTopologically();

    /**
     * 获取更细粒度的顶点 subtask级别
     * @return
     */
    @Override
    Iterable<ExecutionVertex> getAllExecutionVertices();

    @Override
    ExecutionJobVertex getJobVertex(JobVertexID id);

    @Override
    Map<JobVertexID, ExecutionJobVertex> getAllVertices();

    /**
     * Gets the number of restarts, including full restarts and fine grained restarts. If a recovery
     * is currently pending, this recovery is included in the count.
     *
     * @return The number of restarts so far
     *
     * 获取重试次数
     */
    long getNumberOfRestarts();

    /**
     * 获取产生的所有中间结果集
     * @return
     */
    Map<IntermediateDataSetID, IntermediateResult> getAllIntermediateResults();

    /**
     * Gets the intermediate result partition by the given partition ID, or throw an exception if
     * the partition is not found.
     *
     * @param id of the intermediate result partition
     * @return intermediate result partition
     *
     * 通过id查询被分区的中间结果集
     */
    IntermediateResultPartition getResultPartitionOrThrow(final IntermediateResultPartitionID id);

    /**
     * Merges all accumulator results from the tasks previously executed in the Executions.
     *
     * @return The accumulator map
     *
     * 获取所有累加值
     */
    Map<String, OptionalFailure<Accumulator<?, ?>>> aggregateUserAccumulators();

    /**
     * Updates the accumulators during the runtime of a job. Final accumulator results are
     * transferred through the UpdateTaskExecutionState message.
     *
     * @param accumulatorSnapshot The serialized flink and user-defined accumulators
     *                            使用快照中保留的累加值更新图的累加值
     */
    void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot);


    /**
     * 设置监听器  捕获本地异常和全局异常
     * @param internalTaskFailuresListener
     */
    void setInternalTaskFailuresListener(InternalFailuresListener internalTaskFailuresListener);

    /**
     * 将一组顶点加入到graph中
     * @param topologicallySorted
     * @param jobManagerJobMetricGroup
     * @throws JobException
     */
    void attachJobGraph(
            List<JobVertex> topologicallySorted, JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobException;

    /**
     * 运行graph
     */
    void transitionToRunning();

    /**
     * 取消运行
     */
    void cancel();

    /**
     * Suspends the current ExecutionGraph.
     *
     * <p>The JobStatus will be directly set to {@link JobStatus#SUSPENDED} iff the current state is
     * not a terminal state. All ExecutionJobVertices will be canceled and the onTerminalState() is
     * executed.
     *
     * <p>The {@link JobStatus#SUSPENDED} state is a local terminal state which stops the execution
     * of the job but does not remove the job from the HA job store so that it can be recovered by
     * another JobManager.
     *
     * @param suspensionCause Cause of the suspension
     *                        暂停运行
     */
    void suspend(Throwable suspensionCause);

    /**
     * 通知job执行失败
     * @param cause
     * @param timestamp
     */
    void failJob(Throwable cause, long timestamp);

    /**
     * Returns the termination future of this {@link ExecutionGraph}. The termination future is
     * completed with the terminal {@link JobStatus} once the ExecutionGraph reaches this terminal
     * state and all {@link Execution} have been terminated.
     *
     * @return Termination future of this {@link ExecutionGraph}.
     */
    CompletableFuture<JobStatus> getTerminationFuture();

    /**
     * 等待 直到进入终止状态
     * @return
     * @throws InterruptedException
     */
    @VisibleForTesting
    JobStatus waitUntilTerminal() throws InterruptedException;

    boolean transitionState(JobStatus current, JobStatus newState);

    /**
     * 增加重试次数
     */
    void incrementRestarts();

    void initFailureCause(Throwable t, long timestamp);

    /**
     * Updates the state of one of the ExecutionVertex's Execution attempts. If the new status if
     * "FINISHED", this also updates the accumulators.
     *
     * @param state The state update.
     * @return True, if the task update was properly applied, false, if the execution attempt was
     *     not found.
     *     更新的信息包含在TaskExecutionStateTransition中
     */
    boolean updateState(TaskExecutionStateTransition state);

    /**
     * 获取属于该graph的所有execution
     * @return
     */
    Map<ExecutionAttemptID, Execution> getRegisteredExecutions();

    void registerJobStatusListener(JobStatusListener listener);

    ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker();

    /**
     * 返回结束的顶点
     * @return
     */
    int getNumFinishedVertices();

    @Nonnull
    ComponentMainThreadExecutor getJobMasterMainThreadExecutor();

    /**
     * 初始化某个task
     * @param ejv
     * @param createTimestamp
     * @param jobManagerJobMetricGroup
     * @throws JobException
     */
    default void initializeJobVertex(
            ExecutionJobVertex ejv,
            long createTimestamp,
            JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobException {
        initializeJobVertex(
                ejv,
                createTimestamp,
                VertexInputInfoComputationUtils.computeVertexInputInfos(
                        ejv, getAllIntermediateResults()::get),
                jobManagerJobMetricGroup);
    }

    /**
     * Initialize the given execution job vertex, mainly includes creating execution vertices
     * according to the parallelism, and connecting to the predecessors.
     *
     * @param ejv The execution job vertex that needs to be initialized.
     * @param createTimestamp The timestamp for creating execution vertices, used to initialize the
     *     first Execution with.
     * @param jobVertexInputInfos The input infos of this job vertex.
     *                            根据这些信息初始化顶点
     */
    void initializeJobVertex(
            ExecutionJobVertex ejv,
            long createTimestamp,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos,
            JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobException;

    /**
     * Notify that some job vertices have been newly initialized, execution graph will try to update
     * scheduling topology.
     *
     * @param vertices The execution job vertices that are newly initialized.
     *                 通知有顶点更新了
     */
    void notifyNewlyInitializedJobVertices(List<ExecutionJobVertex> vertices);

    /**
     * 获取顶点信息 以及重试次数
     * @param attemptId
     * @return
     */
    Optional<String> findVertexWithAttempt(final ExecutionAttemptID attemptId);

    /**
     * 找到执行对象
     * @param attemptId
     * @return
     */
    Optional<AccessExecution> findExecution(final ExecutionAttemptID attemptId);
}
