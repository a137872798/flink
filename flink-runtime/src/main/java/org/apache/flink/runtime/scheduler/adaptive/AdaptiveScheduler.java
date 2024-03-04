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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricher.Context;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.DefaultVertexAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.MutableVertexAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.failure.DefaultFailureEnricherContext;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismStore;
import org.apache.flink.runtime.scheduler.ExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.JobStatusStore;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerUtils;
import org.apache.flink.runtime.scheduler.UpdateSchedulerNgOnInternalFailuresListener;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobAllocationsInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.ReservedSlots;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAllocator;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.runtime.scheduler.adaptive.scalingpolicy.EnforceMinimalIncreaseRescalingController;
import org.apache.flink.runtime.scheduler.adaptive.scalingpolicy.RescalingController;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.metrics.DeploymentStateTimeMetrics;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.util.BoundedFIFOQueue;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link SchedulerNG} implementation that uses the declarative resource management and
 * automatically adapts the parallelism in case not enough resource could be acquired to run at the
 * configured parallelism, as described in FLIP-160.
 *
 * <p>This scheduler only supports jobs with streaming semantics, i.e., all vertices are connected
 * via pipelined data-exchanges.
 *
 * <p>The implementation is spread over multiple {@link State} classes that control which RPCs are
 * allowed in a given state and what state transitions are possible (see the FLIP for an overview).
 * This class can thus be roughly split into 2 parts:
 *
 * <p>1) RPCs, which must forward the call to the state via {@link State#tryRun(Class,
 * ThrowingConsumer, String)} or {@link State#tryCall(Class, FunctionWithException, String)}.
 *
 * <p>2) Context methods, which are called by states, to either transition into another state or
 * access functionality of some component in the scheduler.
 * 自适应调度器
 */
public class AdaptiveScheduler
        implements SchedulerNG,   // 具备各种功能 包括调度
                Created.Context,
                WaitingForResources.Context,
                CreatingExecutionGraph.Context,
                Executing.Context,
                Restarting.Context,
                Failing.Context,
                Finished.Context,
                StopWithSavepoint.Context {  // 实现各种上下文 可以进行状态间的切换

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveScheduler.class);

    /**
     * 这是job的图对象 内部还包含了顶点 以及各个子任务
     */
    private final JobGraph jobGraph;

    /**
     * 该对象维护该job下各顶点的并行度
     */
    private final VertexParallelismStore initialParallelismStore;

    /**
     * 通过该对象可以申请slot
     */
    private final DeclarativeSlotPool declarativeSlotPool;

    private final long initializationTimestamp;

    private final Executor ioExecutor;
    private final ClassLoader userCodeClassLoader;

    /**
     * 该对象可以清理检查点
     */
    private final CheckpointsCleaner checkpointsCleaner;

    /**
     * 存储检查点数据的仓库
     */
    private final CompletedCheckpointStore completedCheckpointStore;

    /**
     * ID生成器
     */
    private final CheckpointIDCounter checkpointIdCounter;

    private final CompletableFuture<JobStatus> jobTerminationFuture = new CompletableFuture<>();

    /**
     * 计算重启的延迟时间
     */
    private final RestartBackoffTimeStrategy restartBackoffTimeStrategy;

    private final ComponentMainThreadExecutor componentMainThreadExecutor;
    private final FatalErrorHandler fatalErrorHandler;

    /**
     * 该对象可以丰富失败信息
     */
    private final Collection<FailureEnricher> failureEnrichers;

    /**
     * 监听job状态变化
     */
    private final Collection<JobStatusListener> jobStatusListeners;

    /**
     * 该对象可以计算slot的需求量 以及分配
     */
    private final SlotAllocator slotAllocator;

    /**
     * 判断是否需要重新调度
     */
    private final RescalingController rescalingController;

    private final Duration initialResourceAllocationTimeout;

    private final Duration resourceStabilizationTimeout;

    /**
     * 用于产生执行图
     */
    private final ExecutionGraphFactory executionGraphFactory;

    /**
     * 一开始处于创建状态  该对象就是一个状态机
     */
    private State state = new Created(this, LOG);

    private boolean isTransitioningState = false;

    private int numRestarts = 0;

    /**
     * 该对象维护了各子任务的attemptNumber
     */
    private final MutableVertexAttemptNumberStore vertexAttemptNumberStore =
            new DefaultVertexAttemptNumberStore();

    /**
     * 用于执行后台任务
     */
    private BackgroundTask<ExecutionGraph> backgroundTask = BackgroundTask.finishedBackgroundTask();

    private final SchedulerExecutionMode executionMode;

    /**
     * TODO
     */
    private final DeploymentStateTimeMetrics deploymentTimeMetrics;

    /**
     * 表示长度有限的队列
     */
    private final BoundedFIFOQueue<RootExceptionHistoryEntry> exceptionHistory;
    private JobGraphJobInformation jobInformation;

    /**
     * 记录资源的需求
     */
    private ResourceCounter desiredResources = ResourceCounter.empty();

    /**
     * TODO
     */
    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    private final Duration slotIdleTimeout;

    public AdaptiveScheduler(
            JobGraph jobGraph,
            @Nullable JobResourceRequirements jobResourceRequirements,
            Configuration configuration,
            DeclarativeSlotPool declarativeSlotPool,
            SlotAllocator slotAllocator,
            Executor ioExecutor,
            ClassLoader userCodeClassLoader,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointRecoveryFactory checkpointRecoveryFactory,
            Duration initialResourceAllocationTimeout,
            Duration resourceStabilizationTimeout,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            RestartBackoffTimeStrategy restartBackoffTimeStrategy,
            long initializationTimestamp,
            ComponentMainThreadExecutor mainThreadExecutor,
            FatalErrorHandler fatalErrorHandler,
            JobStatusListener jobStatusListener,
            Collection<FailureEnricher> failureEnrichers,
            ExecutionGraphFactory executionGraphFactory)
            throws JobExecutionException {

        assertPreconditions(jobGraph);

        this.jobGraph = jobGraph;
        this.executionMode = configuration.get(JobManagerOptions.SCHEDULER_MODE);

        // 根据job图信息 填充并行度
        VertexParallelismStore vertexParallelismStore =
                computeVertexParallelismStore(jobGraph, executionMode);
        if (jobResourceRequirements != null) {
            // 基于 Requirements 信息产生新的store
            vertexParallelismStore =
                    DefaultVertexParallelismStore.applyJobResourceRequirements(
                                    vertexParallelismStore, jobResourceRequirements)
                            .orElse(vertexParallelismStore);
        }

        this.initialParallelismStore = vertexParallelismStore;
        this.jobInformation = new JobGraphJobInformation(jobGraph, vertexParallelismStore);

        this.declarativeSlotPool = declarativeSlotPool;
        this.initializationTimestamp = initializationTimestamp;
        this.ioExecutor = ioExecutor;
        this.userCodeClassLoader = userCodeClassLoader;
        this.restartBackoffTimeStrategy = restartBackoffTimeStrategy;
        this.fatalErrorHandler = fatalErrorHandler;
        this.checkpointsCleaner = checkpointsCleaner;

        // 创建存储检查点的仓库
        this.completedCheckpointStore =
                SchedulerUtils.createCompletedCheckpointStoreIfCheckpointingIsEnabled(
                        jobGraph, configuration, checkpointRecoveryFactory, ioExecutor, LOG);
        this.checkpointIdCounter =
                SchedulerUtils.createCheckpointIDCounterIfCheckpointingIsEnabled(
                        jobGraph, checkpointRecoveryFactory);

        this.slotAllocator = slotAllocator;

        // 给slot池注册监听器
        declarativeSlotPool.registerNewSlotsListener(this::newResourcesAvailable);

        this.componentMainThreadExecutor = mainThreadExecutor;

        // 控制是否伸缩
        this.rescalingController = new EnforceMinimalIncreaseRescalingController(configuration);

        this.initialResourceAllocationTimeout = initialResourceAllocationTimeout;

        this.resourceStabilizationTimeout = resourceStabilizationTimeout;

        this.executionGraphFactory = executionGraphFactory;

        // 记录job切换到不同状态的时间点
        final JobStatusStore jobStatusStore = new JobStatusStore(initializationTimestamp);
        final Collection<JobStatusListener> tmpJobStatusListeners = new ArrayList<>();
        tmpJobStatusListeners.add(Preconditions.checkNotNull(jobStatusListener));
        tmpJobStatusListeners.add(jobStatusStore);

        // TODO 统计的先忽略
        final MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings =
                MetricOptions.JobStatusMetricsSettings.fromConfiguration(configuration);

        deploymentTimeMetrics =
                new DeploymentStateTimeMetrics(jobGraph.getJobType(), jobStatusMetricsSettings);

        SchedulerBase.registerJobMetrics(
                jobManagerJobMetricGroup,
                jobStatusStore,
                () -> (long) numRestarts,
                deploymentTimeMetrics,
                tmpJobStatusListeners::add,
                initializationTimestamp,
                jobStatusMetricsSettings);

        jobStatusListeners = Collections.unmodifiableCollection(tmpJobStatusListeners);
        this.failureEnrichers = failureEnrichers;

        // 存储错误信息
        this.exceptionHistory =
                new BoundedFIFOQueue<>(
                        configuration.getInteger(WebOptions.MAX_EXCEPTION_HISTORY_SIZE));
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        this.slotIdleTimeout =
                Duration.ofMillis(configuration.get(JobManagerOptions.SLOT_IDLE_TIMEOUT));
    }

    /**
     * 检查前置条件
     * @param jobGraph
     * @throws RuntimeException
     */
    private static void assertPreconditions(JobGraph jobGraph) throws RuntimeException {
        Preconditions.checkState(
                jobGraph.getJobType() == JobType.STREAMING,
                "The adaptive scheduler only supports streaming jobs.");

        for (JobVertex vertex : jobGraph.getVertices()) {
            // 顶点必须设置并行度
            Preconditions.checkState(
                    vertex.getParallelism() > 0,
                    "The adaptive scheduler expects the parallelism being set for each JobVertex (violated JobVertex: %s).",
                    vertex.getID());
            for (JobEdge jobEdge : vertex.getInputs()) {
                Preconditions.checkState(
                        jobEdge.getSource()
                                .getResultType()
                                .isPipelinedOrPipelinedBoundedResultPartition(),
                        "The adaptive scheduler supports pipelined data exchanges (violated by %s -> %s).",
                        jobEdge.getSource().getProducer(),
                        jobEdge.getTarget().getID());
            }
        }
    }

    /**
     * Creates the parallelism store for a set of vertices, optionally with a flag to leave the
     * vertex parallelism unchanged. If the flag is set, the parallelisms must be valid for
     * execution.
     *
     * <p>We need to set parallelism to the max possible value when requesting resources, but when
     * executing the graph we should respect what we are actually given.
     *
     * @param vertices The vertices to store parallelism information for   出现的顶点
     * @param adjustParallelism Whether to adjust the parallelism   表示是否要调整并行度
     * @param defaultMaxParallelismFunc a function for computing a default max parallelism if none
     *     is specified on a given vertex   最大并行度
     * @return The parallelism store.
     * 计算自适应模式下的并行度
     */
    @VisibleForTesting
    static VertexParallelismStore computeReactiveModeVertexParallelismStore(
            Iterable<JobVertex> vertices,
            Function<JobVertex, Integer> defaultMaxParallelismFunc,
            boolean adjustParallelism) {
        DefaultVertexParallelismStore store = new DefaultVertexParallelismStore();

        for (JobVertex vertex : vertices) {
            // if no max parallelism was configured by the user, we calculate and set a default
            final int maxParallelism =
                    vertex.getMaxParallelism() == JobVertex.MAX_PARALLELISM_DEFAULT
                            ? defaultMaxParallelismFunc.apply(vertex)  // 通过该函数计算最大并行度 (按照公式计算 会很大 )
                            : vertex.getMaxParallelism();
            // If the parallelism has already been adjusted, respect what has been configured in the
            // vertex. Otherwise, scale it to the max parallelism to attempt to be "as parallel as
            // possible"
            final int parallelism;
            // 将当前并行度调整为最大并行度
            if (adjustParallelism) {
                parallelism = maxParallelism;
            } else {
                parallelism = vertex.getParallelism();
            }

            VertexParallelismInformation parallelismInfo =
                    new DefaultVertexParallelismInfo(
                            parallelism,
                            maxParallelism,
                            // Allow rescaling if the new desired max parallelism
                            // is not less than what was declared here during scheduling.
                            // This prevents the situation where more resources are requested
                            // based on the computed default, when actually fewer are necessary.
                            (newMax) ->
                                    newMax >= maxParallelism
                                            ? Optional.empty()
                                            : Optional.of(
                                                    "Cannot lower max parallelism in Reactive mode."));
            store.setParallelismInfo(vertex.getID(), parallelismInfo);
        }

        return store;
    }

    /**
     * Creates the parallelism store that should be used for determining scheduling requirements,
     * which may choose different parallelisms than set in the {@link JobGraph} depending on the
     * execution mode.
     *
     * @param jobGraph The job graph for execution.
     * @param executionMode The mode of scheduler execution.
     * @return The parallelism store.
     * 计算并行度
     */
    private static VertexParallelismStore computeVertexParallelismStore(
            JobGraph jobGraph, SchedulerExecutionMode executionMode) {
        if (executionMode == SchedulerExecutionMode.REACTIVE) {
            return computeReactiveModeVertexParallelismStore(
                    jobGraph.getVertices(), SchedulerBase::getDefaultMaxParallelism, true);
        }
        // 表示mode为空
        return SchedulerBase.computeVertexParallelismStore(jobGraph);
    }

    /**
     * Creates the parallelism store that should be used to build the {@link ExecutionGraph}, which
     * will respect the vertex parallelism of the passed {@link JobGraph} in all execution modes.
     *
     * @param jobGraph The job graph for execution.
     * @param executionMode The mode of scheduler execution.
     * @param defaultMaxParallelismFunc a function for computing a default max parallelism if none
     *     is specified on a given vertex
     * @return The parallelism store.
     * 计算并行度
     */
    @VisibleForTesting
    static VertexParallelismStore computeVertexParallelismStoreForExecution(
            JobGraph jobGraph,
            SchedulerExecutionMode executionMode,
            Function<JobVertex, Integer> defaultMaxParallelismFunc) {
        if (executionMode == SchedulerExecutionMode.REACTIVE) {
            return computeReactiveModeVertexParallelismStore(
                    jobGraph.getVertices(), defaultMaxParallelismFunc, false);
        }
        return SchedulerBase.computeVertexParallelismStore(
                jobGraph.getVertices(), defaultMaxParallelismFunc);
    }

    /**
     * 当发现有新的slot可用时触发该方法
     * @param physicalSlots
     */
    private void newResourcesAvailable(Collection<? extends PhysicalSlot> physicalSlots) {
        state.tryRun(
                ResourceListener.class,
                ResourceListener::onNewResourcesAvailable,  // 将当前状态转换后执行监听方法
                "newResourcesAvailable");
    }

    /**
     * 开始调度
     */
    @Override
    public void startScheduling() {
        // 周期性检查是否有空闲的slot 并释放
        checkIdleSlotTimeout();
        state.as(Created.class)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Can only start scheduling when being in Created state."))
                // 会转到goToWaitingForResources
                .startScheduling();
    }

    /**
     * 关闭本对象
     * @return
     */
    @Override
    public CompletableFuture<Void> closeAsync() {
        LOG.debug("Closing the AdaptiveScheduler. Trying to suspend the current job execution.");

        // 会进入finish状态
        state.suspend(new FlinkException("AdaptiveScheduler is being stopped."));

        Preconditions.checkState(
                state instanceof Finished,
                "Scheduler state should be finished after calling state.suspend.");

        backgroundTask.abort();
        // wait for the background task to finish and then close services
        return FutureUtils.composeAfterwards(
                FutureUtils.runAfterwardsAsync(
                        backgroundTask.getTerminationFuture(),
                        () -> stopCheckpointServicesSafely(jobTerminationFuture.get()),
                        getMainThreadExecutor()),
                checkpointsCleaner::closeAsync);
    }

    /**
     * 停止检查点
     * @param terminalState
     */
    private void stopCheckpointServicesSafely(JobStatus terminalState) {
        LOG.debug("Stopping the checkpoint services with state {}.", terminalState);

        Exception exception = null;

        try {
            completedCheckpointStore.shutdown(terminalState, checkpointsCleaner);
        } catch (Exception e) {
            exception = e;
        }

        try {
            checkpointIdCounter.shutdown(terminalState).get();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            LOG.warn("Failed to stop checkpoint services.", exception);
        }
    }

    /**
     * 进入finished状态
     */
    @Override
    public void cancel() {
        state.cancel();
    }

    @Override
    public CompletableFuture<JobStatus> getJobTerminationFuture() {
        return jobTerminationFuture;
    }

    /**
     * 也是进入finished状态
     * @param cause A cause that describes the global failure.
     */
    @Override
    public void handleGlobalFailure(Throwable cause) {
        final FailureEnricher.Context ctx =
                DefaultFailureEnricherContext.forGlobalFailure(
                        jobInformation.getJobID(),
                        jobInformation.getName(),
                        jobManagerJobMetricGroup,
                        ioExecutor,
                        userCodeClassLoader);
        final CompletableFuture<Map<String, String>> failureLabels =
                FailureEnricherUtils.labelFailure(
                        cause, ctx, getMainThreadExecutor(), failureEnrichers);
        state.handleGlobalFailure(cause, failureLabels);
    }

    private CompletableFuture<Map<String, String>> labelFailure(
            final TaskExecutionStateTransition taskExecutionStateTransition) {
        if (taskExecutionStateTransition.getExecutionState() == ExecutionState.FAILED
                && !failureEnrichers.isEmpty()) {
            final Throwable cause = taskExecutionStateTransition.getError(userCodeClassLoader);
            final Context ctx =
                    DefaultFailureEnricherContext.forTaskFailure(
                            jobGraph.getJobID(),
                            jobGraph.getName(),
                            jobManagerJobMetricGroup,
                            ioExecutor,
                            userCodeClassLoader);
            return FailureEnricherUtils.labelFailure(
                    cause, ctx, getMainThreadExecutor(), failureEnrichers);
        }
        return FailureEnricherUtils.EMPTY_FAILURE_LABELS;
    }

    /**
     * 更新任务状态
     * @param taskExecutionState
     * @return
     */
    @Override
    public boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                // 这会获取执行图 并更新状态
                                stateWithExecutionGraph.updateTaskExecutionState(
                                        taskExecutionState, labelFailure(taskExecutionState)),
                        "updateTaskExecutionState")
                .orElse(false);
    }

    /**
     * 请求下一个输入流 转发给执行图 最终转发给Execution
     * @param vertexID
     * @param executionAttempt
     * @return
     * @throws IOException
     */
    @Override
    public SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.requestNextInputSplit(
                                        vertexID, executionAttempt),
                        "requestNextInputSplit")
                .orElseThrow(
                        () -> new IOException("Scheduler is currently not executing the job."));
    }

    /**
     * 请求分区状态 也是转发
     * @param intermediateResultId
     * @param resultPartitionId
     * @return
     * @throws PartitionProducerDisposedException
     */
    @Override
    public ExecutionState requestPartitionState(
            IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.requestPartitionState(
                                        intermediateResultId, resultPartitionId),
                        "requestPartitionState")
                .orElseThrow(() -> new PartitionProducerDisposedException(resultPartitionId));
    }

    /**
     * 将归档图和错误信息合并成 ExecutionGraphInfo
     * @return
     */
    @Override
    public ExecutionGraphInfo requestJob() {
        return new ExecutionGraphInfo(state.getJob(), exceptionHistory.toArrayList());
    }

    @Override
    public CheckpointStatsSnapshot requestCheckpointStats() {
        return state.getJob().getCheckpointStatsSnapshot();
    }

    @Override
    public void archiveFailure(RootExceptionHistoryEntry failure) {
        exceptionHistory.add(failure);
    }

    @Override
    public JobStatus requestJobStatus() {
        return state.getJobStatus();
    }

    @Override
    public JobDetails requestJobDetails() {
        return JobDetails.createDetailsForJob(state.getJob());
    }

    // 下面多个方法都是转发

    /**
     * 获取KvState的位置
     * @param jobId
     * @param registrationName
     * @return
     * @throws UnknownKvStateLocation
     * @throws FlinkJobNotFoundException
     */
    @Override
    public KvStateLocation requestKvStateLocation(JobID jobId, String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException {
        final Optional<StateWithExecutionGraph> asOptional =
                state.as(StateWithExecutionGraph.class);

        if (asOptional.isPresent()) {
            // 也是转发
            return asOptional.get().requestKvStateLocation(jobId, registrationName);
        } else {
            throw new UnknownKvStateLocation(registrationName);
        }
    }

    @Override
    public void notifyKvStateRegistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName,
            KvStateID kvStateId,
            InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.notifyKvStateRegistered(
                                jobId,
                                jobVertexId,
                                keyGroupRange,
                                registrationName,
                                kvStateId,
                                kvStateServerAddress),
                "notifyKvStateRegistered");
    }

    @Override
    public void notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName)
            throws FlinkJobNotFoundException {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.notifyKvStateUnregistered(
                                jobId, jobVertexId, keyGroupRange, registrationName),
                "notifyKvStateUnregistered");
    }

    @Override
    public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.updateAccumulators(accumulatorSnapshot),
                "updateAccumulators");
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable String targetDirectory, boolean cancelJob, SavepointFormatType formatType) {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.triggerSavepoint(
                                        targetDirectory, cancelJob, formatType),
                        "triggerSavepoint")
                .orElse(
                        FutureUtils.completedExceptionally(
                                new CheckpointException(
                                        "The Flink job is currently not executing.",
                                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE)));
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType) {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.triggerCheckpoint(checkpointType),
                        "triggerCheckpoint")
                .orElse(
                        FutureUtils.completedExceptionally(
                                new CheckpointException(
                                        "The Flink job is currently not executing.",
                                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE)));
    }

    @Override
    public void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot checkpointState) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.acknowledgeCheckpoint(
                                jobID,
                                executionAttemptID,
                                checkpointId,
                                checkpointMetrics,
                                checkpointState),
                "acknowledgeCheckpoint");
    }

    @Override
    public void notifyEndOfData(ExecutionAttemptID executionAttemptID) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.notifyEndOfData(executionAttemptID),
                "notifyEndOfData");
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph ->
                        stateWithExecutionGraph.reportCheckpointMetrics(
                                executionAttemptID, checkpointId, checkpointMetrics),
                "reportCheckpointMetrics");
    }

    @Override
    public void declineCheckpoint(DeclineCheckpoint decline) {
        state.tryRun(
                StateWithExecutionGraph.class,
                stateWithExecutionGraph -> stateWithExecutionGraph.declineCheckpoint(decline),
                "declineCheckpoint");
    }

    /**
     * 这里会间接调用 goToStopWithSavepoint
     * @param targetDirectory
     * @param terminate
     * @param formatType
     * @return
     */
    @Override
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable String targetDirectory, boolean terminate, SavepointFormatType formatType) {
        return state.tryCall(
                        Executing.class,
                        executing ->
                                executing.stopWithSavepoint(targetDirectory, terminate, formatType),
                        "stopWithSavepoint")
                .orElse(
                        FutureUtils.completedExceptionally(
                                new CheckpointException(
                                        "The Flink job is currently not executing.",
                                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE)));
    }

    @Override
    public void deliverOperatorEventToCoordinator(
            ExecutionAttemptID taskExecution, OperatorID operator, OperatorEvent evt)
            throws FlinkException {
        final StateWithExecutionGraph stateWithExecutionGraph =
                state.as(StateWithExecutionGraph.class)
                        .orElseThrow(
                                () ->
                                        new TaskNotRunningException(
                                                "Task is not known or in state running on the JobManager."));

        stateWithExecutionGraph.deliverOperatorEventToCoordinator(taskExecution, operator, evt);
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {
        return state.tryCall(
                        StateWithExecutionGraph.class,
                        stateWithExecutionGraph ->
                                stateWithExecutionGraph.deliverCoordinationRequestToCoordinator(
                                        operator, request),
                        "deliverCoordinationRequestToCoordinator")
                .orElseGet(
                        () ->
                                FutureUtils.completedExceptionally(
                                        new FlinkException(
                                                "Coordinator of operator "
                                                        + operator
                                                        + " does not exist")));
    }

    /**
     * 获取资源的需求量
     * @return
     */
    @Override
    public JobResourceRequirements requestJobResourceRequirements() {
        final JobResourceRequirements.Builder builder = JobResourceRequirements.newBuilder();
        for (JobInformation.VertexInformation vertex : jobInformation.getVertices()) {
            builder.setParallelismForJobVertex(
                    vertex.getJobVertexID(), vertex.getMinParallelism(), vertex.getParallelism());
        }
        return builder.build();
    }

    /**
     * 更新资源消耗
     * @param jobResourceRequirements new resource requirements
     */
    @Override
    public void updateJobResourceRequirements(JobResourceRequirements jobResourceRequirements) {
        if (executionMode == SchedulerExecutionMode.REACTIVE) {
            throw new UnsupportedOperationException(
                    "Cannot change the parallelism of a job running in reactive mode.");
        }
        // 更新store的数据
        final Optional<VertexParallelismStore> maybeUpdateVertexParallelismStore =
                DefaultVertexParallelismStore.applyJobResourceRequirements(
                        jobInformation.getVertexParallelismStore(), jobResourceRequirements);
        if (maybeUpdateVertexParallelismStore.isPresent()) {
            // 更新job信息
            this.jobInformation =
                    new JobGraphJobInformation(jobGraph, maybeUpdateVertexParallelismStore.get());
            declareDesiredResources();
            state.tryRun(
                    ResourceListener.class,
                    ResourceListener::onNewResourceRequirements,  // 如果发现需要重新调度  进入restart状态
                    "Current state does not react to desired parallelism changes.");
        }
    }

    // ----------------------------------------------------------------

    @Override
    public boolean hasDesiredResources() {
        final Collection<? extends SlotInfo> freeSlots =
                declarativeSlotPool.getFreeSlotInfoTracker().getFreeSlotsInformation();
        return hasDesiredResources(desiredResources, freeSlots);
    }

    /**
     * 判断slot资源是否足够
     * @param desiredResources
     * @param freeSlots
     * @return
     */
    @VisibleForTesting
    static boolean hasDesiredResources(
            ResourceCounter desiredResources, Collection<? extends SlotInfo> freeSlots) {
        ResourceCounter outstandingResources = desiredResources;
        final Iterator<? extends SlotInfo> slotIterator = freeSlots.iterator();

        while (!outstandingResources.isEmpty() && slotIterator.hasNext()) {
            final SlotInfo slotInfo = slotIterator.next();
            final ResourceProfile resourceProfile = slotInfo.getResourceProfile();

            if (outstandingResources.containsResource(resourceProfile)) {
                outstandingResources = outstandingResources.subtract(resourceProfile, 1);
            } else {
                outstandingResources = outstandingResources.subtract(ResourceProfile.UNKNOWN, 1);
            }
        }

        // 为空 就代表slot足够
        return outstandingResources.isEmpty();
    }

    /**
     * 判断slot是否满足最小需求
     * @return
     */
    @Override
    public boolean hasSufficientResources() {
        return slotAllocator
                .determineParallelism(jobInformation, declarativeSlotPool.getAllSlotsInformation())
                .isPresent();
    }

    /**
     * 产生分配计划
     * @param slotAllocator
     * @param previousExecutionGraph
     * @return
     * @throws NoResourceAvailableException
     */
    private JobSchedulingPlan determineParallelism(
            SlotAllocator slotAllocator, @Nullable ExecutionGraph previousExecutionGraph)
            throws NoResourceAvailableException {

        return slotAllocator
                .determineParallelismAndCalculateAssignment(
                        jobInformation,
                        declarativeSlotPool.getFreeSlotInfoTracker().getFreeSlotsInformation(),
                        JobAllocationsInformation.fromGraph(previousExecutionGraph))
                .orElseThrow(
                        () ->
                                new NoResourceAvailableException(
                                        "Not enough resources available for scheduling."));
    }

    /**
     * 产生归档对象
     * @param jobStatus jobStatus to initialize the {@link ArchivedExecutionGraph} with
     * @param cause cause describing a failure cause; {@code null} if there is none
     * @return
     */
    @Override
    public ArchivedExecutionGraph getArchivedExecutionGraph(
            JobStatus jobStatus, @Nullable Throwable cause) {
        return ArchivedExecutionGraph.createSparseArchivedExecutionGraphWithJobVertices(
                jobInformation.getJobID(),
                jobInformation.getName(),
                jobStatus,
                cause,
                jobInformation.getCheckpointingSettings(),
                initializationTimestamp,
                jobGraph.getVertices(),
                initialParallelismStore);
    }

    /**
     * 开始等待资源
     * @param previousExecutionGraph
     */
    @Override
    public void goToWaitingForResources(@Nullable ExecutionGraph previousExecutionGraph) {
        declareDesiredResources();

        transitionToState(
                // 切换到等待资源的状态
                new WaitingForResources.Factory(
                        this,
                        LOG,
                        this.initialResourceAllocationTimeout,
                        this.resourceStabilizationTimeout,
                        previousExecutionGraph));
    }

    /**
     * 检测当前资源是否充足
     */
    private void declareDesiredResources() {
        final ResourceCounter newDesiredResources = calculateDesiredResources();

        // 表示需要的资源量发生了变化
        if (!newDesiredResources.equals(this.desiredResources)) {
            this.desiredResources = newDesiredResources;
            // 先设置需求量 这样才能从外部offsetSlot
            declarativeSlotPool.setResourceRequirements(this.desiredResources);
        }
    }

    /**
     * 计算需要的资源
     * @return
     */
    private ResourceCounter calculateDesiredResources() {
        return slotAllocator.calculateRequiredSlots(jobInformation.getVertices());
    }

    @Override
    public void goToExecuting(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            List<ExceptionHistoryEntry> failureCollection) {
        transitionToState(
                new Executing.Factory(
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG,
                        this,
                        userCodeClassLoader,
                        failureCollection));
    }

    @Override
    public void goToCanceling(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            List<ExceptionHistoryEntry> failureCollection) {

        transitionToState(
                new Canceling.Factory(
                        this,
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG,
                        userCodeClassLoader,
                        failureCollection));
    }

    @Override
    public void goToRestarting(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Duration backoffTime,
            List<ExceptionHistoryEntry> failureCollection) {

        for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
            final int attemptNumber =
                    executionVertex.getCurrentExecutionAttempt().getAttemptNumber();

            // 因为重启了 更新attemptNumber
            this.vertexAttemptNumberStore.setAttemptCount(
                    executionVertex.getJobvertexId(),
                    executionVertex.getParallelSubtaskIndex(),
                    attemptNumber + 1);
        }

        transitionToState(
                new Restarting.Factory(
                        this,
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG,
                        backoffTime,
                        userCodeClassLoader,
                        failureCollection));
        numRestarts++;
    }

    @Override
    public void goToFailing(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Throwable failureCause,
            List<ExceptionHistoryEntry> failureCollection) {
        transitionToState(
                new Failing.Factory(
                        this,
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        LOG,
                        failureCause,
                        userCodeClassLoader,
                        failureCollection));
    }

    @Override
    public CompletableFuture<String> goToStopWithSavepoint(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            CheckpointScheduling checkpointScheduling,
            CompletableFuture<String> savepointFuture,
            List<ExceptionHistoryEntry> failureCollection) {

        StopWithSavepoint stopWithSavepoint =
                transitionToState(
                        new StopWithSavepoint.Factory(
                                this,
                                executionGraph,
                                executionGraphHandler,
                                operatorCoordinatorHandler,
                                checkpointScheduling,
                                LOG,
                                userCodeClassLoader,
                                savepointFuture,
                                failureCollection));
        return stopWithSavepoint.getOperationFuture();
    }

    @Override
    public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
        transitionToState(new Finished.Factory(this, archivedExecutionGraph, LOG));
    }

    @Override
    public void goToCreatingExecutionGraph(@Nullable ExecutionGraph previousExecutionGraph) {
        final CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
                executionGraphWithAvailableResourcesFuture =
                        createExecutionGraphWithAvailableResourcesAsync(previousExecutionGraph);
        transitionToState(
                // 切换到创建执行图状态
                new CreatingExecutionGraph.Factory(
                        this,
                        executionGraphWithAvailableResourcesFuture,
                        LOG,
                        previousExecutionGraph));
    }

    /**
     * 根据资源图 创建并行度对象
     * @param previousExecutionGraph
     * @return
     */
    private CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
            createExecutionGraphWithAvailableResourcesAsync(
                    @Nullable ExecutionGraph previousExecutionGraph) {
        final JobSchedulingPlan schedulingPlan;
        final VertexParallelismStore adjustedParallelismStore;

        try {
            schedulingPlan = determineParallelism(slotAllocator, previousExecutionGraph);
            JobGraph adjustedJobGraph = jobInformation.copyJobGraph();

            for (JobVertex vertex : adjustedJobGraph.getVertices()) {
                JobVertexID id = vertex.getID();

                // use the determined "available parallelism" to use
                // the resources we have access to
                // 根据计划调整并行度  计划的并行度是由当前可分配slot决定的
                vertex.setParallelism(schedulingPlan.getVertexParallelism().getParallelism(id));
            }

            // use the originally configured max parallelism
            // as the default for consistent runs
            // 基于新的图创建 store
            adjustedParallelismStore =
                    computeVertexParallelismStoreForExecution(
                            adjustedJobGraph,
                            executionMode,
                            (vertex) -> {
                                // 获取之前的并行度
                                VertexParallelismInformation vertexParallelismInfo =
                                        initialParallelismStore.getParallelismInfo(vertex.getID());
                                return vertexParallelismInfo.getMaxParallelism();
                            });
        } catch (Exception exception) {
            return FutureUtils.completedExceptionally(exception);
        }

        // 产生结果
        return createExecutionGraphAndRestoreStateAsync(adjustedParallelismStore)
                .thenApply(
                        executionGraph ->
                                CreatingExecutionGraph.ExecutionGraphWithVertexParallelism.create(
                                        executionGraph, schedulingPlan));
    }

    /**
     * 尝试分配slot
     * @param executionGraphWithVertexParallelism executionGraphWithVertexParallelism to assign
     *     slots to resources
     * @return
     */
    @Override
    public CreatingExecutionGraph.AssignmentResult tryToAssignSlots(
            CreatingExecutionGraph.ExecutionGraphWithVertexParallelism
                    executionGraphWithVertexParallelism) {
        final ExecutionGraph executionGraph =
                executionGraphWithVertexParallelism.getExecutionGraph();

        // 通过执行图启动任务
        executionGraph.start(componentMainThreadExecutor);
        executionGraph.transitionToRunning();

        // 监听任务执行状态
        executionGraph.setInternalTaskFailuresListener(
                new UpdateSchedulerNgOnInternalFailuresListener(this));

        final JobSchedulingPlan jobSchedulingPlan =
                executionGraphWithVertexParallelism.getJobSchedulingPlan();

        // 进行资源分配
        return slotAllocator
                .tryReserveResources(jobSchedulingPlan)
                .map(reservedSlots -> assignSlotsToExecutionGraph(executionGraph, reservedSlots))
                .map(CreatingExecutionGraph.AssignmentResult::success)
                .orElseGet(CreatingExecutionGraph.AssignmentResult::notPossible);
    }

    /**
     * 保存分配结果
     * @param executionGraph
     * @param reservedSlots  分配结果
     * @return
     */
    @Nonnull
    private ExecutionGraph assignSlotsToExecutionGraph(
            ExecutionGraph executionGraph, ReservedSlots reservedSlots) {
        for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
            // 获取分配给该子任务的slot
            final LogicalSlot assignedSlot = reservedSlots.getSlotFor(executionVertex.getID());
            final CompletableFuture<Void> registrationFuture =
                    executionVertex
                            .getCurrentExecutionAttempt()
                            .registerProducedPartitions(assignedSlot.getTaskManagerLocation());
            Preconditions.checkState(
                    registrationFuture.isDone(),
                    "Partition registration must be completed immediately for reactive mode");

            executionVertex.tryAssignResource(assignedSlot);
        }

        return executionGraph;
    }

    /**
     * 使用新的 adjustedParallelismStore 创建执行图
     * @param adjustedParallelismStore
     * @return
     */
    private CompletableFuture<ExecutionGraph> createExecutionGraphAndRestoreStateAsync(
            VertexParallelismStore adjustedParallelismStore) {
        backgroundTask.abort();

        backgroundTask =
                backgroundTask.runAfter(
                        () -> createExecutionGraphAndRestoreState(adjustedParallelismStore),
                        ioExecutor);

        return FutureUtils.switchExecutor(
                backgroundTask.getResultFuture(), getMainThreadExecutor());
    }

    /**
     * 使用新的store创建执行图
     * @param adjustedParallelismStore
     * @return
     * @throws Exception
     */
    @Nonnull
    private ExecutionGraph createExecutionGraphAndRestoreState(
            VertexParallelismStore adjustedParallelismStore) throws Exception {
        return executionGraphFactory.createAndRestoreExecutionGraph(
                jobInformation.copyJobGraph(),
                completedCheckpointStore,
                checkpointsCleaner,
                checkpointIdCounter,
                TaskDeploymentDescriptorFactory.PartitionLocationConstraint.MUST_BE_KNOWN,
                initializationTimestamp,
                vertexAttemptNumberStore,
                adjustedParallelismStore,
                deploymentTimeMetrics,
                // adaptive scheduler works in streaming mode, actually it only
                // supports must be pipelined result partition, mark partition finish is
                // no need.
                rp -> false,
                LOG);
    }

    /**
     *
     * @param executionGraph executionGraph for making the scaling decision.
     * @return
     */
    @Override
    public boolean shouldRescale(ExecutionGraph executionGraph) {
        final Optional<VertexParallelism> maybeNewParallelism =
                slotAllocator.determineParallelism(
                        jobInformation, declarativeSlotPool.getAllSlotsInformation());
        // 并行度变化是否需要重新调度
        return maybeNewParallelism
                .filter(
                        vertexParallelism ->
                                rescalingController.shouldRescale(
                                        getCurrentParallelism(executionGraph), vertexParallelism))
                .isPresent();
    }

    private static VertexParallelism getCurrentParallelism(ExecutionGraph executionGraph) {
        return new VertexParallelism(
                executionGraph.getAllVertices().values().stream()
                        .collect(
                                Collectors.toMap(
                                        ExecutionJobVertex::getJobVertexId,
                                        ExecutionJobVertex::getParallelism)));
    }

    /**
     * 进入结束状态时会触发该方法
     * @param archivedExecutionGraph archivedExecutionGraph represents the final state of the
     */
    @Override
    public void onFinished(ArchivedExecutionGraph archivedExecutionGraph) {

        @Nullable
        final Throwable optionalFailure =
                archivedExecutionGraph.getFailureInfo() != null
                        ? archivedExecutionGraph.getFailureInfo().getException()
                        : null;
        LOG.info(
                "Job {} reached terminal state {}.",
                archivedExecutionGraph.getJobID(),
                archivedExecutionGraph.getState(),
                optionalFailure);

        jobTerminationFuture.complete(archivedExecutionGraph.getState());
    }

    /**
     * 出现异常时触发该方法
     * @param failure failure describing the failure cause
     * @return
     */
    @Override
    public FailureResult howToHandleFailure(Throwable failure) {
        if (ExecutionFailureHandler.isUnrecoverableError(failure)) {
            return FailureResult.canNotRestart(
                    new JobException("The failure is not recoverable", failure));
        }

        restartBackoffTimeStrategy.notifyFailure(failure);
        if (restartBackoffTimeStrategy.canRestart()) {
            return FailureResult.canRestart(
                    failure, Duration.ofMillis(restartBackoffTimeStrategy.getBackoffTime()));
        } else {
            return FailureResult.canNotRestart(
                    new JobException(
                            "Recovery is suppressed by " + restartBackoffTimeStrategy, failure));
        }
    }

    @Override
    public Executor getIOExecutor() {
        return ioExecutor;
    }

    @Override
    public ComponentMainThreadExecutor getMainThreadExecutor() {
        return componentMainThreadExecutor;
    }

    @Override
    public JobManagerJobMetricGroup getMetricGroup() {
        return jobManagerJobMetricGroup;
    }

    @Override
    public boolean isState(State expectedState) {
        return expectedState == this.state;
    }

    @Override
    public void runIfState(State expectedState, Runnable action) {
        if (isState(expectedState)) {
            try {
                action.run();
            } catch (Throwable t) {
                fatalErrorHandler.onFatalError(t);
            }
        } else {
            LOG.debug(
                    "Ignoring scheduled action because expected state {} is not the actual state {}.",
                    expectedState,
                    state);
        }
    }

    @Override
    public ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay) {
        return componentMainThreadExecutor.schedule(
                () -> runIfState(expectedState, action), delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    // ----------------------------------------------------------------

    /**
     * Transition the scheduler to another state. This method guards against state transitions while
     * there is already a transition ongoing. This effectively means that you can not call this
     * method from a State constructor or State#onLeave.
     *
     * @param targetState State to transition to
     * @param <T> Type of the target state
     * @return A target state instance
     * 转换到另一个状态
     */
    @VisibleForTesting
    <T extends State> T transitionToState(StateFactory<T> targetState) {
        Preconditions.checkState(
                !isTransitioningState,
                "State transitions must not be triggered while another state transition is in progress.");
        Preconditions.checkState(
                state.getClass() != targetState.getStateClass(),
                "Attempted to transition into the very state the scheduler is already in.");
        componentMainThreadExecutor.assertRunningInMainThread();

        try {
            isTransitioningState = true;
            LOG.debug(
                    "Transition from state {} to {}.",
                    state.getClass().getSimpleName(),
                    targetState.getStateClass().getSimpleName());

            final JobStatus previousJobStatus = state.getJobStatus();

            // 表示进入另一种状态
            state.onLeave(targetState.getStateClass());
            T targetStateInstance = targetState.getState();
            state = targetStateInstance;

            final JobStatus newJobStatus = state.getJobStatus();

            if (previousJobStatus != newJobStatus) {
                final long timestamp = System.currentTimeMillis();
                jobStatusListeners.forEach(
                        listener ->
                                // 通知监听器
                                listener.jobStatusChanges(
                                        jobInformation.getJobID(), newJobStatus, timestamp));
            }

            return targetStateInstance;
        } finally {
            isTransitioningState = false;
        }
    }

    @VisibleForTesting
    State getState() {
        return state;
    }

    /**
     * Check for slots that are idle for more than {@link JobManagerOptions#SLOT_IDLE_TIMEOUT} and
     * release them back to the ResourceManager.
     * 检查是否获取slot超时
     */
    private void checkIdleSlotTimeout() {
        // 表示终止了
        if (getState().getJobStatus().isGloballyTerminalState()) {
            // Job has reached the terminal state, so we can return all slots to the ResourceManager
            // to speed things up because we no longer need them. This optimization lets us skip
            // waiting for the slot pool service to close.
            // 归还所有slot
            for (SlotInfo slotInfo : declarativeSlotPool.getAllSlotsInformation()) {
                declarativeSlotPool.releaseSlot(
                        slotInfo.getAllocationId(),
                        new FlinkException(
                                "Returning slots to their owners, because the job has reached a globally terminal state."));
            }
            return;
        } else if (getState().getJobStatus().isTerminalState()) {
            // do nothing
            // prevent idleness check running again while scheduler was already shut down
            // don't release slots because JobMaster may want to hold on to slots in case
            // it re-acquires leadership
            return;
        }
        // 将长时间闲置的slot释放
        declarativeSlotPool.releaseIdleSlots(System.currentTimeMillis());
        getMainThreadExecutor()
                .schedule(
                        this::checkIdleSlotTimeout,  // 周期性检查
                        slotIdleTimeout.toMillis(),
                        TimeUnit.MILLISECONDS);
    }
}
