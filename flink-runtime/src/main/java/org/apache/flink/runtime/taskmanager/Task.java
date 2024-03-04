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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointStoreUtil;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.runtime.jobgraph.tasks.CoordinatedTask;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotPayload;
import org.apache.flink.types.Either;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TaskManagerExceptionUtils;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.WrappingRuntimeException;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The Task represents one execution of a parallel subtask on a TaskManager. A Task wraps a Flink
 * operator (which may be a user function) and runs it, providing all services necessary for example
 * to consume input data, produce its results (intermediate result partitions) and communicate with
 * the JobManager.
 *
 * <p>The Flink operators (implemented as subclasses of {@link TaskInvokable} have only data
 * readers, writers, and certain event callbacks. The task connects those to the network stack and
 * actor messages, and tracks the state of the execution and handles exceptions.
 *
 * <p>Tasks have no knowledge about how they relate to other tasks, or whether they are the first
 * attempt to execute the task, or a repeated attempt. All of that is only known to the JobManager.
 * All the task knows are its own runnable code, the task's configuration, and the IDs of the
 * intermediate results to consume and produce (if any).
 *
 * <p>Each Task is run by one dedicated thread.
 */
public class Task
        implements Runnable, TaskSlotPayload, TaskActions, PartitionProducerStateProvider {

    /** The class logger. */
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    /** The thread group that contains all task threads. */
    private static final ThreadGroup TASK_THREADS_GROUP = new ThreadGroup("Flink Task Threads");

    /** For atomic state updates.
     * 原子更新当前task 状态
     * */
    private static final AtomicReferenceFieldUpdater<Task, ExecutionState> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    Task.class, ExecutionState.class, "executionState");

    // ------------------------------------------------------------------------
    //  Constant fields that are part of the initial Task construction
    // ------------------------------------------------------------------------

    /** The job that the task belongs to. */
    private final JobID jobId;

    /** The vertex in the JobGraph whose code the task executes. */
    private final JobVertexID vertexId;

    /** The execution attempt of the parallel subtask.
     * 通过该对象可以定位到子任务
     * */
    private final ExecutionAttemptID executionId;

    /** ID which identifies the slot in which the task is supposed to run.
     * 对应一个被分配的slot slot代表资源的载体
     * */
    private final AllocationID allocationId;

    /** TaskInfo object for this task.
     * 描述该任务的信息
     * */
    private final TaskInfo taskInfo;

    /** The name of the task, including subtask indexes. */
    private final String taskNameWithSubtask;

    /** The job-wide configuration object. */
    private final Configuration jobConfiguration;

    /** The task-specific configuration. */
    private final Configuration taskConfiguration;

    /** The jar files used by this task.
     * 用于检索jar文件的
     * */
    private final Collection<PermanentBlobKey> requiredJarFiles;

    /** The classpaths used by this task.
     * 表示该task用到的jar包 对应的路径
     * */
    private final Collection<URL> requiredClasspaths;

    /** The name of the class that holds the invokable code. */
    private final String nameOfInvokableClass;

    /** Access to task manager configuration and host names.
     * 提供运行时信息
     * */
    private final TaskManagerRuntimeInfo taskManagerConfig;

    /** The memory manager to be used by this task. */
    private final MemoryManager memoryManager;

    /** Shared memory manager provided by the task manager.
     * 表示可被共享的资源
     * */
    private final SharedResources sharedResources;

    /** The I/O manager to be used by this task.
     * 这个是管理文件的
     * */
    private final IOManager ioManager;

    /** The BroadcastVariableManager to be used by this task.
     * 管理广播变量
     * */
    private final BroadcastVariableManager broadcastVariableManager;

    /**
     * 可以通过该对象分发事件
     */
    private final TaskEventDispatcher taskEventDispatcher;

    /** Information provider for external resources. */
    private final ExternalResourceInfoProvider externalResourceInfoProvider;

    /** The manager for state of operators running in this task/slot.
     * 维护state
     * */
    private final TaskStateManager taskStateManager;

    /**
     * Serialized version of the job specific execution configuration (see {@link ExecutionConfig}).
     * 表示执行时的配置
     */
    private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

    /**
     * 用于向分区写入数据
     */
    private final ResultPartitionWriter[] partitionWriters;

    /**
     * 可以从gate读取数据
     */
    private final IndexedInputGate[] inputGates;

    /** Connection to the task manager. */
    private final TaskManagerActions taskManagerActions;

    /** Input split provider for the task.
     * 为task提供已经拆分过的数据
     * */
    private final InputSplitProvider inputSplitProvider;

    /** Checkpoint notifier used to communicate with the CheckpointCoordinator.
     * 通过该对象可以将结果通知给 CheckpointCoordinator
     * */
    private final CheckpointResponder checkpointResponder;

    /**
     * The gateway for operators to send messages to the operator coordinators on the Job Manager.
     * 借助网关对象可以将一组事件通知到operator
     */
    private final TaskOperatorEventGateway operatorCoordinatorEventGateway;

    /** GlobalAggregateManager used to update aggregates on the JobMaster.
     * 用于维护全局的累加值
     * */
    private final GlobalAggregateManager aggregateManager;

    /** The library cache, from which the task can request its class loader.
     * */
    private final LibraryCacheManager.ClassLoaderHandle classLoaderHandle;

    /** The cache for user-defined files that the invokable requires.
     * 用于缓存文件
     * */
    private final FileCache fileCache;

    /** The service for kvState registration of this task.
     * 通过该对象与 KV 服务交互
     * */
    private final KvStateService kvStateService;

    /** The registry of this task which enables live reporting of accumulators.
     * 该对象维护累加数据
     * */
    private final AccumulatorRegistry accumulatorRegistry;

    /** The thread that executes the task. */
    private final Thread executingThread;

    /** Parent group for all metrics of this task. */
    private final TaskMetricGroup metrics;

    /** Partition producer state checker to request partition states from.
     * 通过分区可以找到ExecutionState
     * */
    private final PartitionProducerStateChecker partitionProducerStateChecker;

    /** Executor to run future callbacks. */
    private final Executor executor;

    /** Future that is completed once {@link #run()} exits. */
    private final CompletableFuture<ExecutionState> terminationFuture = new CompletableFuture<>();

    /** The factory of channel state write request executor.
     * 可以创建处理检查点请求的对象
     * */
    private final ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory;

    // ------------------------------------------------------------------------
    //  Fields that control the task execution. All these fields are volatile
    //  (which means that they introduce memory barriers), to establish
    //  proper happens-before semantics on parallel modification
    // ------------------------------------------------------------------------

    /** atomic flag that makes sure the invokable is canceled exactly once upon error.
     * 被设置后 在出错时 会取消一次
     * */
    private final AtomicBoolean invokableHasBeenCanceled;
    /**
     * The invokable of this task, if initialized. All accesses must copy the reference and check
     * for null, as this field is cleared as part of the disposal logic.
     * 通过该对象可以执行任务
     */
    @Nullable private volatile TaskInvokable invokable;

    /** The current execution state of the task. */
    private volatile ExecutionState executionState = ExecutionState.CREATED;

    /** The observed exception, in case the task execution failed. */
    private volatile Throwable failureCause;

    /** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
    private long taskCancellationInterval;

    /** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
    private long taskCancellationTimeout;

    /**
     * This class loader should be set as the context class loader for threads that may dynamically
     * load user code.
     */
    private UserCodeClassLoader userCodeClassLoader;

    /**
     * <b>IMPORTANT:</b> This constructor may not start any work that would need to be undone in the
     * case of a failing task deployment.
     */
    public Task(
            JobInformation jobInformation,
            TaskInformation taskInformation,
            ExecutionAttemptID executionAttemptID,
            AllocationID slotAllocationId,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
            MemoryManager memManager,
            SharedResources sharedResources,
            IOManager ioManager,
            ShuffleEnvironment<?, ?> shuffleEnvironment,
            KvStateService kvStateService,
            BroadcastVariableManager bcVarManager,
            TaskEventDispatcher taskEventDispatcher,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            TaskStateManager taskStateManager,
            TaskManagerActions taskManagerActions,
            InputSplitProvider inputSplitProvider,
            CheckpointResponder checkpointResponder,
            TaskOperatorEventGateway operatorCoordinatorEventGateway,
            GlobalAggregateManager aggregateManager,
            LibraryCacheManager.ClassLoaderHandle classLoaderHandle,
            FileCache fileCache,
            TaskManagerRuntimeInfo taskManagerConfig,
            @Nonnull TaskMetricGroup metricGroup,
            PartitionProducerStateChecker partitionProducerStateChecker,
            Executor executor,
            ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory) {

        Preconditions.checkNotNull(jobInformation);
        Preconditions.checkNotNull(taskInformation);

        this.taskInfo =
                new TaskInfo(
                        taskInformation.getTaskName(),
                        taskInformation.getMaxNumberOfSubtasks(),
                        executionAttemptID.getSubtaskIndex(),
                        taskInformation.getNumberOfSubtasks(),
                        executionAttemptID.getAttemptNumber(),
                        String.valueOf(slotAllocationId));

        this.jobId = jobInformation.getJobId();
        this.vertexId = taskInformation.getJobVertexId();
        this.executionId = Preconditions.checkNotNull(executionAttemptID);
        this.allocationId = Preconditions.checkNotNull(slotAllocationId);
        this.taskNameWithSubtask = taskInfo.getTaskNameWithSubtasks();
        this.jobConfiguration = jobInformation.getJobConfiguration();
        this.taskConfiguration = taskInformation.getTaskConfiguration();
        this.requiredJarFiles = jobInformation.getRequiredJarFileBlobKeys();
        this.requiredClasspaths = jobInformation.getRequiredClasspathURLs();
        this.nameOfInvokableClass = taskInformation.getInvokableClassName();
        // 执行时配置 会覆盖部分TM配置
        this.serializedExecutionConfig = jobInformation.getSerializedExecutionConfig();

        Configuration tmConfig = taskManagerConfig.getConfiguration();
        this.taskCancellationInterval =
                tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL);
        this.taskCancellationTimeout =
                tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT);

        this.memoryManager = Preconditions.checkNotNull(memManager);
        this.sharedResources = Preconditions.checkNotNull(sharedResources);
        this.ioManager = Preconditions.checkNotNull(ioManager);
        this.broadcastVariableManager = Preconditions.checkNotNull(bcVarManager);
        this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
        this.taskStateManager = Preconditions.checkNotNull(taskStateManager);
        this.accumulatorRegistry = new AccumulatorRegistry(jobId, executionId);

        this.inputSplitProvider = Preconditions.checkNotNull(inputSplitProvider);
        this.checkpointResponder = Preconditions.checkNotNull(checkpointResponder);
        this.operatorCoordinatorEventGateway =
                Preconditions.checkNotNull(operatorCoordinatorEventGateway);
        this.aggregateManager = Preconditions.checkNotNull(aggregateManager);
        this.taskManagerActions = checkNotNull(taskManagerActions);
        this.externalResourceInfoProvider = checkNotNull(externalResourceInfoProvider);

        this.classLoaderHandle = Preconditions.checkNotNull(classLoaderHandle);
        this.fileCache = Preconditions.checkNotNull(fileCache);
        this.kvStateService = Preconditions.checkNotNull(kvStateService);
        this.taskManagerConfig = Preconditions.checkNotNull(taskManagerConfig);

        this.metrics = metricGroup;

        this.partitionProducerStateChecker =
                Preconditions.checkNotNull(partitionProducerStateChecker);
        this.executor = Preconditions.checkNotNull(executor);
        this.channelStateExecutorFactory = channelStateExecutorFactory;

        // create the reader and writer structures

        final String taskNameWithSubtaskAndId = taskNameWithSubtask + " (" + executionId + ')';

        // 产生洗牌的上下文对象
        final ShuffleIOOwnerContext taskShuffleContext =
                shuffleEnvironment.createShuffleIOOwnerContext(
                        taskNameWithSubtaskAndId, executionId, metrics.getIOMetricGroup());

        // produced intermediate result partitions
        // 产生存储中间结果集的分区
        final ResultPartitionWriter[] resultPartitionWriters =
                shuffleEnvironment
                        .createResultPartitionWriters(
                                taskShuffleContext, resultPartitionDeploymentDescriptors)
                        .toArray(new ResultPartitionWriter[] {});

        this.partitionWriters = resultPartitionWriters;

        // consumed intermediate result partitions
        final IndexedInputGate[] gates =
                shuffleEnvironment
                        .createInputGates(taskShuffleContext, this, inputGateDeploymentDescriptors)
                        .toArray(new IndexedInputGate[0]);

        this.inputGates = new IndexedInputGate[gates.length];
        int counter = 0;
        for (IndexedInputGate gate : gates) {
            inputGates[counter++] =
                    // 包装统计数据
                    new InputGateWithMetrics(
                            gate, metrics.getIOMetricGroup().getNumBytesInCounter());
        }

        if (shuffleEnvironment instanceof NettyShuffleEnvironment) {
            //noinspection deprecation
            ((NettyShuffleEnvironment) shuffleEnvironment)
                    .registerLegacyNetworkMetrics(
                            metrics.getIOMetricGroup(), resultPartitionWriters, gates);
        }

        invokableHasBeenCanceled = new AtomicBoolean(false);

        // finally, create the executing thread, but do not start it
        executingThread = new Thread(TASK_THREADS_GROUP, this, taskNameWithSubtask);
    }

    // ------------------------------------------------------------------------
    //  Accessors
    // ------------------------------------------------------------------------

    @Override
    public JobID getJobID() {
        return jobId;
    }

    public JobVertexID getJobVertexId() {
        return vertexId;
    }

    @Override
    public ExecutionAttemptID getExecutionId() {
        return executionId;
    }

    @Override
    public AllocationID getAllocationId() {
        return allocationId;
    }

    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    public Configuration getJobConfiguration() {
        return jobConfiguration;
    }

    public Configuration getTaskConfiguration() {
        return this.taskConfiguration;
    }

    public AccumulatorRegistry getAccumulatorRegistry() {
        return accumulatorRegistry;
    }

    public TaskMetricGroup getMetricGroup() {
        return metrics;
    }

    public Thread getExecutingThread() {
        return executingThread;
    }

    @Override
    public CompletableFuture<ExecutionState> getTerminationFuture() {
        return terminationFuture;
    }

    @VisibleForTesting
    long getTaskCancellationInterval() {
        return taskCancellationInterval;
    }

    @VisibleForTesting
    long getTaskCancellationTimeout() {
        return taskCancellationTimeout;
    }

    @Nullable
    @VisibleForTesting
    TaskInvokable getInvokable() {
        return invokable;
    }

    /**
     * 是否需要背压
     * @return
     */
    public boolean isBackPressured() {
        // 表示都还没启动呢
        if (invokable == null
                || partitionWriters.length == 0
                || (executionState != ExecutionState.INITIALIZING
                        && executionState != ExecutionState.RUNNING)) {
            return false;
        }
        for (int i = 0; i < partitionWriters.length; ++i) {
            // 一旦某个writer无法写入数据 代表内存不足  开启背压
            if (!partitionWriters[i].isAvailable()) {
                return true;
            }
        }
        return false;
    }

    // ------------------------------------------------------------------------
    //  Task Execution
    // ------------------------------------------------------------------------

    /**
     * Returns the current execution state of the task.
     *
     * @return The current execution state of the task.
     */
    public ExecutionState getExecutionState() {
        return this.executionState;
    }

    /**
     * Checks whether the task has failed, is canceled, or is being canceled at the moment.
     *
     * @return True is the task in state FAILED, CANCELING, or CANCELED, false otherwise.
     */
    public boolean isCanceledOrFailed() {
        return executionState == ExecutionState.CANCELING
                || executionState == ExecutionState.CANCELED
                || executionState == ExecutionState.FAILED;
    }

    /**
     * If the task has failed, this method gets the exception that caused this task to fail.
     * Otherwise this method returns null.
     *
     * @return The exception that caused the task to fail, or null, if the task has not failed.
     */
    public Throwable getFailureCause() {
        return failureCause;
    }

    /** Starts the task's thread. */
    public void startTaskThread() {
        executingThread.start();
    }

    /** The core work method that bootstraps the task and executes its code. */
    @Override
    public void run() {
        try {
            doRun();
        } finally {
            terminationFuture.complete(executionState);
        }
    }

    /**
     * task 简单理解就是一个后台任务
     */
    private void doRun() {
        // ----------------------------
        //  Initial State transition
        // ----------------------------
        while (true) {
            ExecutionState current = this.executionState;
            if (current == ExecutionState.CREATED) {
                // 进行状态机切换
                if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
                    // success, we can start our work
                    break;
                }
                // 此时进入了一个完结的状态
            } else if (current == ExecutionState.FAILED) {
                // we were immediately failed. tell the TaskManager that we reached our final state
                notifyFinalState();
                if (metrics != null) {
                    metrics.close();
                }
                // 退出循环
                return;
            } else if (current == ExecutionState.CANCELING) {
                if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
                    // we were immediately canceled. tell the TaskManager that we reached our final
                    // state
                    notifyFinalState();
                    if (metrics != null) {
                        metrics.close();
                    }
                    return;
                }
            } else {
                if (metrics != null) {
                    metrics.close();
                }
                throw new IllegalStateException(
                        "Invalid state for beginning of operation of task " + this + '.');
            }
        }

        // 此时进入了部署阶段

        // all resource acquisitions and registrations from here on
        // need to be undone in the end
        Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
        TaskInvokable invokable = null;

        try {
            // ----------------------------
            //  Task Bootstrap - We periodically
            //  check for canceling as a shortcut
            // ----------------------------

            // activate safety net for task thread
            LOG.debug("Creating FileSystem stream leak safety net for task {}", this);

            // 该对象用于维护closable对象
            FileSystemSafetyNet.initializeSafetyNetForThread();

            // first of all, get a user-code classloader
            // this may involve downloading the job's JAR files and/or classes
            LOG.info("Loading JAR files for task {}.", this);

            // 获取类加载器
            userCodeClassLoader = createUserCodeClassloader();

            // 反序列化 得到执行配置
            final ExecutionConfig executionConfig =
                    serializedExecutionConfig.deserializeValue(userCodeClassLoader.asClassLoader());
            Configuration executionConfigConfiguration = executionConfig.toConfiguration();

            // 覆盖默认配置

            // override task cancellation interval from Flink config if set in ExecutionConfig
            taskCancellationInterval =
                    executionConfigConfiguration
                            .getOptional(TaskManagerOptions.TASK_CANCELLATION_INTERVAL)
                            .orElse(taskCancellationInterval);

            // override task cancellation timeout from Flink config if set in ExecutionConfig
            taskCancellationTimeout =
                    executionConfigConfiguration
                            .getOptional(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT)
                            .orElse(taskCancellationTimeout);

            // 发现此时任务已经被取消 或失败
            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            // register the task with the network stack
            // this operation may fail if the system does not have enough
            // memory to run the necessary data exchanges
            // the registration must also strictly be undone
            // ----------------------------------------------------------------

            LOG.debug("Registering task at network: {}.", this);

            // 触发各对象的 setup
            setupPartitionsAndGates(partitionWriters, inputGates);

            for (ResultPartitionWriter partitionWriter : partitionWriters) {
                // 注册到分发器上 这样产生相关事件就可以通知 相关监听器
                taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
            }

            // next, kick off the background copying of files for the distributed cache
            try {
                // 从配置中加载缓存数据
                for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                        DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
                    LOG.info("Obtaining local cache file for '{}'.", entry.getKey());

                    // 产生本地文件 并返回路径
                    Future<Path> cp =
                            fileCache.createTmpFile(
                                    entry.getKey(), entry.getValue(), jobId, executionId);
                    distributedCacheEntries.put(entry.getKey(), cp);
                }
            } catch (Exception e) {
                throw new Exception(
                        String.format(
                                "Exception while adding files to distributed cache of task %s (%s).",
                                taskNameWithSubtask, executionId),
                        e);
            }

            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            //  call the user code initialization methods
            // ----------------------------------------------------------------

            // 为该task创建用于存储 KvState的容器
            TaskKvStateRegistry kvStateRegistry =
                    kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());

            // 创建环境对象
            Environment env =
                    new RuntimeEnvironment(
                            jobId,
                            vertexId,
                            executionId,
                            executionConfig,
                            taskInfo,
                            jobConfiguration,
                            taskConfiguration,
                            userCodeClassLoader,
                            memoryManager,
                            sharedResources,
                            ioManager,
                            broadcastVariableManager,
                            taskStateManager,
                            aggregateManager,
                            accumulatorRegistry,
                            kvStateRegistry,
                            inputSplitProvider,
                            distributedCacheEntries,
                            partitionWriters,
                            inputGates,
                            taskEventDispatcher,
                            checkpointResponder,
                            operatorCoordinatorEventGateway,
                            taskManagerConfig,
                            metrics,
                            this,
                            externalResourceInfoProvider,
                            channelStateExecutorFactory,
                            taskManagerActions);

            // Make sure the user code classloader is accessible thread-locally.
            // We are setting the correct context class loader before instantiating the invokable
            // so that it is available to the invokable during its entire lifetime.
            executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

            // When constructing invokable, separate threads can be constructed and thus should be
            // monitored for system exit (in addition to invoking thread itself monitored below).
            FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
            try {
                // now load and instantiate the task's invokable code
                // 开始加载 任务对象
                invokable =
                        loadAndInstantiateInvokable(
                                userCodeClassLoader.asClassLoader(), nameOfInvokableClass, env);
            } finally {
                FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
            }

            // ----------------------------------------------------------------
            //  actual task core work
            // ----------------------------------------------------------------

            // we must make strictly sure that the invokable is accessible to the cancel() call
            // by the time we switched to running.
            this.invokable = invokable;

            // 在这里完成了 restore invoke
            restoreAndInvoke(invokable);

            // make sure, we enter the catch block if the task leaves the invoke() method due
            // to the fact that it has been canceled
            if (isCanceledOrFailed()) {
                throw new CancelTaskException();
            }

            // ----------------------------------------------------------------
            //  finalization of a successful execution
            // ----------------------------------------------------------------

            // finish the produced partitions. if this fails, we consider the execution failed.
            // 此时数据已经写入分区了
            for (ResultPartitionWriter partitionWriter : partitionWriters) {
                if (partitionWriter != null) {
                    // 比如流水线模式  会发送一条表示end的记录
                    partitionWriter.finish();
                }
            }

            // try to mark the task as finished
            // if that fails, the task was canceled/failed in the meantime
            // 转换成finished
            if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
                throw new CancelTaskException();
            }
        } catch (Throwable t) {
            // ----------------------------------------------------------------
            // the execution failed. either the invokable code properly failed, or
            // an exception was thrown as a side effect of cancelling
            // ----------------------------------------------------------------

            t = preProcessException(t);

            try {
                // transition into our final state. we should be either in DEPLOYING, INITIALIZING,
                // RUNNING, CANCELING, or FAILED
                // loop for multiple retries during concurrent state changes via calls to cancel()
                // or to failExternally()
                while (true) {
                    ExecutionState current = this.executionState;

                    if (current == ExecutionState.RUNNING
                            || current == ExecutionState.INITIALIZING
                            || current == ExecutionState.DEPLOYING) {
                        // 表示是被取消的
                        if (ExceptionUtils.findThrowable(t, CancelTaskException.class)
                                .isPresent()) {
                            if (transitionState(current, ExecutionState.CANCELED, t)) {
                                cancelInvokable(invokable);
                                break;
                            }
                        } else {
                            // 转换成failed
                            if (transitionState(current, ExecutionState.FAILED, t)) {
                                cancelInvokable(invokable);
                                break;
                            }
                        }
                    } else if (current == ExecutionState.CANCELING) {
                        if (transitionState(current, ExecutionState.CANCELED)) {
                            break;
                        }
                    } else if (current == ExecutionState.FAILED) {
                        // in state failed already, no transition necessary any more
                        break;
                    }
                    // unexpected state, go to failed
                    else if (transitionState(current, ExecutionState.FAILED, t)) {
                        LOG.error(
                                "Unexpected state in task {} ({}) during an exception: {}.",
                                taskNameWithSubtask,
                                executionId,
                                current);
                        break;
                    }
                    // else fall through the loop and
                }
            } catch (Throwable tt) {
                String message =
                        String.format(
                                "FATAL - exception in exception handler of task %s (%s).",
                                taskNameWithSubtask, executionId);
                LOG.error(message, tt);
                notifyFatalError(message, tt);
            }
        } finally {
            try {
                LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);

                // clear the reference to the invokable. this helps guard against holding references
                // to the invokable and its structures in cases where this Task object is still
                // referenced
                this.invokable = null;

                // free the network resources
                // 释放资源
                releaseResources();

                // free memory resources
                // 释放内存
                if (invokable != null) {
                    memoryManager.releaseAll(invokable);
                }

                // remove all of the tasks resources
                // 解除引用
                fileCache.releaseJob(jobId, executionId);

                // close and de-activate safety net for task thread
                LOG.debug("Ensuring all FileSystem streams are closed for task {}", this);
                FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

                notifyFinalState();
            } catch (Throwable t) {
                // an error in the resource cleanup is fatal
                String message =
                        String.format(
                                "FATAL - exception in resource cleanup of task %s (%s).",
                                taskNameWithSubtask, executionId);
                LOG.error(message, t);
                notifyFatalError(message, t);
            }

            // un-register the metrics at the end so that the task may already be
            // counted as finished when this happens
            // errors here will only be logged
            try {
                metrics.close();
            } catch (Throwable t) {
                LOG.error(
                        "Error during metrics de-registration of task {} ({}).",
                        taskNameWithSubtask,
                        executionId,
                        t);
            }
        }
    }

    /** Unwrap, enrich and handle fatal errors.
     * 处理异常前的检测
     * */
    private Throwable preProcessException(Throwable t) {
        // unwrap wrapped exceptions to make stack traces more compact
        if (t instanceof WrappingRuntimeException) {
            t = ((WrappingRuntimeException) t).unwrap();
        }

        TaskManagerExceptionUtils.tryEnrichTaskManagerError(t);

        // check if the exception is unrecoverable
        if (ExceptionUtils.isJvmFatalError(t)
                || (t instanceof OutOfMemoryError
                        && taskManagerConfig.shouldExitJvmOnOutOfMemoryError())) {

            // terminate the JVM immediately
            // don't attempt a clean shutdown, because we cannot expect the clean shutdown
            // to complete
            try {
                LOG.error(
                        "Encountered fatal error {} - terminating the JVM",
                        t.getClass().getName(),
                        t);
            } finally {
                Runtime.getRuntime().halt(-1);
            }
        }

        return t;
    }

    /**
     * 准备执行 Invokable
     * @param finalInvokable
     * @throws Exception
     */
    private void restoreAndInvoke(TaskInvokable finalInvokable) throws Exception {
        try {
            // switch to the INITIALIZING state, if that fails, we have been canceled/failed in the
            // meantime
            // 部署阶段就是 Invoke 和 env的准备阶段  此时进入初始化阶段
            if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.INITIALIZING)) {
                throw new CancelTaskException();
            }

            taskManagerActions.updateTaskExecutionState(
                    new TaskExecutionState(executionId, ExecutionState.INITIALIZING));

            // make sure the user code classloader is accessible thread-locally
            executingThread.setContextClassLoader(userCodeClassLoader.asClassLoader());

            // 先进行数据恢复
            runWithSystemExitMonitoring(finalInvokable::restore);

            // 进入running状态
            if (!transitionState(ExecutionState.INITIALIZING, ExecutionState.RUNNING)) {
                throw new CancelTaskException();
            }

            // notify everyone that we switched to running
            taskManagerActions.updateTaskExecutionState(
                    new TaskExecutionState(executionId, ExecutionState.RUNNING));

            // 执行invoke
            runWithSystemExitMonitoring(finalInvokable::invoke);
        } catch (Throwable throwable) {
            try {
                runWithSystemExitMonitoring(() -> finalInvokable.cleanUp(throwable));
            } catch (Throwable cleanUpThrowable) {
                throwable.addSuppressed(cleanUpThrowable);
            }
            throw throwable;
        }
        // 进行清理
        runWithSystemExitMonitoring(() -> finalInvokable.cleanUp(null));
    }

    /**
     * Monitor user codes from exiting JVM covering user function invocation. This can be done in a
     * finer-grained way like enclosing user callback functions individually, but as exit triggered
     * by framework is not performed and expected in this invoke function anyhow, we can monitor
     * exiting JVM for entire scope.
     */
    private void runWithSystemExitMonitoring(RunnableWithException action) throws Exception {
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            action.run();
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    @VisibleForTesting
    public static void setupPartitionsAndGates(
            ResultPartitionWriter[] producedPartitions, InputGate[] inputGates) throws IOException {

        for (ResultPartitionWriter partition : producedPartitions) {
            partition.setup();
        }

        // InputGates must be initialized after the partitions, since during InputGate#setup
        // we are requesting partitions
        for (InputGate gate : inputGates) {
            gate.setup();
        }
    }

    /**
     * Releases resources before task exits. We should also fail the partition to release if the
     * task has failed, is canceled, or is being canceled at the moment.
     * 释放资源
     */
    private void releaseResources() {
        LOG.debug(
                "Release task {} network resources (state: {}).",
                taskNameWithSubtask,
                getExecutionState());

        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            // 移除掉handler
            taskEventDispatcher.unregisterPartition(partitionWriter.getPartitionId());
        }

        // close network resources
        if (isCanceledOrFailed()) {
            failAllResultPartitions();
        }
        closeAllResultPartitions();
        closeAllInputGates();

        try {
            taskStateManager.close();
        } catch (Exception e) {
            LOG.error("Failed to close task state manager for task {}.", taskNameWithSubtask, e);
        }
    }

    /**
     * 通知所有分区 写入失败
     */
    private void failAllResultPartitions() {
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            partitionWriter.fail(getFailureCause());
        }
    }

    /**
     * 关闭writer
     */
    private void closeAllResultPartitions() {
        for (ResultPartitionWriter partitionWriter : partitionWriters) {
            try {
                partitionWriter.close();
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                LOG.error(
                        "Failed to release result partition for task {}.", taskNameWithSubtask, t);
            }
        }
    }

    /**
     * 关闭所有gate
     */
    private void closeAllInputGates() {
        TaskInvokable invokable = this.invokable;
        if (invokable == null || !invokable.isUsingNonBlockingInput()) {
            // Cleanup resources instead of invokable if it is null, or prevent it from being
            // blocked on input, or interrupt if it is already blocked. Not needed for StreamTask
            // (which does NOT use blocking input); for which this could cause race conditions
            for (InputGate inputGate : inputGates) {
                try {
                    inputGate.close();
                } catch (Throwable t) {
                    ExceptionUtils.rethrowIfFatalError(t);
                    LOG.error("Failed to release input gate for task {}.", taskNameWithSubtask, t);
                }
            }
        }
    }

    /**
     * 获取用户类加载器
     * @return
     * @throws Exception
     */
    private UserCodeClassLoader createUserCodeClassloader() throws Exception {
        long startDownloadTime = System.currentTimeMillis();

        // triggers the download of all missing jar files from the job manager
        final UserCodeClassLoader userCodeClassLoader =
                // 该类加载器会从特殊的路径加载类
                classLoaderHandle.getOrResolveClassLoader(requiredJarFiles, requiredClasspaths);

        LOG.debug(
                "Getting user code class loader for task {} at library cache manager took {} milliseconds",
                executionId,
                System.currentTimeMillis() - startDownloadTime);

        return userCodeClassLoader;
    }

    /**
     * 通知TM 当前task进入finish状态
     */
    private void notifyFinalState() {
        checkState(executionState.isTerminal());
        taskManagerActions.updateTaskExecutionState(
                new TaskExecutionState(executionId, executionState, failureCause));
    }

    private void notifyFatalError(String message, Throwable cause) {
        taskManagerActions.notifyFatalError(message, cause);
    }

    /**
     * Try to transition the execution state from the current state to the new state.
     *
     * @param currentState of the execution
     * @param newState of the execution
     * @return true if the transition was successful, otherwise false
     */
    private boolean transitionState(ExecutionState currentState, ExecutionState newState) {
        return transitionState(currentState, newState, null);
    }

    /**
     * Try to transition the execution state from the current state to the new state.
     *
     * @param currentState of the execution
     * @param newState of the execution
     * @param cause of the transition change or null
     * @return true if the transition was successful, otherwise false
     *
     * 进行状态切换
     */
    private boolean transitionState(
            ExecutionState currentState, ExecutionState newState, Throwable cause) {
        if (STATE_UPDATER.compareAndSet(this, currentState, newState)) {
            if (cause == null) {
                LOG.info(
                        "{} ({}) switched from {} to {}.",
                        taskNameWithSubtask,
                        executionId,
                        currentState,
                        newState);
            } else if (ExceptionUtils.findThrowable(cause, CancelTaskException.class).isPresent()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "{} ({}) switched from {} to {} due to CancelTaskException:",
                            taskNameWithSubtask,
                            executionId,
                            currentState,
                            newState,
                            cause);
                } else {
                    LOG.info(
                            "{} ({}) switched from {} to {} due to CancelTaskException.",
                            taskNameWithSubtask,
                            executionId,
                            currentState,
                            newState);
                }
            } else {
                // proper failure of the task. record the exception as the root
                // cause
                failureCause = cause;
                LOG.warn(
                        "{} ({}) switched from {} to {} with failure cause:",
                        taskNameWithSubtask,
                        executionId,
                        currentState,
                        newState,
                        cause);
            }

            return true;
        } else {
            return false;
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    //  Canceling / Failing the task from the outside
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Cancels the task execution. If the task is already in a terminal state (such as FINISHED,
     * CANCELED, FAILED), or if the task is already canceling this does nothing. Otherwise it sets
     * the state to CANCELING, and, if the invokable code is running, starts an asynchronous thread
     * that aborts that code.
     *
     * <p>This method never blocks.
     * 取消执行
     */
    public void cancelExecution() {
        LOG.info("Attempting to cancel task {} ({}).", taskNameWithSubtask, executionId);
        cancelOrFailAndCancelInvokable(ExecutionState.CANCELING, null);
    }

    /**
     * Marks task execution failed for an external reason (a reason other than the task code itself
     * throwing an exception). If the task is already in a terminal state (such as FINISHED,
     * CANCELED, FAILED), or if the task is already canceling this does nothing. Otherwise it sets
     * the state to FAILED, and, if the invokable code is running, starts an asynchronous thread
     * that aborts that code.
     *
     * <p>This method never blocks.
     */
    @Override
    public void failExternally(Throwable cause) {
        LOG.info("Attempting to fail task externally {} ({}).", taskNameWithSubtask, executionId);
        cancelOrFailAndCancelInvokable(ExecutionState.FAILED, cause);
    }

    /**
     * 取消或者让执行失败
     * @param targetState
     * @param cause
     */
    private void cancelOrFailAndCancelInvokable(ExecutionState targetState, Throwable cause) {
        try {
            cancelOrFailAndCancelInvokableInternal(targetState, cause);
        } catch (Throwable t) {
            if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(t)) {
                String message =
                        String.format(
                                "FATAL - exception in cancelling task %s (%s).",
                                taskNameWithSubtask, executionId);
                notifyFatalError(message, t);
            } else {
                throw t;
            }
        }
    }

    /**
     * 取消或者让执行失败
     * @param targetState
     * @param cause
     */
    @VisibleForTesting
    void cancelOrFailAndCancelInvokableInternal(ExecutionState targetState, Throwable cause) {
        cause = preProcessException(cause);

        while (true) {
            ExecutionState current = executionState;

            // if the task is already canceled (or canceling) or finished or failed,
            // then we need not do anything
            if (current.isTerminal() || current == ExecutionState.CANCELING) {
                LOG.info("Task {} is already in state {}", taskNameWithSubtask, current);
                return;
            }

            if (current == ExecutionState.DEPLOYING || current == ExecutionState.CREATED) {
                if (transitionState(current, targetState, cause)) {
                    // if we manage this state transition, then the invokable gets never called
                    // we need not call cancel on it
                    return;
                }
            } else if (current == ExecutionState.INITIALIZING
                    || current == ExecutionState.RUNNING) {
                if (transitionState(current, targetState, cause)) {
                    // we are canceling / failing out of the running state
                    // we need to cancel the invokable

                    // copy reference to guard against concurrent null-ing out the reference
                    final TaskInvokable invokable = this.invokable;

                    if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
                        LOG.info(
                                "Triggering cancellation of task code {} ({}).",
                                taskNameWithSubtask,
                                executionId);

                        // because the canceling may block on user code, we cancel from a separate
                        // thread
                        // we do not reuse the async call handler, because that one may be blocked,
                        // in which
                        // case the canceling could not continue

                        // The canceller calls cancel and interrupts the executing thread once
                        // 产生一个取消对象
                        Runnable canceler =
                                new TaskCanceler(
                                        LOG, invokable, executingThread, taskNameWithSubtask);

                        Thread cancelThread =
                                new Thread(
                                        executingThread.getThreadGroup(),
                                        canceler,
                                        String.format(
                                                "Canceler for %s (%s).",
                                                taskNameWithSubtask, executionId));
                        cancelThread.setDaemon(true);
                        cancelThread.setUncaughtExceptionHandler(
                                FatalExitExceptionHandler.INSTANCE);
                        cancelThread.start();

                        // the periodic interrupting thread - a different thread than the canceller,
                        // in case
                        // the application code does blocking stuff in its cancellation paths.
                        Runnable interrupter =
                                new TaskInterrupter(
                                        LOG,
                                        invokable,
                                        executingThread,
                                        taskNameWithSubtask,
                                        taskCancellationInterval);

                        Thread interruptingThread =
                                new Thread(
                                        executingThread.getThreadGroup(),
                                        interrupter,
                                        String.format(
                                                "Canceler/Interrupts for %s (%s).",
                                                taskNameWithSubtask, executionId));
                        interruptingThread.setDaemon(true);
                        interruptingThread.setUncaughtExceptionHandler(
                                FatalExitExceptionHandler.INSTANCE);
                        interruptingThread.start();

                        // if a cancellation timeout is set, the watchdog thread kills the process
                        // if graceful cancellation does not succeed
                        // 如果取消有时间
                        if (taskCancellationTimeout > 0) {
                            Runnable cancelWatchdog =
                                    new TaskCancelerWatchDog(
                                            taskInfo,
                                            executingThread,
                                            taskManagerActions,
                                            taskCancellationTimeout);

                            Thread watchDogThread =
                                    new Thread(
                                            executingThread.getThreadGroup(),
                                            cancelWatchdog,
                                            String.format(
                                                    "Cancellation Watchdog for %s (%s).",
                                                    taskNameWithSubtask, executionId));
                            watchDogThread.setDaemon(true);
                            watchDogThread.setUncaughtExceptionHandler(
                                    FatalExitExceptionHandler.INSTANCE);
                            watchDogThread.start();
                        }
                    }
                    return;
                }
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Unexpected state: %s of task %s (%s).",
                                current, taskNameWithSubtask, executionId));
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Partition State Listeners
    // ------------------------------------------------------------------------

    /**
     * 查看中间结果集某个分区的状态
     * @param intermediateDataSetId ID of the parent intermediate data set.
     * @param resultPartitionId ID of the result partition to check. This identifies the producing
     *     execution and partition.
     * @param responseConsumer consumer for the response handle.  用于处理查询的结果
     */
    @Override
    public void requestPartitionProducerState(
            final IntermediateDataSetID intermediateDataSetId,
            final ResultPartitionID resultPartitionId,
            Consumer<? super ResponseHandle> responseConsumer) {

        // 会转发给 jobMaster
        final CompletableFuture<ExecutionState> futurePartitionState =
                partitionProducerStateChecker.requestPartitionProducerState(
                        jobId, intermediateDataSetId, resultPartitionId);

        FutureUtils.assertNoException(
                futurePartitionState
                        .handle(PartitionProducerStateResponseHandle::new)
                        .thenAcceptAsync(responseConsumer, executor));
    }

    // ------------------------------------------------------------------------
    //  Notifications on the invokable
    // ------------------------------------------------------------------------

    /**
     * Calls the invokable to trigger a checkpoint.
     *
     * @param checkpointID The ID identifying the checkpoint.
     * @param checkpointTimestamp The timestamp associated with the checkpoint.
     * @param checkpointOptions Options for performing this checkpoint.
     *                          让其产生一个检查点
     */
    public void triggerCheckpointBarrier(
            final long checkpointID,
            final long checkpointTimestamp,
            final CheckpointOptions checkpointOptions) {

        final TaskInvokable invokable = this.invokable;

        // 根据相关信息产生元数据
        final CheckpointMetaData checkpointMetaData =
                new CheckpointMetaData(
                        checkpointID, checkpointTimestamp, System.currentTimeMillis());

        if (executionState == ExecutionState.RUNNING) {
            checkState(invokable instanceof CheckpointableTask, "invokable is not checkpointable");
            try {
                // 产生检查点
                ((CheckpointableTask) invokable)
                        .triggerCheckpointAsync(checkpointMetaData, checkpointOptions)
                        .handle(
                                (triggerResult, exception) -> {
                                    if (exception != null || !triggerResult) {
                                        // 表示产生失败
                                        declineCheckpoint(
                                                checkpointID,
                                                CheckpointFailureReason.TASK_FAILURE,
                                                exception);
                                        return false;
                                    }
                                    return true;
                                });
            } catch (RejectedExecutionException ex) {
                // This may happen if the mailbox is closed. It means that the task is shutting
                // down, so we just ignore it.
                LOG.debug(
                        "Triggering checkpoint {} for {} ({}) was rejected by the mailbox",
                        checkpointID,
                        taskNameWithSubtask,
                        executionId);
                declineCheckpoint(
                        checkpointID, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING);
            } catch (Throwable t) {
                if (getExecutionState() == ExecutionState.RUNNING) {
                    failExternally(
                            new Exception(
                                    "Error while triggering checkpoint "
                                            + checkpointID
                                            + " for "
                                            + taskNameWithSubtask,
                                    t));
                } else {
                    LOG.debug(
                            "Encountered error while triggering checkpoint {} for "
                                    + "{} ({}) while being not in state running.",
                            checkpointID,
                            taskNameWithSubtask,
                            executionId,
                            t);
                }
            }
        } else {
            LOG.debug(
                    "Declining checkpoint request for non-running task {} ({}).",
                    taskNameWithSubtask,
                    executionId);

            // send back a message that we did not do the checkpoint
            // 只有在running状态 才能产生检查点  所以这里要返回失败
            declineCheckpoint(
                    checkpointID, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY);
        }
    }

    private void declineCheckpoint(long checkpointID, CheckpointFailureReason failureReason) {
        declineCheckpoint(checkpointID, failureReason, null);
    }

    private void declineCheckpoint(
            long checkpointID,
            CheckpointFailureReason failureReason,
            @Nullable Throwable failureCause) {
        checkpointResponder.declineCheckpoint(
                jobId,
                executionId,
                checkpointID,
                new CheckpointException(
                        "Task name with subtask : " + taskNameWithSubtask,
                        failureReason,
                        failureCause));
    }

    public void notifyCheckpointComplete(final long checkpointID) {
        notifyCheckpoint(
                checkpointID,
                CheckpointStoreUtil.INVALID_CHECKPOINT_ID,
                NotifyCheckpointOperation.COMPLETE);
    }

    public void notifyCheckpointAborted(
            final long checkpointID, final long latestCompletedCheckpointId) {
        notifyCheckpoint(
                checkpointID, latestCompletedCheckpointId, NotifyCheckpointOperation.ABORT);
    }

    public void notifyCheckpointSubsumed(long checkpointID) {
        notifyCheckpoint(
                checkpointID,
                CheckpointStoreUtil.INVALID_CHECKPOINT_ID,
                NotifyCheckpointOperation.SUBSUME);
    }

    /**
     * 通知检查点的3种情况
     * @param checkpointId
     * @param latestCompletedCheckpointId
     * @param notifyCheckpointOperation
     */
    private void notifyCheckpoint(
            long checkpointId,
            long latestCompletedCheckpointId,
            NotifyCheckpointOperation notifyCheckpointOperation) {
        TaskInvokable invokable = this.invokable;

        if (executionState == ExecutionState.RUNNING && invokable != null) {
            checkState(invokable instanceof CheckpointableTask, "invokable is not checkpointable");
            try {
                // 3种情况执行不同的钩子
                switch (notifyCheckpointOperation) {
                    case ABORT:
                        ((CheckpointableTask) invokable)
                                .notifyCheckpointAbortAsync(
                                        checkpointId, latestCompletedCheckpointId);
                        break;
                    case COMPLETE:
                        ((CheckpointableTask) invokable)
                                .notifyCheckpointCompleteAsync(checkpointId);
                        break;
                    case SUBSUME:
                        ((CheckpointableTask) invokable)
                                .notifyCheckpointSubsumedAsync(checkpointId);
                }
            } catch (RejectedExecutionException ex) {
                // This may happen if the mailbox is closed. It means that the task is shutting
                // down, so we just ignore it.
                LOG.debug(
                        "Notify checkpoint {}} {} for {} ({}) was rejected by the mailbox.",
                        notifyCheckpointOperation,
                        checkpointId,
                        taskNameWithSubtask,
                        executionId);
            } catch (Throwable t) {
                switch (notifyCheckpointOperation) {
                    case ABORT:
                    case COMPLETE:
                        if (getExecutionState() == ExecutionState.RUNNING) {
                            failExternally(
                                    new RuntimeException(
                                            String.format(
                                                    "Error while notify checkpoint %s.",
                                                    notifyCheckpointOperation),
                                            t));
                        }
                        break;
                    case SUBSUME:
                        // just rethrow the throwable out as we do not expect notification of
                        // subsume could fail the task.
                        ExceptionUtils.rethrow(t);
                }
            }
        } else {
            LOG.info(
                    "Ignoring checkpoint {} notification for non-running task {}.",
                    notifyCheckpointOperation,
                    taskNameWithSubtask);
        }
    }

    /**
     * Dispatches an operator event to the invokable task.
     *
     * <p>If the event delivery did not succeed, this method throws an exception. Callers can use
     * that exception for error reporting, but need not react with failing this task (this method
     * takes care of that).
     *
     * @throws FlinkException This method throws exceptions indicating the reason why delivery did
     *     not succeed.
     *     将一些事件投递给 operator
     */
    public void deliverOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> evt)
            throws FlinkException {
        final TaskInvokable invokable = this.invokable;
        final ExecutionState currentState = this.executionState;

        // 必须在 invokable 还在运行的时候
        if (invokable == null
                || (currentState != ExecutionState.RUNNING
                        && currentState != ExecutionState.INITIALIZING)) {
            throw new TaskNotRunningException("Task is not running, but in state " + currentState);
        }

        if (invokable instanceof CoordinatedTask) {
            try {
                // 投递事件
                ((CoordinatedTask) invokable).dispatchOperatorEvent(operator, evt);
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                if (getExecutionState() == ExecutionState.RUNNING
                        || getExecutionState() == ExecutionState.INITIALIZING) {
                    FlinkException e = new FlinkException("Error while handling operator event", t);
                    failExternally(e);
                    throw e;
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private void cancelInvokable(TaskInvokable invokable) {
        // in case of an exception during execution, we still call "cancel()" on the task
        if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
            try {
                invokable.cancel();
            } catch (Throwable t) {
                LOG.error("Error while canceling task {}.", taskNameWithSubtask, t);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("%s (%s) [%s]", taskNameWithSubtask, executionId, executionState);
    }

    @VisibleForTesting
    class PartitionProducerStateResponseHandle implements ResponseHandle {
        private final Either<ExecutionState, Throwable> result;

        PartitionProducerStateResponseHandle(
                @Nullable ExecutionState producerState, @Nullable Throwable t) {
            this.result = producerState != null ? Either.Left(producerState) : Either.Right(t);
        }

        @Override
        public ExecutionState getConsumerExecutionState() {
            return executionState;
        }

        @Override
        public Either<ExecutionState, Throwable> getProducerExecutionState() {
            return result;
        }

        @Override
        public void cancelConsumption() {
            cancelExecution();
        }

        @Override
        public void failConsumption(Throwable cause) {
            failExternally(cause);
        }
    }

    /**
     * Instantiates the given task invokable class, passing the given environment (and possibly the
     * initial task state) to the task's constructor.
     *
     * <p>The method will first try to instantiate the task via a constructor accepting both the
     * Environment and the TaskStateSnapshot. If no such constructor exists, and there is no initial
     * state, the method will fall back to the stateless convenience constructor that accepts only
     * the Environment.
     *
     * @param classLoader The classloader to load the class through.
     * @param className The name of the class to load.
     * @param environment The task environment.
     * @return The instantiated invokable task object.
     * @throws Throwable Forwards all exceptions that happen during initialization of the task. Also
     *     throws an exception if the task class misses the necessary constructor.
     *     就是类加载那套
     */
    private static TaskInvokable loadAndInstantiateInvokable(
            ClassLoader classLoader, String className, Environment environment) throws Throwable {

        final Class<? extends TaskInvokable> invokableClass;
        try {
            invokableClass =
                    Class.forName(className, true, classLoader).asSubclass(TaskInvokable.class);
        } catch (Throwable t) {
            throw new Exception("Could not load the task's invokable class.", t);
        }

        Constructor<? extends TaskInvokable> statelessCtor;

        try {
            statelessCtor = invokableClass.getConstructor(Environment.class);
        } catch (NoSuchMethodException ee) {
            throw new FlinkException("Task misses proper constructor", ee);
        }

        // instantiate the class
        try {
            //noinspection ConstantConditions  --> cannot happen
            return statelessCtor.newInstance(environment);
        } catch (InvocationTargetException e) {
            // directly forward exceptions from the eager initialization
            throw e.getTargetException();
        } catch (Exception e) {
            throw new FlinkException("Could not instantiate the task's invokable class.", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Task cancellation
    //
    //  The task cancellation uses in total three threads, as a safety net
    //  against various forms of user- and JVM bugs.
    //
    //    - The first thread calls 'cancel()' on the invokable and closes
    //      the input and output connections, for fast thread termination
    //    - The second thread periodically interrupts the invokable in order
    //      to pull the thread out of blocking wait and I/O operations
    //    - The third thread (watchdog thread) waits until the cancellation
    //      timeout and then performs a hard cancel (kill process, or let
    //      the TaskManager know)
    //
    //  Previously, thread two and three were in one thread, but we needed
    //  to separate this to make sure the watchdog thread does not call
    //  'interrupt()'. This is a workaround for the following JVM bug
    //   https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8138622
    // ------------------------------------------------------------------------

    /**
     * This runner calls cancel() on the invokable, closes input-/output resources, and initially
     * interrupts the task thread.
     * 该对象用于取消invokable
     */
    private class TaskCanceler implements Runnable {

        private final Logger logger;
        private final TaskInvokable invokable;
        private final Thread executor;
        private final String taskName;

        TaskCanceler(Logger logger, TaskInvokable invokable, Thread executor, String taskName) {
            this.logger = logger;
            this.invokable = invokable;
            this.executor = executor;
            this.taskName = taskName;
        }

        @Override
        public void run() {
            try {
                // the user-defined cancel method may throw errors.
                // we need do continue despite that
                try {
                    invokable.cancel();
                } catch (Throwable t) {
                    ExceptionUtils.rethrowIfFatalError(t);
                    logger.error("Error while canceling the task {}.", taskName, t);
                }

                // Early release of input and output buffer pools. We do this
                // in order to unblock async Threads, which produce/consume the
                // intermediate streams outside of the main Task Thread (like
                // the Kafka consumer).
                // Notes: 1) This does not mean to release all network resources,
                // the task thread itself will release them; 2) We can not close
                // ResultPartitions here because of possible race conditions with
                // Task thread so we just call the fail here.
                failAllResultPartitions();
                closeAllInputGates();

                invokable.maybeInterruptOnCancel(executor, null, null);
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                logger.error("Error in the task canceler for task {}.", taskName, t);
            }
        }
    }

    /** This thread sends the delayed, periodic interrupt calls to the executing thread. */
    private static final class TaskInterrupter implements Runnable {

        /** The logger to report on the fatal condition. */
        private final Logger log;

        /** The invokable task. */
        private final TaskInvokable task;

        /** The executing task thread that we wait for to terminate. */
        private final Thread executorThread;

        /** The name of the task, for logging purposes. */
        private final String taskName;

        /** The interval in which we interrupt. */
        private final long interruptIntervalMillis;

        TaskInterrupter(
                Logger log,
                TaskInvokable task,
                Thread executorThread,
                String taskName,
                long interruptIntervalMillis) {

            this.log = log;
            this.task = task;
            this.executorThread = executorThread;
            this.taskName = taskName;
            this.interruptIntervalMillis = interruptIntervalMillis;
        }

        @Override
        public void run() {
            try {
                // we initially wait for one interval
                // in most cases, the threads go away immediately (by the cancellation thread)
                // and we need not actually do anything
                executorThread.join(interruptIntervalMillis);

                // log stack trace where the executing thread is stuck and
                // interrupt the running thread periodically while it is still alive
                while (executorThread.isAlive()) {
                    task.maybeInterruptOnCancel(executorThread, taskName, interruptIntervalMillis);
                    try {
                        executorThread.join(interruptIntervalMillis);
                    } catch (InterruptedException e) {
                        // we ignore this and fall through the loop
                    }
                }
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                log.error("Error in the task canceler for task {}.", taskName, t);
            }
        }
    }

    /**
     * Watchdog for the cancellation. If the task thread does not go away gracefully within a
     * certain time, we trigger a hard cancel action (notify TaskManager of fatal error, which in
     * turn kills the process).
     * 检查在一定时间后任务是否被取消
     */
    private static class TaskCancelerWatchDog implements Runnable {

        /** The executing task thread that we wait for to terminate. */
        private final Thread executorThread;

        /** The TaskManager to notify if cancellation does not happen in time. */
        private final TaskManagerActions taskManager;

        /** The timeout for cancellation. */
        private final long timeoutMillis;

        private final TaskInfo taskInfo;

        TaskCancelerWatchDog(
                TaskInfo taskInfo,
                Thread executorThread,
                TaskManagerActions taskManager,
                long timeoutMillis) {

            checkArgument(timeoutMillis > 0);

            this.taskInfo = taskInfo;
            this.executorThread = executorThread;
            this.taskManager = taskManager;
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public void run() {
            try {
                Deadline timeout = Deadline.fromNow(Duration.ofMillis(timeoutMillis));
                while (executorThread.isAlive() && timeout.hasTimeLeft()) {
                    try {
                        executorThread.join(Math.max(1, timeout.timeLeft().toMillis()));
                    } catch (InterruptedException ignored) {
                        // we don't react to interrupted exceptions, simply fall through the loop
                    }
                }

                if (executorThread.isAlive()) {
                    logTaskThreadStackTrace(
                            executorThread,
                            taskInfo.getTaskNameWithSubtasks(),
                            timeoutMillis,
                            "notifying TM");
                    String msg =
                            "Task did not exit gracefully within "
                                    + (timeoutMillis / 1000)
                                    + " + seconds.";
                    taskManager.notifyFatalError(msg, new FlinkRuntimeException(msg));
                }
            } catch (Throwable t) {
                throw new FlinkRuntimeException("Error in Task Cancellation Watch Dog", t);
            }
        }
    }

    public static void logTaskThreadStackTrace(
            Thread thread, String taskName, long timeoutMs, String action) {
        StackTraceElement[] stack = thread.getStackTrace();
        StringBuilder stackTraceStr = new StringBuilder();
        for (StackTraceElement e : stack) {
            stackTraceStr.append(e).append('\n');
        }

        LOG.warn(
                "Task '{}' did not react to cancelling signal - {}; it is stuck for {} seconds in method:\n {}",
                taskName,
                action,
                timeoutMs / 1000,
                stackTraceStr);
    }

    /** Various operation of notify checkpoint. */
    public enum NotifyCheckpointOperation {
        ABORT,
        COMPLETE,
        SUBSUME
    }
}
