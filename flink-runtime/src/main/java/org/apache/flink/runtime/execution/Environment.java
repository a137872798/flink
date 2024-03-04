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

package org.apache.flink.runtime.execution;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * The Environment gives the code executed in a task access to the task's properties (such as name,
 * parallelism), the configurations, the data stream readers and writers, as well as the various
 * components that are provided by the TaskManager, such as memory manager, I/O manager, ...
 *
 * 环境对象 可以从这里获取很多高层面的东西
 */
public interface Environment {

    /**
     * Returns the job specific {@link ExecutionConfig}.
     *
     * @return The execution configuration associated with the current job.
     * 获取执行时配置
     */
    ExecutionConfig getExecutionConfig();

    /**
     * Returns the ID of the job that the task belongs to.
     *
     * @return the ID of the job from the original job graph
     * 所属的 job图的id
     */
    JobID getJobID();

    /**
     * Gets the ID of the JobVertex for which this task executes a parallel subtask.
     *
     * @return The JobVertexID of this task.
     * 获取当前task的顶点id
     */
    JobVertexID getJobVertexId();

    /**
     * Gets the ID of the task execution attempt.
     *
     * @return The ID of the task execution attempt.
     * 这个是执行id
     */
    ExecutionAttemptID getExecutionId();

    /**
     * Returns the task-wide configuration object, originally attached to the job vertex.
     *
     * @return The task-wide configuration
     */
    Configuration getTaskConfiguration();

    /**
     * Gets the task manager info, with configuration and hostname.
     *
     * @return The task manager info, with configuration and hostname.
     * 获取运行时信息
     */
    TaskManagerRuntimeInfo getTaskManagerInfo();

    /**
     * Returns the task specific metric group.
     *
     * @return The MetricGroup of this task.
     * TODO
     */
    TaskMetricGroup getMetricGroup();

    /**
     * Returns the job-wide configuration object that was attached to the JobGraph.
     *
     * @return The job-wide configuration
     */
    Configuration getJobConfiguration();

    /**
     * Returns the {@link TaskInfo} object associated with this subtask
     *
     * @return TaskInfo for this subtask
     * 获取本任务信息
     */
    TaskInfo getTaskInfo();

    /**
     * Returns the input split provider assigned to this environment.
     *
     * @return The input split provider or {@code null} if no such provider has been assigned to
     *     this environment.
     *     通过该对象可以将内部的输入流拆分
     */
    InputSplitProvider getInputSplitProvider();

    /** Gets the gateway through which operators can send events to the operator coordinators.
     * 该网关对象 用于往coordinator发送operatorEvent 或者请求
     * */
    TaskOperatorEventGateway getOperatorCoordinatorEventGateway();

    /**
     * Returns the current {@link IOManager}.
     *
     * @return the current {@link IOManager}.
     * 该对象管理文件连接
     */
    IOManager getIOManager();

    /**
     * Returns the current {@link MemoryManager}.
     *
     * @return the current {@link MemoryManager}.
     * 该对象管理内存
     */
    MemoryManager getMemoryManager();

    /** @return the resources shared among all tasks of this task manager.
     * 获取一些可以被共享的资源
     * */
    SharedResources getSharedResources();

    /** Returns the user code class loader */
    UserCodeClassLoader getUserCodeClassLoader();

    /**
     * 获取分布式缓存的路径  这个缓存指的就是利用hdfs存储的数据
     * @return
     */
    Map<String, Future<Path>> getDistributedCacheEntries();

    /**
     * 用于管理广播变量
     * @return
     */
    BroadcastVariableManager getBroadcastVariableManager();

    /**
     * 可以借助环境对象拿到task状态管理器
     * @return
     */
    TaskStateManager getTaskStateManager();

    /**
     * 维护全局作用域的聚合值
     * @return
     */
    GlobalAggregateManager getGlobalAggregateManager();

    /**
     * Get the {@link ExternalResourceInfoProvider} which contains infos of available external
     * resources.
     *
     * @return {@link ExternalResourceInfoProvider} which contains infos of available external
     *     resources
     *     ExternalResourceInfoProvider 指定一个资源名 可以获取关联的一组外部资源
     */
    ExternalResourceInfoProvider getExternalResourceInfoProvider();

    /**
     * Return the registry for accumulators which are periodically sent to the job manager.
     *
     * @return the registry
     * 该对象内部维护了一组聚合值
     */
    AccumulatorRegistry getAccumulatorRegistry();

    /**
     * Returns the registry for {@link InternalKvState} instances.
     *
     * @return KvState registry
     * 用于注册本task使用到的state
     */
    TaskKvStateRegistry getTaskKvStateRegistry();

    /**
     * Confirms that the invokable has successfully completed all steps it needed to for the
     * checkpoint with the give checkpoint-ID. This method does not include any state in the
     * checkpoint.
     *
     * @param checkpointId ID of this checkpoint
     * @param checkpointMetrics metrics for this checkpoint
     *                          确认某个检查点生成完成了
     */
    void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics);

    /**
     * Confirms that the invokable has successfully completed all required steps for the checkpoint
     * with the give checkpoint-ID. This method does include the given state in the checkpoint.
     *
     * @param checkpointId ID of this checkpoint
     * @param checkpointMetrics metrics for this checkpoint
     * @param subtaskState All state handles for the checkpointed state
     */
    void acknowledgeCheckpoint(
            long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState);

    /**
     * Declines a checkpoint. This tells the checkpoint coordinator that this task will not be able
     * to successfully complete a certain checkpoint.
     *
     * @param checkpointId The ID of the declined checkpoint.
     * @param checkpointException The exception why the checkpoint was declined.    该异常描述为什么失败
     *                            拒绝检查点
     */
    void declineCheckpoint(long checkpointId, CheckpointException checkpointException);

    /**
     * Marks task execution failed for an external reason (a reason other than the task code itself
     * throwing an exception). If the task is already in a terminal state (such as FINISHED,
     * CANCELED, FAILED), or if the task is already canceling this does nothing. Otherwise it sets
     * the state to FAILED, and, if the invokable code is running, starts an asynchronous thread
     * that aborts that code.
     *
     * <p>This method never blocks.
     *
     * 表示任务执行失败了
     */
    void failExternally(Throwable cause);

    // --------------------------------------------------------------------------------------------
    //  Fields relevant to the I/O system. Should go into Task
    // --------------------------------------------------------------------------------------------

    ResultPartitionWriter getWriter(int index);

    ResultPartitionWriter[] getAllWriters();

    IndexedInputGate getInputGate(int index);

    IndexedInputGate[] getAllInputGates();

    TaskEventDispatcher getTaskEventDispatcher();

    TaskManagerActions getTaskManagerActions();

    // --------------------------------------------------------------------------------------------
    //  Fields set in the StreamTask to provide access to mailbox and other runtime resources
    // --------------------------------------------------------------------------------------------

    default void setMainMailboxExecutor(MailboxExecutor mainMailboxExecutor) {}

    default MailboxExecutor getMainMailboxExecutor() {
        throw new UnsupportedOperationException();
    }

    default void setAsyncOperationsThreadPool(ExecutorService executorService) {}

    default ExecutorService getAsyncOperationsThreadPool() {
        throw new UnsupportedOperationException();
    }

    default void setCheckpointStorageAccess(CheckpointStorageAccess checkpointStorageAccess) {}

    default CheckpointStorageAccess getCheckpointStorageAccess() {
        throw new UnsupportedOperationException();
    }

    ChannelStateWriteRequestExecutorFactory getChannelStateExecutorFactory();
}
