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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.blocklist.BlocklistListener;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** {@link JobMaster} rpc gateway interface.
 * 通过网关与JobMaster交互
 * */
public interface JobMasterGateway
        extends CheckpointCoordinatorGateway,
                FencedRpcGateway<JobMasterId>,  // 表示访问需要token
                KvStateLocationOracle,  // 表示可以通过jobId和 kvState名字去查询 keyState的位置
                KvStateRegistryGateway,  // 通过与KvStateRegistry通信  来注册和注销 KvState
                JobMasterOperatorEventGateway,  // 表示本对象还可以与协调者交互
                BlocklistListener {  // 感知集群中被阻塞的节点

    /**
     * Cancels the currently executed job.
     *
     * @param timeout of this operation
     * @return Future acknowledge of the operation
     */
    CompletableFuture<Acknowledge> cancel(@RpcTimeout Time timeout);

    /**
     * Updates the task execution state for a given task.
     *
     * @param taskExecutionState New task execution state for a given task   此时最新的状态
     * @return Future flag of the task execution state update result
     * 更新运行的某个execution   execution相当于是任务的载体 可以进行计算
     */
    CompletableFuture<Acknowledge> updateTaskExecutionState(
            final TaskExecutionState taskExecutionState);

    /**
     * Requests the next input split for the {@link ExecutionJobVertex}. The next input split is
     * sent back to the sender as a {@link SerializedInputSplit} message.
     *
     * @param vertexID The job vertex id
     * @param executionAttempt The execution attempt id
     * @return The future of the input split. If there is no further input split, will return an
     *     empty object.
     *     请求下一个输入流
     */
    CompletableFuture<SerializedInputSplit> requestNextInputSplit(
            final JobVertexID vertexID, final ExecutionAttemptID executionAttempt);

    /**
     * Requests the current state of the partition. The state of a partition is currently bound to
     * the state of the producing execution.
     *
     * @param intermediateResultId The execution attempt ID of the task requesting the partition  这表示一个结果
     *     state.
     * @param partitionId The partition ID of the partition to request the state of.   被请求的分区
     * @return The future of the partition state
     * 请求下一个分区状态
     */
    CompletableFuture<ExecutionState> requestPartitionState(
            final IntermediateDataSetID intermediateResultId, final ResultPartitionID partitionId);

    /**
     * Disconnects the given {@link org.apache.flink.runtime.taskexecutor.TaskExecutor} from the
     * {@link JobMaster}.
     *
     * @param resourceID identifying the TaskManager to disconnect
     * @param cause for the disconnection of the TaskManager
     * @return Future acknowledge once the JobMaster has been disconnected from the TaskManager
     * 断开与某个任务管理器的连接
     * 看来 JobMaster是需要和 TaskManager保持通信的
     */
    CompletableFuture<Acknowledge> disconnectTaskManager(ResourceID resourceID, Exception cause);

    /**
     * Disconnects the resource manager from the job manager because of the given cause.
     *
     * @param resourceManagerId identifying the resource manager leader id
     * @param cause of the disconnect
     *              通知JM 与 RM断开连接
     */
    void disconnectResourceManager(
            final ResourceManagerId resourceManagerId, final Exception cause);

    /**
     * Offers the given slots to the job manager. The response contains the set of accepted slots.
     *
     * @param taskManagerId identifying the task manager
     * @param slots to offer to the job manager
     * @param timeout for the rpc call
     * @return Future set of accepted slots.
     * 往某个地方提供一组资源
     */
    CompletableFuture<Collection<SlotOffer>> offerSlots(
            final ResourceID taskManagerId,
            final Collection<SlotOffer> slots,
            @RpcTimeout final Time timeout);

    /**
     * Fails the slot with the given allocation id and cause.
     *
     * @param taskManagerId identifying the task manager
     * @param allocationId identifying the slot to fail
     * @param cause of the failing
     *              释放slot
     */
    void failSlot(
            final ResourceID taskManagerId, final AllocationID allocationId, final Exception cause);

    /**
     * Registers the task manager at the job manager.
     *
     * @param jobId jobId specifying the job for which the JobMaster should be responsible
     * @param taskManagerRegistrationInformation the information for registering a task manager at
     *     the job manager   这里有 TM的地址信息
     * @param timeout for the rpc call
     * @return Future registration response indicating whether the registration was successful or
     *     not
     *     在JobMaster上注册一个 taskManager
     */
    CompletableFuture<RegistrationResponse> registerTaskManager(
            final JobID jobId,
            final TaskManagerRegistrationInformation taskManagerRegistrationInformation,
            @RpcTimeout final Time timeout);

    /**
     * Sends the heartbeat to job manager from task manager.
     *
     * @param resourceID unique id of the task manager
     * @param payload report payload
     * @return future which is completed exceptionally if the operation fails
     * 作为TM 还需要发送心跳信息到 JM
     */
    CompletableFuture<Void> heartbeatFromTaskManager(
            final ResourceID resourceID, final TaskExecutorToJobManagerHeartbeatPayload payload);

    /**
     * Sends heartbeat request from the resource manager.
     *
     * @param resourceID unique id of the resource manager
     * @return future which is completed exceptionally if the operation fails
     * RM发送心跳到JM
     */
    CompletableFuture<Void> heartbeatFromResourceManager(final ResourceID resourceID);

    /**
     * Request the details of the executed job.
     *
     * @param timeout for the rpc call
     * @return Future details of the executed job
     * 获取当前在运行的job的详细信息
     */
    CompletableFuture<JobDetails> requestJobDetails(@RpcTimeout Time timeout);

    /**
     * Requests the current job status.
     *
     * @param timeout for the rpc call
     * @return Future containing the current job status
     * 获取此时Job的状态
     */
    CompletableFuture<JobStatus> requestJobStatus(@RpcTimeout Time timeout);

    /**
     * Requests the {@link ExecutionGraphInfo} of the executed job.
     *
     * @param timeout for the rpc call
     * @return Future which is completed with the {@link ExecutionGraphInfo} of the executed job
     * 获取Job的执行图
     */
    CompletableFuture<ExecutionGraphInfo> requestJob(@RpcTimeout Time timeout);

    /**
     * Requests the {@link CheckpointStatsSnapshot} of the job.
     *
     * @param timeout for the rpc call
     * @return Future which is completed with the {@link CheckpointStatsSnapshot} of the job
     * 获取检查点统计信息
     */
    CompletableFuture<CheckpointStatsSnapshot> requestCheckpointStats(@RpcTimeout Time timeout);

    /**
     * Triggers taking a savepoint of the executed job.
     *
     * @param targetDirectory to which to write the savepoint data or null if the default savepoint
     *     directory should be used
     * @param formatType binary format for the savepoint
     * @param timeout for the rpc call
     * @return Future which is completed with the savepoint path once completed
     * 触发保存点 应该是要通知到各 Execution
     */
    CompletableFuture<String> triggerSavepoint(
            @Nullable final String targetDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType,
            @RpcTimeout final Time timeout);

    /**
     * Triggers taking a checkpoint of the executed job.
     *
     * @param checkpointType to determine how checkpoint should be taken
     * @param timeout for the rpc call
     * @return Future which is completed with the CompletedCheckpoint once completed
     * 通知各节点创建检查点
     */
    CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            final CheckpointType checkpointType, @RpcTimeout final Time timeout);

    /**
     * Triggers taking a checkpoint of the executed job.
     *
     * @param timeout for the rpc call
     * @return Future which is completed with the checkpoint path once completed
     */
    default CompletableFuture<String> triggerCheckpoint(@RpcTimeout final Time timeout) {
        return triggerCheckpoint(CheckpointType.DEFAULT, timeout)
                .thenApply(CompletedCheckpoint::getExternalPointer);  // 返回存储检查点数据的指针
    }

    /**
     * Stops the job with a savepoint.
     *
     * @param targetDirectory to which to write the savepoint data or null if the default savepoint
     *     directory should be used
     * @param terminate flag indicating if the job should terminate or just suspend
     * @param timeout for the rpc call
     * @return Future which is completed with the savepoint path once completed
     * 停止某个保存点
     */
    CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final SavepointFormatType formatType,
            final boolean terminate,
            @RpcTimeout final Time timeout);

    /**
     * Notifies that not enough resources are available to fulfill the resource requirements of a
     * job.
     *
     * @param acquiredResources the resources that have been acquired for the job  这是需要的资源
     *                          通知此时没有足够的资源了
     */
    void notifyNotEnoughResourcesAvailable(Collection<ResourceRequirement> acquiredResources);

    /**
     * Update the aggregate and return the new value.
     *
     * @param aggregateName The name of the aggregate to update
     * @param aggregand The value to add to the aggregate
     * @param serializedAggregationFunction The function to apply to the current aggregate and
     *     aggregand to obtain the new aggregate value, this should be of type {@link
     *     AggregateFunction}    被序列化后的函数对象
     * @return The updated aggregate
     * 更新一个聚合值
     */
    CompletableFuture<Object> updateGlobalAggregate(
            String aggregateName, Object aggregand, byte[] serializedAggregationFunction);

    /**
     * Deliver a coordination request to a specified coordinator and return the response.
     *
     * @param operatorId identifying the coordinator to receive the request
     * @param serializedRequest serialized request to deliver
     * @return A future containing the response. The response will fail with a {@link
     *     org.apache.flink.util.FlinkException} if the task is not running, or no
     *     operator/coordinator exists for the given ID, or the coordinator cannot handle client
     *     events.
     *     将一个协调者请求  发送给协调者
     */
    CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            @RpcTimeout Time timeout);

    /**
     * Notifies the {@link org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker}
     * to stop tracking the target result partitions and release the locally occupied resources on
     * {@link org.apache.flink.runtime.taskexecutor.TaskExecutor}s if any.
     * 表示不再追踪某些分区结果   本身应该是要跟踪 查看结果集是否成功发送到某个分区上吧
     */
    CompletableFuture<?> stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> partitionIds);

    /**
     * Read current {@link JobResourceRequirements job resource requirements}.
     *
     * @return Future which that contains current resource requirements.
     * 获取整个job需要的资源
     */
    CompletableFuture<JobResourceRequirements> requestJobResourceRequirements();

    /**
     * Update {@link JobResourceRequirements job resource requirements}.
     *
     * @param jobResourceRequirements new resource requirements
     * @return Future which is completed successfully when requirements are updated
     * 更新整个job需要的资源
     */
    CompletableFuture<Acknowledge> updateJobResourceRequirements(
            JobResourceRequirements jobResourceRequirements);

    /**
     * Notifies that the task has reached the end of data.
     *
     * @param executionAttempt The execution attempt id.
     *                         表示数据处理完了
     */
    void notifyEndOfData(final ExecutionAttemptID executionAttempt);
}
