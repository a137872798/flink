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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistContext;
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.*;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.*;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.DataSetMetaInfo;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.resourcemanager.registration.JobManagerRegistration;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.resourcemanager.registration.WorkerRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceAllocator;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceEventListener;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.ThreadDumpInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.*;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.*;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkExpectedException;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with him remotely:
 *
 * <ul>
 *   <li>{@link #registerJobMaster(JobMasterId, ResourceID, String, JobID, Time)} registers a {@link
 *       JobMaster} at the resource manager
 * </ul>
 *
 * 资源管理器 可以作为rpc端点被访问
 */
public abstract class ResourceManager<WorkerType extends ResourceIDRetrievable>
        extends FencedRpcEndpoint<ResourceManagerId>
        implements DelegationTokenManager.Listener, ResourceManagerGateway {

    public static final String RESOURCE_MANAGER_NAME = "resourcemanager";

    /** Unique id of the resource manager.
     * 标识资源管理器的 id
     * */
    private final ResourceID resourceId;

    /** All currently registered JobMasterGateways scoped by JobID.
     * 每个id 对应一个jobMaster
     * JobManagerRegistration 中包含网关对象 可以与 JobMaster通信
     * */
    private final Map<JobID, JobManagerRegistration> jobManagerRegistrations;

    /** All currently registered JobMasterGateways scoped by ResourceID.
     * 通过 resourceId 来查询
     * */
    private final Map<ResourceID, JobManagerRegistration> jmResourceIdRegistrations;

    /** Service to retrieve the job leader ids.
     * 通过该对象 查询注册的job 对应的JobMaster leader
     * */
    private final JobLeaderIdService jobLeaderIdService;

    /** All currently registered TaskExecutors with there framework specific worker information.
     * WorkerRegistration 中包含了 TaskExecutorGateway 可以用于通信
     * */
    private final Map<ResourceID, WorkerRegistration<WorkerType>> taskExecutors;

    /** Ongoing registration of TaskExecutors per resource ID.
     * 简单维护 gateway
     * */
    private final Map<ResourceID, CompletableFuture<TaskExecutorGateway>>
            taskExecutorGatewayFutures;

    /**
     * 心跳服务对象
     */
    private final HeartbeatServices heartbeatServices;

    /** Fatal error handler. */
    private final FatalErrorHandler fatalErrorHandler;

    /** The slot manager maintains the available slots.
     * 可以以 声明式 和细粒度 2种方式 为job从 TM上申请资源并分配
     * */
    private final SlotManager slotManager;

    /**
     * 追踪分区信息
     */
    private final ResourceManagerPartitionTracker clusterPartitionTracker;

    /**
     * 包含 ip port
     */
    private final ClusterInformation clusterInformation;

    protected final ResourceManagerMetricGroup resourceManagerMetricGroup;

    protected final Executor ioExecutor;

    private final CompletableFuture<Void> startedFuture;

    // 用于与 JobMaster / TaskExecutor 维持心跳

    /** The heartbeat manager with task managers. */
    private HeartbeatManager<TaskExecutorHeartbeatPayload, Void> taskManagerHeartbeatManager;

    /** The heartbeat manager with job managers. */
    private HeartbeatManager<Void, Void> jobManagerHeartbeatManager;

    /**
     * TODO 有关权限认证的先忽略
     */
    private final DelegationTokenManager delegationTokenManager;

    /**
     * 维护慢节点 便于在一些操作时避开
     */
    protected final BlocklistHandler blocklistHandler;

    private final AtomicReference<byte[]> latestTokens = new AtomicReference<>();

    /**
     * 通过该对象进行资源分配
     */
    private final ResourceAllocator resourceAllocator;

    public ResourceManager(
            RpcService rpcService,
            UUID leaderSessionId,
            ResourceID resourceId,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            SlotManager slotManager,
            ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            JobLeaderIdService jobLeaderIdService,
            ClusterInformation clusterInformation,
            FatalErrorHandler fatalErrorHandler,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            Time rpcTimeout,
            Executor ioExecutor) {

        super(
                rpcService,
                RpcServiceUtils.createRandomName(RESOURCE_MANAGER_NAME),
                ResourceManagerId.fromUuid(leaderSessionId));

        this.resourceId = checkNotNull(resourceId);
        this.heartbeatServices = checkNotNull(heartbeatServices);
        this.slotManager = checkNotNull(slotManager);
        this.jobLeaderIdService = checkNotNull(jobLeaderIdService);
        this.clusterInformation = checkNotNull(clusterInformation);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.resourceManagerMetricGroup = checkNotNull(resourceManagerMetricGroup);

        this.jobManagerRegistrations = CollectionUtil.newHashMapWithExpectedSize(4);
        this.jmResourceIdRegistrations = CollectionUtil.newHashMapWithExpectedSize(4);
        this.taskExecutors = CollectionUtil.newHashMapWithExpectedSize(8);
        this.taskExecutorGatewayFutures = CollectionUtil.newHashMapWithExpectedSize(8);
        this.blocklistHandler =
                blocklistHandlerFactory.create(
                        new ResourceManagerBlocklistContext(),
                        this::getNodeIdOfTaskManager,
                        getMainThreadExecutor(),
                        log);

        this.jobManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
        this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

        this.clusterPartitionTracker =
                checkNotNull(clusterPartitionTrackerFactory)
                        .get(
                                // 表示当某个 TM 下线时 并且将它持有的一组中间数据集作为参数 触发该函数
                                (taskExecutorResourceId, dataSetIds) ->
                                        taskExecutors
                                                .get(taskExecutorResourceId)
                                                .getTaskExecutorGateway()
                                                .releaseClusterPartitions(dataSetIds, rpcTimeout)
                                                .exceptionally(
                                                        throwable -> {
                                                            log.debug(
                                                                    "Request for release of cluster partitions belonging to data sets {} was not successful.",
                                                                    dataSetIds,
                                                                    throwable);
                                                            throw new CompletionException(
                                                                    throwable);
                                                        }));
        this.ioExecutor = ioExecutor;

        this.startedFuture = new CompletableFuture<>();

        this.delegationTokenManager = delegationTokenManager;

        // 资源分配器   是由子类实现的
        this.resourceAllocator = getResourceAllocator();
    }

    // ------------------------------------------------------------------------
    //  RPC lifecycle methods
    // ------------------------------------------------------------------------

    /**
     * 在启动RPC服务时触发该方法
     * @throws Exception
     */
    @Override
    public final void onStart() throws Exception {
        try {
            log.info("Starting the resource manager.");
            startResourceManagerServices();
            startedFuture.complete(null);
        } catch (Throwable t) {
            final ResourceManagerException exception =
                    new ResourceManagerException(
                            String.format("Could not start the ResourceManager %s", getAddress()),
                            t);
            onFatalError(exception);
            throw exception;
        }
    }

    /**
     *
     * @throws Exception
     */
    private void startResourceManagerServices() throws Exception {
        try {
            // 启动负责监听 job 相关的 JobMaster leader 节点位置的对象
            jobLeaderIdService.start(new JobLeaderIdActionsImpl());

            // TODO
            registerMetrics();

            // 开启心跳服务  当心跳超时时会断开连接
            startHeartbeatServices();

            // 为slotManager 设置组件
            slotManager.start(
                    getFencingToken(),
                    getMainThreadExecutor(),
                    resourceAllocator,
                    new ResourceEventListenerImpl(),
                    blocklistHandler::isBlockedTaskManager);

            // TODO
            delegationTokenManager.start(this);

            initialize();
        } catch (Exception e) {
            handleStartResourceManagerServicesException(e);
        }
    }

    private void handleStartResourceManagerServicesException(Exception e) throws Exception {
        try {
            stopResourceManagerServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    /**
     * Completion of this future indicates that the resource manager is fully started and is ready
     * to serve.
     */
    public CompletableFuture<Void> getStartedFuture() {
        return startedFuture;
    }

    /**
     * rpc服务停止时触发
     * @return
     */
    @Override
    public final CompletableFuture<Void> onStop() {
        try {
            stopResourceManagerServices();
        } catch (Exception exception) {
            return FutureUtils.completedExceptionally(
                    new FlinkException(
                            "Could not properly shut down the ResourceManager.", exception));
        }

        return CompletableFuture.completedFuture(null);
    }

    /**
     * 停止本对象
     * @throws Exception
     */
    private void stopResourceManagerServices() throws Exception {
        Exception exception = null;

        try {
            terminate();
        } catch (Exception e) {
            exception =
                    new ResourceManagerException("Error while shutting down resource manager", e);
        }

        try {
            delegationTokenManager.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        stopHeartbeatServices();

        try {
            slotManager.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            jobLeaderIdService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        resourceManagerMetricGroup.close();

        clearStateInternal();

        ExceptionUtils.tryRethrowException(exception);
    }

    // ------------------------------------------------------------------------
    //  RPC methods
    // ------------------------------------------------------------------------

    /**
     * 某个 JM 通过rpc调用该方法  将自己注册上来
     * @param jobMasterId The fencing token for the JobMaster leader
     * @param jobManagerResourceId
     * @param jobManagerAddress
     * @param jobId The Job ID of the JobMaster that registers
     * @param timeout Timeout for the future to complete
     * @return
     */
    @Override
    public CompletableFuture<RegistrationResponse> registerJobMaster(
            final JobMasterId jobMasterId,
            final ResourceID jobManagerResourceId,
            final String jobManagerAddress,
            final JobID jobId,
            final Time timeout) {

        checkNotNull(jobMasterId);
        checkNotNull(jobManagerResourceId);
        checkNotNull(jobManagerAddress);
        checkNotNull(jobId);

        if (!jobLeaderIdService.containsJob(jobId)) {
            try {
                // 添加job 并找到JM leader地址
                jobLeaderIdService.addJob(jobId);
            } catch (Exception e) {
                ResourceManagerException exception =
                        new ResourceManagerException(
                                "Could not add the job " + jobId + " to the job id leader service.",
                                e);

                onFatalError(exception);

                log.error("Could not add job {} to job leader id service.", jobId, e);
                return FutureUtils.completedExceptionally(exception);
            }
        }

        log.info(
                "Registering job manager {}@{} for job {}.", jobMasterId, jobManagerAddress, jobId);

        CompletableFuture<JobMasterId> jobMasterIdFuture;

        try {
            jobMasterIdFuture = jobLeaderIdService.getLeaderId(jobId);
        } catch (Exception e) {
            // we cannot check the job leader id so let's fail
            // TODO: Maybe it's also ok to skip this check in case that we cannot check the leader
            // id
            ResourceManagerException exception =
                    new ResourceManagerException(
                            "Cannot obtain the "
                                    + "job leader id future to verify the correct job leader.",
                            e);

            onFatalError(exception);

            log.debug(
                    "Could not obtain the job leader id future to verify the correct job leader.");
            return FutureUtils.completedExceptionally(exception);
        }

        // 连接到JM
        CompletableFuture<JobMasterGateway> jobMasterGatewayFuture =
                getRpcService().connect(jobManagerAddress, jobMasterId, JobMasterGateway.class);

        CompletableFuture<RegistrationResponse> registrationResponseFuture =
                jobMasterGatewayFuture.thenCombineAsync(
                        jobMasterIdFuture,
                        (JobMasterGateway jobMasterGateway, JobMasterId leadingJobMasterId) -> {
                            // 连接成功后 进入该方法  首先确保该JM 还是leader
                            if (Objects.equals(leadingJobMasterId, jobMasterId)) {
                                // 注册JM
                                return registerJobMasterInternal(
                                        jobMasterGateway,
                                        jobId,
                                        jobManagerAddress,
                                        jobManagerResourceId);
                            } else {
                                // 通知注册失败 因为此时该JM已经不是leader了
                                final String declineMessage =
                                        String.format(
                                                "The leading JobMaster id %s did not match the received JobMaster id %s. "
                                                        + "This indicates that a JobMaster leader change has happened.",
                                                leadingJobMasterId, jobMasterId);
                                log.debug(declineMessage);
                                return new RegistrationResponse.Failure(
                                        new FlinkException(declineMessage));
                            }
                        },
                        getMainThreadExecutor());

        // handle exceptions which might have occurred in one of the futures inputs of combine
        return registrationResponseFuture.handleAsync(
                (RegistrationResponse registrationResponse, Throwable throwable) -> {
                    if (throwable != null) {
                        if (log.isDebugEnabled()) {
                            log.debug(
                                    "Registration of job manager {}@{} failed.",
                                    jobMasterId,
                                    jobManagerAddress,
                                    throwable);
                        } else {
                            log.info(
                                    "Registration of job manager {}@{} failed.",
                                    jobMasterId,
                                    jobManagerAddress);
                        }

                        return new RegistrationResponse.Failure(throwable);
                    } else {
                        return registrationResponse;
                    }
                },
                ioExecutor);
    }

    /**
     * 注册TM  也是其他组件通过gateway对象访问RM 并进行注册的
     * @param taskExecutorRegistration the task executor registration.
     * @param timeout The timeout for the response.
     * @return
     */
    @Override
    public CompletableFuture<RegistrationResponse> registerTaskExecutor(
            final TaskExecutorRegistration taskExecutorRegistration, final Time timeout) {

        // 进行连接
        CompletableFuture<TaskExecutorGateway> taskExecutorGatewayFuture =
                getRpcService()
                        .connect(
                                taskExecutorRegistration.getTaskExecutorAddress(),
                                TaskExecutorGateway.class);
        taskExecutorGatewayFutures.put(
                taskExecutorRegistration.getResourceId(), taskExecutorGatewayFuture);

        return taskExecutorGatewayFuture.handleAsync(
                (TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
                    final ResourceID resourceId = taskExecutorRegistration.getResourceId();
                    if (taskExecutorGatewayFuture == taskExecutorGatewayFutures.get(resourceId)) {
                        taskExecutorGatewayFutures.remove(resourceId);
                        if (throwable != null) {
                            return new RegistrationResponse.Failure(throwable);
                        } else {
                            return registerTaskExecutorInternal(
                                    taskExecutorGateway, taskExecutorRegistration);
                        }
                    } else {
                        log.debug(
                                "Ignoring outdated TaskExecutorGateway connection for {}.",
                                resourceId.getStringWithMetadata());
                        return new RegistrationResponse.Failure(
                                new FlinkException("Decline outdated task executor registration."));
                    }
                },
                getMainThreadExecutor());
    }

    /**
     * TM 将内部slot信息上报给RM
     * @param taskManagerResourceId
     * @param taskManagerRegistrationId id identifying the sending TaskManager
     * @param slotReport which is sent to the ResourceManager
     * @param timeout for the operation
     * @return
     */
    @Override
    public CompletableFuture<Acknowledge> sendSlotReport(
            ResourceID taskManagerResourceId,
            InstanceID taskManagerRegistrationId,
            SlotReport slotReport,
            Time timeout) {
        final WorkerRegistration<WorkerType> workerTypeWorkerRegistration =
                taskExecutors.get(taskManagerResourceId);

        // 要实例匹配
        if (workerTypeWorkerRegistration.getInstanceID().equals(taskManagerRegistrationId)) {
            // 已经存在则进行更新
            SlotManager.RegistrationResult registrationResult =
                    slotManager.registerTaskManager(
                            workerTypeWorkerRegistration,
                            slotReport,
                            workerTypeWorkerRegistration.getTotalResourceProfile(),
                            workerTypeWorkerRegistration.getDefaultSlotResourceProfile());

            // 只有首次添加 才会返回success
            if (registrationResult == SlotManager.RegistrationResult.SUCCESS) {
                WorkerResourceSpec workerResourceSpec =
                        WorkerResourceSpec.fromTotalResourceProfile(
                                workerTypeWorkerRegistration.getTotalResourceProfile(),
                                slotReport.getNumSlotStatus());
                onWorkerRegistered(workerTypeWorkerRegistration.getWorker(), workerResourceSpec);
                // 表示此时资源管理器上slot太多  返回异常 并断开连接
            } else if (registrationResult == SlotManager.RegistrationResult.REJECTED) {
                closeTaskManagerConnection(
                                taskManagerResourceId,
                                new FlinkExpectedException(
                                        "Task manager could not be registered to SlotManager."))
                        .ifPresent(ResourceManager.this::stopWorkerIfSupported);
            } else {
                // 这种就可能是更新
                log.debug("TaskManager {} is ignored by SlotManager.", taskManagerResourceId);
            }
            return CompletableFuture.completedFuture(Acknowledge.get());
        } else {
            return FutureUtils.completedExceptionally(
                    new ResourceManagerException(
                            String.format(
                                    "Unknown TaskManager registration id %s.",
                                    taskManagerRegistrationId)));
        }
    }

    protected void onWorkerRegistered(WorkerType worker, WorkerResourceSpec workerResourceSpec) {
        // noop
    }

    /**
     * 接收 TM的心跳
     * @param resourceID
     * @param heartbeatPayload payload from the originating TaskManager
     * @return
     */
    @Override
    public CompletableFuture<Void> heartbeatFromTaskManager(
            final ResourceID resourceID, final TaskExecutorHeartbeatPayload heartbeatPayload) {
        return taskManagerHeartbeatManager.receiveHeartbeat(resourceID, heartbeatPayload);
    }

    /**
     * 收到JM心跳
     * @param resourceID
     * @return
     */
    @Override
    public CompletableFuture<Void> heartbeatFromJobManager(final ResourceID resourceID) {
        return jobManagerHeartbeatManager.receiveHeartbeat(resourceID, null);
    }

    @Override
    public void disconnectTaskManager(final ResourceID resourceId, final Exception cause) {
        closeTaskManagerConnection(resourceId, cause)
                .ifPresent(ResourceManager.this::stopWorkerIfSupported);
    }

    @Override
    public void disconnectJobManager(
            final JobID jobId, JobStatus jobStatus, final Exception cause) {
        // 表示全局终止job 而不是job 所在节点 下线
        if (jobStatus.isGloballyTerminalState()) {
            removeJob(jobId, cause);
        } else {
            closeJobManagerConnection(jobId, ResourceRequirementHandling.RETAIN, cause);
        }
    }

    /**
     * 告知资源管理器  job的资源开销
     * @param jobMasterId id of the JobMaster
     * @param resourceRequirements resource requirements
     * @param timeout
     * @return
     */
    @Override
    public CompletableFuture<Acknowledge> declareRequiredResources(
            JobMasterId jobMasterId, ResourceRequirements resourceRequirements, Time timeout) {
        final JobID jobId = resourceRequirements.getJobId();
        final JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

        if (null != jobManagerRegistration) {
            if (Objects.equals(jobMasterId, jobManagerRegistration.getJobMasterId())) {
                return getReadyToServeFuture()
                        .thenApply(
                                acknowledge -> {
                                    validateRunsInMainThread();
                                    // 转发请求
                                    slotManager.processResourceRequirements(resourceRequirements);
                                    return null;
                                });
            } else {
                return FutureUtils.completedExceptionally(
                        new ResourceManagerException(
                                "The job leader's id "
                                        + jobManagerRegistration.getJobMasterId()
                                        + " does not match the received id "
                                        + jobMasterId
                                        + '.'));
            }
        } else {
            return FutureUtils.completedExceptionally(
                    new ResourceManagerException(
                            "Could not find registered job manager for job " + jobId + '.'));
        }
    }

    /**
     * 将某个slot修改成free
     * @param instanceID
     * @param slotId
     * @param allocationId
     */
    @Override
    public void notifySlotAvailable(
            final InstanceID instanceID, final SlotID slotId, final AllocationID allocationId) {

        final ResourceID resourceId = slotId.getResourceID();
        WorkerRegistration<WorkerType> registration = taskExecutors.get(resourceId);

        if (registration != null) {
            InstanceID registrationId = registration.getInstanceID();

            if (Objects.equals(registrationId, instanceID)) {
                slotManager.freeSlot(slotId, allocationId);
            } else {
                log.debug(
                        "Invalid registration id for slot available message. This indicates an"
                                + " outdated request.");
            }
        } else {
            log.debug(
                    "Could not find registration for resource id {}. Discarding the slot available"
                            + "message {}.",
                    resourceId.getStringWithMetadata(),
                    slotId);
        }
    }

    /**
     * Cleanup application and shut down cluster.
     *
     * @param finalStatus of the Flink application
     * @param diagnostics diagnostics message for the Flink application or {@code null}
     */
    @Override
    public CompletableFuture<Acknowledge> deregisterApplication(
            final ApplicationStatus finalStatus, @Nullable final String diagnostics) {
        log.info(
                "Shut down cluster because application is in {}, diagnostics {}.",
                finalStatus,
                diagnostics);

        try {
            internalDeregisterApplication(finalStatus, diagnostics);
        } catch (ResourceManagerException e) {
            log.warn("Could not properly shutdown the application.", e);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Integer> getNumberOfRegisteredTaskManagers() {
        return CompletableFuture.completedFuture(taskExecutors.size());
    }

    /**
     * 查询所有注册的TM信息
     * @param timeout of the request
     * @return
     */
    @Override
    public CompletableFuture<Collection<TaskManagerInfo>> requestTaskManagerInfo(Time timeout) {

        final ArrayList<TaskManagerInfo> taskManagerInfos = new ArrayList<>(taskExecutors.size());

        for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> taskExecutorEntry :
                taskExecutors.entrySet()) {
            final ResourceID resourceId = taskExecutorEntry.getKey();
            final WorkerRegistration<WorkerType> taskExecutor = taskExecutorEntry.getValue();

            taskManagerInfos.add(
                    new TaskManagerInfo(
                            resourceId,
                            taskExecutor.getTaskExecutorGateway().getAddress(),
                            taskExecutor.getDataPort(),
                            taskExecutor.getJmxPort(),
                            taskManagerHeartbeatManager.getLastHeartbeatFrom(resourceId),
                            slotManager.getNumberRegisteredSlotsOf(taskExecutor.getInstanceID()),
                            slotManager.getNumberFreeSlotsOf(taskExecutor.getInstanceID()),
                            slotManager.getRegisteredResourceOf(taskExecutor.getInstanceID()),
                            slotManager.getFreeResourceOf(taskExecutor.getInstanceID()),
                            taskExecutor.getHardwareDescription(),
                            taskExecutor.getMemoryConfiguration(),
                            blocklistHandler.isBlockedTaskManager(taskExecutor.getResourceID())));
        }

        return CompletableFuture.completedFuture(taskManagerInfos);
    }

    /**
     * 查询某个TM详情
     * @param resourceId
     * @param timeout of the request
     * @return
     */
    @Override
    public CompletableFuture<TaskManagerInfoWithSlots> requestTaskManagerDetailsInfo(
            ResourceID resourceId, Time timeout) {

        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(resourceId);

        if (taskExecutor == null) {
            return FutureUtils.completedExceptionally(new UnknownTaskExecutorException(resourceId));
        } else {
            final InstanceID instanceId = taskExecutor.getInstanceID();
            final TaskManagerInfoWithSlots taskManagerInfoWithSlots =
                    new TaskManagerInfoWithSlots(
                            new TaskManagerInfo(
                                    resourceId,
                                    taskExecutor.getTaskExecutorGateway().getAddress(),
                                    taskExecutor.getDataPort(),
                                    taskExecutor.getJmxPort(),
                                    taskManagerHeartbeatManager.getLastHeartbeatFrom(resourceId),
                                    slotManager.getNumberRegisteredSlotsOf(instanceId),
                                    slotManager.getNumberFreeSlotsOf(instanceId),
                                    slotManager.getRegisteredResourceOf(instanceId),
                                    slotManager.getFreeResourceOf(instanceId),
                                    taskExecutor.getHardwareDescription(),
                                    taskExecutor.getMemoryConfiguration(),
                                    blocklistHandler.isBlockedTaskManager(
                                            taskExecutor.getResourceID())),
                            slotManager.getAllocatedSlotsOf(instanceId));

            return CompletableFuture.completedFuture(taskManagerInfoWithSlots);
        }
    }

    /**
     * 作为RM 返回当前资源的描述信息
     * @param timeout of the request
     * @return
     */
    @Override
    public CompletableFuture<ResourceOverview> requestResourceOverview(Time timeout) {
        final int numberSlots = slotManager.getNumberRegisteredSlots();
        final ResourceProfile totalResource = slotManager.getRegisteredResource();

        int numberFreeSlots = slotManager.getNumberFreeSlots();
        ResourceProfile freeResource = slotManager.getFreeResource();

        int blockedTaskManagers = 0;
        int totalBlockedFreeSlots = 0;
        if (!blocklistHandler.getAllBlockedNodeIds().isEmpty()) {
            for (WorkerRegistration<WorkerType> registration : taskExecutors.values()) {
                if (blocklistHandler.isBlockedTaskManager(registration.getResourceID())) {
                    // free资源要忽略 block节点
                    blockedTaskManagers++;
                    int blockedFreeSlots =
                            slotManager.getNumberFreeSlotsOf(registration.getInstanceID());
                    totalBlockedFreeSlots += blockedFreeSlots;
                    numberFreeSlots -= blockedFreeSlots;
                    freeResource =
                            freeResource.subtract(
                                    slotManager.getFreeResourceOf(registration.getInstanceID()));
                }
            }
        }
        return CompletableFuture.completedFuture(
                new ResourceOverview(
                        taskExecutors.size(),
                        numberSlots,
                        numberFreeSlots,
                        blockedTaskManagers,
                        totalBlockedFreeSlots,
                        totalResource,
                        freeResource));
    }

    @Override
    public CompletableFuture<Collection<Tuple2<ResourceID, String>>>
            requestTaskManagerMetricQueryServiceAddresses(Time timeout) {
        final ArrayList<CompletableFuture<Optional<Tuple2<ResourceID, String>>>>
                metricQueryServiceAddressFutures = new ArrayList<>(taskExecutors.size());

        for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> workerRegistrationEntry :
                taskExecutors.entrySet()) {
            final ResourceID tmResourceId = workerRegistrationEntry.getKey();
            final WorkerRegistration<WorkerType> workerRegistration =
                    workerRegistrationEntry.getValue();
            final TaskExecutorGateway taskExecutorGateway =
                    workerRegistration.getTaskExecutorGateway();

            // 内部转发给TM
            final CompletableFuture<Optional<Tuple2<ResourceID, String>>>
                    metricQueryServiceAddressFuture =
                            taskExecutorGateway
                                    .requestMetricQueryServiceAddress(timeout)
                                    .thenApply(
                                            o ->
                                                    o.toOptional()
                                                            .map(
                                                                    address ->
                                                                            Tuple2.of(
                                                                                    tmResourceId,
                                                                                    address)));

            metricQueryServiceAddressFutures.add(metricQueryServiceAddressFuture);
        }

        return FutureUtils.combineAll(metricQueryServiceAddressFutures)
                .thenApply(
                        collection ->
                                collection.stream()
                                        .filter(Optional::isPresent)
                                        .map(Optional::get)
                                        .collect(Collectors.toList()));
    }

    /**
     * 转发给TM 请求将某类型文件上传
     * @param taskManagerId identifying the {@link TaskExecutor} to upload the specified file
     * @param fileType type of the file to upload
     * @param timeout for the asynchronous operation
     * @return
     */
    @Override
    public CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByType(
            ResourceID taskManagerId, FileType fileType, Time timeout) {
        log.debug(
                "Request {} file upload from TaskExecutor {}.",
                fileType,
                taskManagerId.getStringWithMetadata());

        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

        if (taskExecutor == null) {
            log.debug(
                    "Request upload of file {} from unregistered TaskExecutor {}.",
                    fileType,
                    taskManagerId.getStringWithMetadata());
            return FutureUtils.completedExceptionally(
                    new UnknownTaskExecutorException(taskManagerId));
        } else {
            return taskExecutor.getTaskExecutorGateway().requestFileUploadByType(fileType, timeout);
        }
    }

    /**
     * 按照名字上传
     * @param taskManagerId identifying the {@link TaskExecutor} to upload the specified file
     * @param fileName name of the file to upload
     * @param timeout for the asynchronous operation
     * @return
     */
    @Override
    public CompletableFuture<TransientBlobKey> requestTaskManagerFileUploadByName(
            ResourceID taskManagerId, String fileName, Time timeout) {
        log.debug(
                "Request upload of file {} from TaskExecutor {}.",
                fileName,
                taskManagerId.getStringWithMetadata());

        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

        if (taskExecutor == null) {
            log.debug(
                    "Request upload of file {} from unregistered TaskExecutor {}.",
                    fileName,
                    taskManagerId.getStringWithMetadata());
            return FutureUtils.completedExceptionally(
                    new UnknownTaskExecutorException(taskManagerId));
        } else {
            return taskExecutor.getTaskExecutorGateway().requestFileUploadByName(fileName, timeout);
        }
    }

    /**
     * TODO 转发获取日志
     * @param taskManagerId identifying the {@link TaskExecutor} to get log list from
     * @param timeout for the asynchronous operation
     * @return
     */
    @Override
    public CompletableFuture<Collection<LogInfo>> requestTaskManagerLogList(
            ResourceID taskManagerId, Time timeout) {
        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);
        if (taskExecutor == null) {
            log.debug(
                    "Requested log list from unregistered TaskExecutor {}.",
                    taskManagerId.getStringWithMetadata());
            return FutureUtils.completedExceptionally(
                    new UnknownTaskExecutorException(taskManagerId));
        } else {
            return taskExecutor.getTaskExecutorGateway().requestLogList(timeout);
        }
    }

    @Override
    public CompletableFuture<Void> releaseClusterPartitions(IntermediateDataSetID dataSetId) {
        // 也会转发给网关 让TM放弃某分区数据
        return clusterPartitionTracker.releaseClusterPartitions(dataSetId);
    }

    /**
     * 更新TM分区数据
     * @param taskExecutorId The id of the task executor.
     * @param clusterPartitionReport The status of the cluster partitions.
     * @return
     */
    @Override
    public CompletableFuture<Void> reportClusterPartitions(
            ResourceID taskExecutorId, ClusterPartitionReport clusterPartitionReport) {
        clusterPartitionTracker.processTaskExecutorClusterPartitionReport(
                taskExecutorId, clusterPartitionReport);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * 获取洗牌信息
     * @param intermediateDataSetID The id of the dataset.
     * @return
     */
    @Override
    public CompletableFuture<List<ShuffleDescriptor>> getClusterPartitionsShuffleDescriptors(
            IntermediateDataSetID intermediateDataSetID) {
        return CompletableFuture.completedFuture(
                clusterPartitionTracker.getClusterPartitionShuffleDescriptors(
                        intermediateDataSetID));
    }

    @Override
    public CompletableFuture<Map<IntermediateDataSetID, DataSetMetaInfo>> listDataSets() {
        // 返回数据集的分区信息
        return CompletableFuture.completedFuture(clusterPartitionTracker.listDataSets());
    }

    /**
     * 请求线程栈信息   转发给TM
     * @param taskManagerId taskManagerId identifying the {@link TaskExecutor} to get the thread
     *     dump from
     * @param timeout timeout of the asynchronous operation
     * @return
     */
    @Override
    public CompletableFuture<ThreadDumpInfo> requestThreadDump(
            ResourceID taskManagerId, Time timeout) {
        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

        if (taskExecutor == null) {
            log.debug(
                    "Requested thread dump from unregistered TaskExecutor {}.",
                    taskManagerId.getStringWithMetadata());
            return FutureUtils.completedExceptionally(
                    new UnknownTaskExecutorException(taskManagerId));
        } else {
            return taskExecutor.getTaskExecutorGateway().requestThreadDump(timeout);
        }
    }

    @Override
    @Local // Bug; see FLINK-27954
    public CompletableFuture<TaskExecutorThreadInfoGateway> requestTaskExecutorThreadInfoGateway(
            ResourceID taskManagerId, Time timeout) {

        final WorkerRegistration<WorkerType> taskExecutor = taskExecutors.get(taskManagerId);

        if (taskExecutor == null) {
            return FutureUtils.completedExceptionally(
                    new UnknownTaskExecutorException(taskManagerId));
        } else {
            return CompletableFuture.completedFuture(taskExecutor.getTaskExecutorGateway());
        }
    }

    /**
     *
     * @param newNodes the new blocked node records
     * @return
     */
    @Override
    public CompletableFuture<Acknowledge> notifyNewBlockedNodes(Collection<BlockedNode> newNodes) {
        blocklistHandler.addNewBlockedNodes(newNodes);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ------------------------------------------------------------------------
    //  Internal methods
    // ------------------------------------------------------------------------

    /**
     * 通过id 检索地址  这里返回的是 nodeId
     * @param taskManagerId
     * @return
     */
    @VisibleForTesting
    String getNodeIdOfTaskManager(ResourceID taskManagerId) {
        checkState(taskExecutors.containsKey(taskManagerId));
        return taskExecutors.get(taskManagerId).getNodeId();
    }

    /**
     * Registers a new JobMaster.
     *
     * @param jobMasterGateway to communicate with the registering JobMaster
     * @param jobId of the job for which the JobMaster is responsible
     * @param jobManagerAddress address of the JobMaster
     * @param jobManagerResourceId ResourceID of the JobMaster
     * @return RegistrationResponse
     * 注册一个新的JM对象
     */
    private RegistrationResponse registerJobMasterInternal(
            final JobMasterGateway jobMasterGateway,
            JobID jobId,
            String jobManagerAddress,
            ResourceID jobManagerResourceId) {
        // 表示该JM 已经注册
        if (jobManagerRegistrations.containsKey(jobId)) {
            JobManagerRegistration oldJobManagerRegistration = jobManagerRegistrations.get(jobId);

            if (Objects.equals(
                    oldJobManagerRegistration.getJobMasterId(),
                    jobMasterGateway.getFencingToken())) {
                // same registration
                log.debug(
                        "Job manager {}@{} was already registered.",
                        jobMasterGateway.getFencingToken(),
                        jobManagerAddress);
            } else {
                // tell old job manager that he is no longer the job leader
                // 断开旧连接
                closeJobManagerConnection(
                        oldJobManagerRegistration.getJobID(),
                        ResourceRequirementHandling.RETAIN,
                        new Exception("New job leader for job " + jobId + " found."));

                JobManagerRegistration jobManagerRegistration =
                        new JobManagerRegistration(jobId, jobManagerResourceId, jobMasterGateway);
                jobManagerRegistrations.put(jobId, jobManagerRegistration);
                jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
                // JM 通过该对象监听阻塞节点
                blocklistHandler.registerBlocklistListener(jobMasterGateway);
            }
        } else {
            // new registration for the job
            JobManagerRegistration jobManagerRegistration =
                    new JobManagerRegistration(jobId, jobManagerResourceId, jobMasterGateway);
            jobManagerRegistrations.put(jobId, jobManagerRegistration);
            jmResourceIdRegistrations.put(jobManagerResourceId, jobManagerRegistration);
            blocklistHandler.registerBlocklistListener(jobMasterGateway);
        }

        log.info(
                "Registered job manager {}@{} for job {}.",
                jobMasterGateway.getFencingToken(),
                jobManagerAddress,
                jobId);

        // 监控与该节点的心跳
        jobManagerHeartbeatManager.monitorTarget(
                jobManagerResourceId, new JobMasterHeartbeatSender(jobMasterGateway));

        return new JobMasterRegistrationSuccess(getFencingToken(), resourceId);
    }

    /**
     * Registers a new TaskExecutor.
     *
     * @param taskExecutorRegistration task executor registration parameters
     * @return RegistrationResponse
     * 注册TM
     */
    private RegistrationResponse registerTaskExecutorInternal(
            TaskExecutorGateway taskExecutorGateway,
            TaskExecutorRegistration taskExecutorRegistration) {
        ResourceID taskExecutorResourceId = taskExecutorRegistration.getResourceId();
        WorkerRegistration<WorkerType> oldRegistration =
                taskExecutors.remove(taskExecutorResourceId);
        if (oldRegistration != null) {
            // TODO :: suggest old taskExecutor to stop itself
            log.debug(
                    "Replacing old registration of TaskExecutor {}.",
                    taskExecutorResourceId.getStringWithMetadata());

            // remove old task manager registration from slot manager
            // 注销之前的TM  包括它声明的资源都要扣除
            slotManager.unregisterTaskManager(
                    oldRegistration.getInstanceID(),
                    new ResourceManagerException(
                            String.format(
                                    "TaskExecutor %s re-connected to the ResourceManager.",
                                    taskExecutorResourceId.getStringWithMetadata())));
        }

        final Optional<WorkerType> newWorkerOptional =
                getWorkerNodeIfAcceptRegistration(taskExecutorResourceId);

        String taskExecutorAddress = taskExecutorRegistration.getTaskExecutorAddress();
        if (!newWorkerOptional.isPresent()) {
            log.warn(
                    "Discard registration from TaskExecutor {} at ({}) because the framework did "
                            + "not recognize it",
                    taskExecutorResourceId.getStringWithMetadata(),
                    taskExecutorAddress);
            return new TaskExecutorRegistrationRejection(
                    "The ResourceManager does not recognize this TaskExecutor.");
        } else {
            // 产生一个worker注册对象
            WorkerType newWorker = newWorkerOptional.get();
            WorkerRegistration<WorkerType> registration =
                    new WorkerRegistration<>(
                            taskExecutorGateway,
                            newWorker,
                            taskExecutorRegistration.getDataPort(),
                            taskExecutorRegistration.getJmxPort(),
                            taskExecutorRegistration.getHardwareDescription(),
                            taskExecutorRegistration.getMemoryConfiguration(),
                            taskExecutorRegistration.getTotalResourceProfile(),
                            taskExecutorRegistration.getDefaultSlotResourceProfile(),
                            taskExecutorRegistration.getNodeId());

            log.info(
                    "Registering TaskManager with ResourceID {} ({}) at ResourceManager",
                    taskExecutorResourceId.getStringWithMetadata(),
                    taskExecutorAddress);
            taskExecutors.put(taskExecutorResourceId, registration);

            // 创建心跳包发送对象
            taskManagerHeartbeatManager.monitorTarget(
                    taskExecutorResourceId, new TaskExecutorHeartbeatSender(taskExecutorGateway));

            return new TaskExecutorRegistrationSuccess(
                    registration.getInstanceID(),
                    resourceId,
                    clusterInformation,
                    latestTokens.get());
        }
    }

    protected void registerMetrics() {
        resourceManagerMetricGroup.gauge(
                MetricNames.NUM_REGISTERED_TASK_MANAGERS, () -> (long) taskExecutors.size());
    }

    private void clearStateInternal() {
        jobManagerRegistrations.clear();
        jmResourceIdRegistrations.clear();
        taskExecutors.clear();

        try {
            jobLeaderIdService.clear();
        } catch (Exception e) {
            onFatalError(
                    new ResourceManagerException(
                            "Could not properly clear the job leader id service.", e));
        }
    }

    /**
     * This method should be called by the framework once it detects that a currently registered job
     * manager has failed.
     *
     * @param jobId identifying the job whose leader shall be disconnected.
     * @param resourceRequirementHandling indicating how existing resource requirements for the
     *     corresponding job should be handled
     * @param cause The exception which cause the JobManager failed.
     *              断开与JM的连接
     */
    protected void closeJobManagerConnection(
            JobID jobId, ResourceRequirementHandling resourceRequirementHandling, Exception cause) {
        JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.remove(jobId);

        if (jobManagerRegistration != null) {
            final ResourceID jobManagerResourceId =
                    jobManagerRegistration.getJobManagerResourceID();
            final JobMasterGateway jobMasterGateway = jobManagerRegistration.getJobManagerGateway();
            final JobMasterId jobMasterId = jobManagerRegistration.getJobMasterId();

            log.info(
                    "Disconnect job manager {}@{} for job {} from the resource manager.",
                    jobMasterId,
                    jobMasterGateway.getAddress(),
                    jobId);

            // 不需要监控该连接了
            jobManagerHeartbeatManager.unmonitorTarget(jobManagerResourceId);

            jmResourceIdRegistrations.remove(jobManagerResourceId);
            // 本身感知到阻塞节点时 会以rpc方式进行通知JM
            blocklistHandler.deregisterBlocklistListener(jobMasterGateway);

            if (resourceRequirementHandling == ResourceRequirementHandling.CLEAR) {
                // 表示需要清理该job需要的资源信息
                slotManager.clearResourceRequirements(jobId);
            }

            // tell the job manager about the disconnect
            jobMasterGateway.disconnectResourceManager(getFencingToken(), cause);
        } else {
            log.debug("There was no registered job manager for job {}.", jobId);
        }
    }

    /**
     * This method should be called by the framework once it detects that a currently registered
     * task executor has failed.
     *
     * @param resourceID Id of the TaskManager that has failed.
     * @param cause The exception which cause the TaskManager failed.
     * @return The {@link WorkerType} of the closed connection, or empty if already removed.
     * 关闭与某个TM的连接
     */
    protected Optional<WorkerType> closeTaskManagerConnection(
            final ResourceID resourceID, final Exception cause) {

        // 断开连接 就不需要再监控超时了
        taskManagerHeartbeatManager.unmonitorTarget(resourceID);

        // 得到注册信息
        WorkerRegistration<WorkerType> workerRegistration = taskExecutors.remove(resourceID);

        if (workerRegistration != null) {
            log.info(
                    "Closing TaskExecutor connection {} because: {}",
                    resourceID.getStringWithMetadata(),
                    cause.getMessage(),
                    ExceptionUtils.returnExceptionIfUnexpected(cause.getCause()));
            ExceptionUtils.logExceptionIfExcepted(cause.getCause(), log);

            // TODO :: suggest failed task executor to stop itself
            // 释放对象
            slotManager.unregisterTaskManager(workerRegistration.getInstanceID(), cause);
            // 移除tracker上维护的一些中间结果集信息 (哪个TM上维护了哪些中间结果集)
            clusterPartitionTracker.processTaskExecutorShutdown(resourceID);

            // 请求断开连接
            workerRegistration.getTaskExecutorGateway().disconnectResourceManager(cause);
        } else {
            log.debug(
                    "No open TaskExecutor connection {}. Ignoring close TaskExecutor connection. Closing reason was: {}",
                    resourceID.getStringWithMetadata(),
                    cause.getMessage());
        }

        return Optional.ofNullable(workerRegistration).map(WorkerRegistration::getWorker);
    }

    protected void removeJob(JobID jobId, Exception cause) {
        try {
            jobLeaderIdService.removeJob(jobId);
        } catch (Exception e) {
            log.warn(
                    "Could not properly remove the job {} from the job leader id service.",
                    jobId,
                    e);
        }

        if (jobManagerRegistrations.containsKey(jobId)) {
            closeJobManagerConnection(jobId, ResourceRequirementHandling.CLEAR, cause);
        }
    }

    /**
     * 收到通知  感知到某个 jobMaster不再是leader
     * @param jobId
     * @param oldJobMasterId
     */
    protected void jobLeaderLostLeadership(JobID jobId, JobMasterId oldJobMasterId) {
        if (jobManagerRegistrations.containsKey(jobId)) {
            JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);

            if (Objects.equals(jobManagerRegistration.getJobMasterId(), oldJobMasterId)) {
                closeJobManagerConnection(
                        jobId,
                        ResourceRequirementHandling.RETAIN,
                        new Exception("Job leader lost leadership."));
            } else {
                log.debug(
                        "Discarding job leader lost leadership, because a new job leader was found for job {}. ",
                        jobId);
            }
        } else {
            log.debug(
                    "Discard job leader lost leadership for outdated leader {} for job {}.",
                    oldJobMasterId,
                    jobId);
        }
    }

    @VisibleForTesting
    public Optional<InstanceID> getInstanceIdByResourceId(ResourceID resourceID) {
        return Optional.ofNullable(taskExecutors.get(resourceID))
                .map(TaskExecutorConnection::getInstanceID);
    }

    protected WorkerType getWorkerByInstanceId(InstanceID instanceId) {
        WorkerType worker = null;
        // TODO: Improve performance by having an index on the instanceId
        for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> entry :
                taskExecutors.entrySet()) {
            if (entry.getValue().getInstanceID().equals(instanceId)) {
                worker = entry.getValue().getWorker();
                break;
            }
        }

        return worker;
    }

    private enum ResourceRequirementHandling {
        RETAIN,
        CLEAR
    }

    // ------------------------------------------------------------------------
    //  Error Handling
    // ------------------------------------------------------------------------

    /**
     * Notifies the ResourceManager that a fatal error has occurred and it cannot proceed.
     *
     * @param t The exception describing the fatal error
     */
    protected void onFatalError(Throwable t) {
        try {
            log.error("Fatal error occurred in ResourceManager.", t);
        } catch (Throwable ignored) {
        }

        // The fatal error handler implementation should make sure that this call is non-blocking
        fatalErrorHandler.onFatalError(t);
    }

    /**
     * 开启心跳服务  心跳服务已经搭好框架 只要实现几个钩子即可
     */
    private void startHeartbeatServices() {
        // 分别创建 JobMaster/TaskExecutor的心跳服务
        taskManagerHeartbeatManager =
                heartbeatServices.createHeartbeatManagerSender(
                        resourceId,
                        new TaskManagerHeartbeatListener(),
                        getMainThreadExecutor(),
                        log);

        jobManagerHeartbeatManager =
                heartbeatServices.createHeartbeatManagerSender(
                        resourceId,
                        new JobManagerHeartbeatListener(),
                        getMainThreadExecutor(),
                        log);
    }

    /**
     * 停止心跳服务
     */
    private void stopHeartbeatServices() {
        taskManagerHeartbeatManager.stop();
        jobManagerHeartbeatManager.stop();
    }

    // ------------------------------------------------------------------------
    //  Framework specific behavior
    // ------------------------------------------------------------------------

    /**
     * Initializes the framework specific components.
     *
     * @throws ResourceManagerException which occurs during initialization and causes the resource
     *     manager to fail.
     *     进行一些初始化操作
     */
    protected abstract void initialize() throws ResourceManagerException;

    /**
     * Terminates the framework specific components.
     *
     * @throws Exception which occurs during termination.
     */
    protected abstract void terminate() throws Exception;

    /**
     * The framework specific code to deregister the application. This should report the
     * application's final status and shut down the resource manager cleanly.
     *
     * <p>This method also needs to make sure all pending containers that are not registered yet are
     * returned.
     *
     * @param finalStatus The application status to report.
     * @param optionalDiagnostics A diagnostics message or {@code null}.
     * @throws ResourceManagerException if the application could not be shut down.
     */
    protected abstract void internalDeregisterApplication(
            ApplicationStatus finalStatus, @Nullable String optionalDiagnostics)
            throws ResourceManagerException;

    /**
     * Get worker node if the worker resource is accepted.
     *
     * @param resourceID The worker resource id
     */
    protected abstract Optional<WorkerType> getWorkerNodeIfAcceptRegistration(
            ResourceID resourceID);

    /**
     * Stops the given worker if supported.
     *
     * @param worker The worker.
     *
     */
    public void stopWorkerIfSupported(WorkerType worker) {
        if (resourceAllocator.isSupported()) {
            // 与worker断开连接后 清理资源
            resourceAllocator.cleaningUpDisconnectedResource(worker.getResourceID());
        }
    }

    /**
     * Get the ready to serve future of the resource manager.
     *
     * @return The ready to serve future of the resource manager, which indicated whether it is
     *     ready to serve.
     */
    protected abstract CompletableFuture<Void> getReadyToServeFuture();

    protected abstract ResourceAllocator getResourceAllocator();

    /**
     * Set {@link SlotManager} whether to fail unfulfillable slot requests.
     *
     * @param failUnfulfillableRequest whether to fail unfulfillable requests
     */
    protected void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
        slotManager.setFailUnfulfillableRequest(failUnfulfillableRequest);
    }

    // ------------------------------------------------------------------------
    //  Static utility classes
    // ------------------------------------------------------------------------

    /**
     * 发送心跳的对象
     */
    private static final class JobMasterHeartbeatSender extends HeartbeatSender<Void> {
        private final JobMasterGateway jobMasterGateway;

        private JobMasterHeartbeatSender(JobMasterGateway jobMasterGateway) {
            this.jobMasterGateway = jobMasterGateway;
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(ResourceID resourceID, Void payload) {
            return jobMasterGateway.heartbeatFromResourceManager(resourceID);
        }
    }

    private static final class TaskExecutorHeartbeatSender extends HeartbeatSender<Void> {
        private final TaskExecutorGateway taskExecutorGateway;

        private TaskExecutorHeartbeatSender(TaskExecutorGateway taskExecutorGateway) {
            this.taskExecutorGateway = taskExecutorGateway;
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(ResourceID resourceID, Void payload) {
            return taskExecutorGateway.heartbeatFromResourceManager(resourceID);
        }
    }

    /**
     * 在 slotManager中 当发现无法为job提供资源时会触发该方法
     */
    private class ResourceEventListenerImpl implements ResourceEventListener {
        @Override
        public void notEnoughResourceAvailable(
                JobID jobId, Collection<ResourceRequirement> acquiredResources) {
            validateRunsInMainThread();

            JobManagerRegistration jobManagerRegistration = jobManagerRegistrations.get(jobId);
            if (jobManagerRegistration != null) {
                jobManagerRegistration
                        .getJobManagerGateway()
                        // 转发给网关通知
                        .notifyNotEnoughResourcesAvailable(acquiredResources);
            }
        }
    }

    /**
     * 该对象感知某个jobMaster 不再是leader
     */
    private class JobLeaderIdActionsImpl implements JobLeaderIdActions {

        @Override
        public void jobLeaderLostLeadership(final JobID jobId, final JobMasterId oldJobMasterId) {
            runAsync(
                    new Runnable() {
                        @Override
                        public void run() {
                            ResourceManager.this.jobLeaderLostLeadership(jobId, oldJobMasterId);
                        }
                    });
        }

        @Override
        public void notifyJobTimeout(final JobID jobId, final UUID timeoutId) {
            runAsync(
                    new Runnable() {
                        @Override
                        public void run() {
                            if (jobLeaderIdService.isValidTimeout(jobId, timeoutId)) {
                                removeJob(
                                        jobId,
                                        new Exception(
                                                "Job " + jobId + "was removed because of timeout"));
                            }
                        }
                    });
        }

        @Override
        public void handleError(Throwable error) {
            onFatalError(error);
        }
    }

    /**
     * 监听心跳包发送结果
     * TaskExecutorHeartbeatPayload 表示 TM发送的心跳包
     */
    private class TaskManagerHeartbeatListener
            implements HeartbeatListener<TaskExecutorHeartbeatPayload, Void> {

        /**
         * 表示心跳包超时
         * @param resourceID Resource ID of the machine whose heartbeat has timed out
         */
        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceID) {
            final String message =
                    String.format(
                            "The heartbeat of TaskManager with id %s timed out.",
                            resourceID.getStringWithMetadata());
            log.info(message);

            // 超时即断开连接
            handleTaskManagerConnectionLoss(resourceID, new TimeoutException(message));
        }

        /**
         * 表示与某个TaskExecutor 断开连接
         * @param resourceID
         * @param cause
         */
        private void handleTaskManagerConnectionLoss(ResourceID resourceID, Exception cause) {
            validateRunsInMainThread();
            // 断开与TM的连接
            closeTaskManagerConnection(resourceID, cause)
                    .ifPresent(ResourceManager.this::stopWorkerIfSupported);
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "TaskManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());
            log.info(message);

            handleTaskManagerConnectionLoss(resourceID, new ResourceManagerException(message));
        }

        /**
         * 从TM收到心跳包
         * @param resourceID Resource ID identifying the sender of the payload
         * @param payload Payload of the received heartbeat
         */
        @Override
        public void reportPayload(
                final ResourceID resourceID, final TaskExecutorHeartbeatPayload payload) {
            validateRunsInMainThread();
            final WorkerRegistration<WorkerType> workerRegistration = taskExecutors.get(resourceID);

            if (workerRegistration == null) {
                log.debug(
                        "Received slot report from TaskManager {} which is no longer registered.",
                        resourceID.getStringWithMetadata());
            } else {
                InstanceID instanceId = workerRegistration.getInstanceID();

                // 在本地同步slot的状态
                slotManager.reportSlotStatus(instanceId, payload.getSlotReport());
                // 表示收到TM上报的分区信息
                clusterPartitionTracker.processTaskExecutorClusterPartitionReport(
                        resourceID, payload.getClusterPartitionReport());
            }
        }

        @Override
        public Void retrievePayload(ResourceID resourceID) {
            return null;
        }
    }

    /**
     * 与JobMaster相关的心跳监听器
     */
    private class JobManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceID) {
            final String message =
                    String.format(
                            "The heartbeat of JobManager with id %s timed out.",
                            resourceID.getStringWithMetadata());
            log.info(message);

            handleJobManagerConnectionLoss(resourceID, new TimeoutException(message));
        }

        /**
         * 关闭与JM的连接
         * @param resourceID
         * @param cause
         */
        private void handleJobManagerConnectionLoss(ResourceID resourceID, Exception cause) {
            validateRunsInMainThread();
            if (jmResourceIdRegistrations.containsKey(resourceID)) {
                JobManagerRegistration jobManagerRegistration =
                        jmResourceIdRegistrations.get(resourceID);

                if (jobManagerRegistration != null) {
                    closeJobManagerConnection(
                            jobManagerRegistration.getJobID(),
                            ResourceRequirementHandling.RETAIN,
                            cause);
                }
            }
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "JobManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());
            log.info(message);

            handleJobManagerConnectionLoss(resourceID, new ResourceManagerException(message));
        }

        @Override
        public void reportPayload(ResourceID resourceID, Void payload) {
            // nothing to do since there is no payload
        }

        @Override
        public Void retrievePayload(ResourceID resourceID) {
            return null;
        }
    }

    private class ResourceManagerBlocklistContext implements BlocklistContext {
        @Override
        public void blockResources(Collection<BlockedNode> blockedNodes) {}

        @Override
        public void unblockResources(Collection<BlockedNode> unBlockedNodes) {
            // when a node is unblocked, we should trigger the resource requirements because the
            // slots on this node become available again.
            // 当某些节点解除阻塞时  通知重新检测资源是否足够 因为此时该节点对应的TaskExecutor也会被考虑了
            slotManager.triggerResourceRequirementsCheck();
        }
    }

    // ------------------------------------------------------------------------
    //  Resource Management
    // ------------------------------------------------------------------------

    /**
     * TODO
     * @param tokens
     * @throws Exception
     */
    @Override
    public void onNewTokensObtained(byte[] tokens) throws Exception {
        latestTokens.set(tokens);

        log.info("Updating delegation tokens for {} task manager(s).", taskExecutors.size());

        if (!taskExecutors.isEmpty()) {
            final List<CompletableFuture<Acknowledge>> futures =
                    new ArrayList<>(taskExecutors.size());

            for (Map.Entry<ResourceID, WorkerRegistration<WorkerType>> workerRegistrationEntry :
                    taskExecutors.entrySet()) {
                WorkerRegistration<WorkerType> registration = workerRegistrationEntry.getValue();
                log.info("Updating delegation tokens for node {}.", registration.getNodeId());
                final TaskExecutorGateway taskExecutorGateway =
                        registration.getTaskExecutorGateway();
                futures.add(taskExecutorGateway.updateDelegationTokens(getFencingToken(), tokens));
            }

            FutureUtils.combineAll(futures).get();
        }
    }
}
