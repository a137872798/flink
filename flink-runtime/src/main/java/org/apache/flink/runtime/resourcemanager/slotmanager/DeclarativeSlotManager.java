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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.rest.messages.taskmanager.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** Implementation of {@link SlotManager} supporting declarative slot management.
 * 声明式的 slot管理器  这个对象的特点就是声明的资源规格是固定的 没有看见拆分的逻辑
 * */
public class DeclarativeSlotManager implements SlotManager {
    private static final Logger LOG = LoggerFactory.getLogger(DeclarativeSlotManager.class);

    /**
     * 该对象追踪slot的状态
     */
    private final SlotTracker slotTracker;

    /**
     * 通过该对象可以追踪每个job的资源需求量/当前已分配量
     */
    private final ResourceTracker resourceTracker;
    private final BiFunction<Executor, ResourceAllocator, TaskExecutorManager>
            taskExecutorManagerFactory;

    /**
     * 该对象维护 TaskExecutor (也称为worker)
     */
    @Nullable private TaskExecutorManager taskExecutorManager;

    /** Timeout for slot requests to the task manager. */
    private final Time taskManagerRequestTimeout;

    private final SlotMatchingStrategy slotMatchingStrategy;

    private final SlotManagerMetricGroup slotManagerMetricGroup;

    /**
     * 记录每个job所在的jobMaster地址
     */
    private final Map<JobID, String> jobMasterTargetAddresses = new HashMap<>();
    private final Map<SlotID, AllocationID> pendingSlotAllocations;

    /** Delay of the requirement change check in the slot manager. */
    private final Duration requirementsCheckDelay;

    /**
     * 标记需要通知没有足够的资源
     */
    private boolean sendNotEnoughResourceNotifications = true;

    /** Scheduled executor for timeouts. */
    private final ScheduledExecutor scheduledExecutor;

    /** ResourceManager's id. */
    @Nullable private ResourceManagerId resourceManagerId;

    /** Executor for future callbacks which have to be "synchronized". */
    @Nullable private Executor mainThreadExecutor;

    /** Callbacks for resource not enough.
     * 回调函数 在资源不足时触发
     * */
    @Nullable private ResourceEventListener resourceEventListener;

    /** The future of the requirements delay check. */
    @Nullable private CompletableFuture<Void> requirementsCheckFuture;

    /** Blocked task manager checker. */
    @Nullable private BlockedTaskManagerChecker blockedTaskManagerChecker;

    /** True iff the component has been started. */
    private boolean started;

    public DeclarativeSlotManager(
            ScheduledExecutor scheduledExecutor,
            SlotManagerConfiguration slotManagerConfiguration,
            SlotManagerMetricGroup slotManagerMetricGroup,
            ResourceTracker resourceTracker,
            SlotTracker slotTracker) {

        Preconditions.checkNotNull(slotManagerConfiguration);
        this.taskManagerRequestTimeout = slotManagerConfiguration.getTaskManagerRequestTimeout();
        this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);
        this.resourceTracker = Preconditions.checkNotNull(resourceTracker);
        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
        this.requirementsCheckDelay = slotManagerConfiguration.getRequirementCheckDelay();

        pendingSlotAllocations = CollectionUtil.newHashMapWithExpectedSize(16);

        this.slotTracker = Preconditions.checkNotNull(slotTracker);
        slotTracker.registerSlotStatusUpdateListener(createSlotStatusUpdateListener());

        slotMatchingStrategy = slotManagerConfiguration.getSlotMatchingStrategy();

        taskExecutorManagerFactory =
                (executor, resourceAllocator) ->
                        new TaskExecutorManager(
                                slotManagerConfiguration.getDefaultWorkerResourceSpec(),
                                slotManagerConfiguration.getNumSlotsPerWorker(),
                                slotManagerConfiguration.getMaxSlotNum(),
                                slotManagerConfiguration.isWaitResultConsumedBeforeRelease(),
                                slotManagerConfiguration.getRedundantTaskManagerNum(),
                                slotManagerConfiguration.getTaskManagerTimeout(),
                                slotManagerConfiguration.getDeclareNeededResourceDelay(),
                                scheduledExecutor,
                                executor,
                                resourceAllocator);

        resourceManagerId = null;
        resourceEventListener = null;
        mainThreadExecutor = null;
        taskExecutorManager = null;
        blockedTaskManagerChecker = null;

        started = false;
    }

    /**
     * 监听slot的状态变化
     * @return
     */
    private SlotStatusUpdateListener createSlotStatusUpdateListener() {
        return (taskManagerSlot, previous, current, jobId) -> {
            if (previous == SlotState.PENDING) {
                // 从pending容器移除
                pendingSlotAllocations.remove(taskManagerSlot.getSlotId());
            }

            if (current == SlotState.PENDING) {
                // 通知为job分配了资源
                resourceTracker.notifyAcquiredResource(jobId, taskManagerSlot.getResourceProfile());
            }
            if (current == SlotState.FREE) {
                // 通知job失去了资源
                resourceTracker.notifyLostResource(jobId, taskManagerSlot.getResourceProfile());
            }

            if (current == SlotState.ALLOCATED) {
                // 记录对应的worker 被使用了一个slot
                taskExecutorManager.occupySlot(taskManagerSlot.getInstanceId());
            }
            if (previous == SlotState.ALLOCATED && current == SlotState.FREE) {
                // 被释放了一个slot
                taskExecutorManager.freeSlot(taskManagerSlot.getInstanceId());
            }
        };
    }

    @Override
    public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
        // this sets up a grace period, e.g., when the cluster was started, to give task executors
        // time to connect
        sendNotEnoughResourceNotifications = failUnfulfillableRequest;

        if (failUnfulfillableRequest) {
            checkResourceRequirementsWithDelay();
        }
    }

    @Override
    public void triggerResourceRequirementsCheck() {
        checkResourceRequirementsWithDelay();
    }

    // ---------------------------------------------------------------------------------------------
    // Component lifecycle methods
    // ---------------------------------------------------------------------------------------------

    /**
     * Starts the slot manager with the given leader id and resource manager actions.
     *
     * @param newResourceManagerId to use for communication with the task managers
     * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
     * @param newResourceAllocator to use for resource (de-)allocations
     * @param newBlockedTaskManagerChecker to query whether a task manager is blocked
     *                                     启动对象 就是设置一些组件
     */
    @Override
    public void start(
            ResourceManagerId newResourceManagerId,
            Executor newMainThreadExecutor,
            ResourceAllocator newResourceAllocator,
            ResourceEventListener newResourceEventListener,
            BlockedTaskManagerChecker newBlockedTaskManagerChecker) {
        LOG.debug("Starting the slot manager.");

        this.resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
        mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
        resourceEventListener = Preconditions.checkNotNull(newResourceEventListener);
        taskExecutorManager =
                taskExecutorManagerFactory.apply(newMainThreadExecutor, newResourceAllocator);
        blockedTaskManagerChecker = Preconditions.checkNotNull(newBlockedTaskManagerChecker);

        started = true;

        registerSlotManagerMetrics();
    }

    private void registerSlotManagerMetrics() {
        slotManagerMetricGroup.gauge(
                MetricNames.TASK_SLOTS_AVAILABLE, () -> (long) getNumberFreeSlots());
        slotManagerMetricGroup.gauge(
                MetricNames.TASK_SLOTS_TOTAL, () -> (long) getNumberRegisteredSlots());
    }

    /** Suspends the component. This clears the internal state of the slot manager.
     * 暂停本对象 设置started为false  并将组件置空
     * */
    @Override
    public void suspend() {
        if (!started) {
            return;
        }

        LOG.info("Suspending the slot manager.");

        slotManagerMetricGroup.close();

        resourceTracker.clear();
        if (taskExecutorManager != null) {
            taskExecutorManager.close();

            for (InstanceID registeredTaskManager : taskExecutorManager.getTaskExecutors()) {
                unregisterTaskManager(
                        registeredTaskManager,
                        new SlotManagerException("The slot manager is being suspended."));
            }
        }

        taskExecutorManager = null;
        resourceManagerId = null;
        resourceEventListener = null;
        blockedTaskManagerChecker = null;
        started = false;
    }

    /**
     * Closes the slot manager.
     *
     * @throws Exception if the close operation fails
     */
    @Override
    public void close() throws Exception {
        LOG.info("Closing the slot manager.");

        suspend();
    }

    // ---------------------------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------------------------

    /**
     * 清理某个job的资源需求信息
     * @param jobId job for which to clear the requirements
     */
    @Override
    public void clearResourceRequirements(JobID jobId) {
        checkInit();
        maybeReclaimInactiveSlots(jobId);
        jobMasterTargetAddresses.remove(jobId);
        resourceTracker.notifyResourceRequirements(jobId, Collections.emptyList());
    }

    /**
     * 处理一个分配请求
     * @param resourceRequirements resource requirements of a job   包含jobId job地址 需要的资源
     */
    @Override
    public void processResourceRequirements(ResourceRequirements resourceRequirements) {
        checkInit();

        // 发现该job未声明资源
        if (resourceRequirements.getResourceRequirements().isEmpty()
                && resourceTracker.isRequirementEmpty(resourceRequirements.getJobId())) {
            return;
        } else if (resourceRequirements.getResourceRequirements().isEmpty()) {
            LOG.info("Clearing resource requirements of job {}", resourceRequirements.getJobId());
        } else {
            LOG.info(
                    "Received resource requirements from job {}: {}",
                    resourceRequirements.getJobId(),
                    resourceRequirements.getResourceRequirements());
        }

        // 表示有需要的资源
        if (!resourceRequirements.getResourceRequirements().isEmpty()) {
            // 记录job的地址
            jobMasterTargetAddresses.put(
                    resourceRequirements.getJobId(), resourceRequirements.getTargetAddress());
        }

        // 将需要的资源更新到tracker
        resourceTracker.notifyResourceRequirements(
                resourceRequirements.getJobId(), resourceRequirements.getResourceRequirements());
        checkResourceRequirementsWithDelay();
    }

    /**
     * 尝试回收不活跃(空闲)的slot
     * @param jobId
     */
    private void maybeReclaimInactiveSlots(JobID jobId) {
        if (!resourceTracker.getAcquiredResources(jobId).isEmpty()) {
            final Collection<TaskExecutorConnection> taskExecutorsWithAllocatedSlots =
                    slotTracker.getTaskExecutorsWithAllocatedSlotsForJob(jobId);
            for (TaskExecutorConnection taskExecutorConnection : taskExecutorsWithAllocatedSlots) {
                final TaskExecutorGateway taskExecutorGateway =
                        taskExecutorConnection.getTaskExecutorGateway();
                // 释放slot  通过rpc调用
                taskExecutorGateway.freeInactiveSlots(jobId, taskManagerRequestTimeout);
            }
        }
    }

    /**
     * 先是注册 TM
     * @param taskExecutorConnection for the new task manager
     * @param initialSlotReport for the new task manager  这里包含初始slot
     * @param totalResourceProfile for the new task manager
     * @param defaultSlotResourceProfile for the new task manager
     * @return
     */
    @Override
    public RegistrationResult registerTaskManager(
            final TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        checkInit();
        LOG.debug(
                "Registering task executor {} under {} at the slot manager.",
                taskExecutorConnection.getResourceID(),
                taskExecutorConnection.getInstanceID());

        // we identify task managers by their instance id
        // 检查是否已经注册
        if (taskExecutorManager.isTaskManagerRegistered(taskExecutorConnection.getInstanceID())) {
            LOG.debug(
                    "Task executor {} was already registered.",
                    taskExecutorConnection.getResourceID());
            // 这里会尝试利用报告的新数据修改本地状态
            reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
            return RegistrationResult.IGNORED;
        } else {
            // 当slot太多时 无法注册
            if (!taskExecutorManager.registerTaskManager(
                    taskExecutorConnection,
                    initialSlotReport,
                    totalResourceProfile,
                    defaultSlotResourceProfile)) {
                LOG.debug(
                        "Task executor {} could not be registered.",
                        taskExecutorConnection.getResourceID());
                return RegistrationResult.REJECTED;
            }

            // register the new slots
            // 上面已经注册成功了
            for (SlotStatus slotStatus : initialSlotReport) {
                // 将哪些TaskExecutor 拥有哪些slot的信息保存到 slotTracker
                slotTracker.addSlot(
                        slotStatus.getSlotID(),
                        slotStatus.getResourceProfile(),
                        taskExecutorConnection,
                        slotStatus.getJobID());
            }

            checkResourceRequirementsWithDelay();
            return RegistrationResult.SUCCESS;
        }
    }

    /**
     * 注销某个worker
     * @param instanceId identifying the task manager to unregister
     * @param cause for unregistering the TaskManager
     * @return
     */
    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        checkInit();

        LOG.debug("Unregistering task executor {} from the slot manager.", instanceId);

        if (taskExecutorManager.isTaskManagerRegistered(instanceId)) {
            // 移除相关的slot  在这之前会通知slot变成空闲状态
            slotTracker.removeSlots(taskExecutorManager.getSlotsOf(instanceId));
            taskExecutorManager.unregisterTaskExecutor(instanceId);
            checkResourceRequirementsWithDelay();

            return true;
        } else {
            LOG.debug(
                    "There is no task executor registered with instance ID {}. Ignoring this message.",
                    instanceId);

            return false;
        }
    }

    /**
     * Reports the current slot allocations for a task manager identified by the given instance id.
     *
     * @param instanceId identifying the task manager for which to report the slot status
     * @param slotReport containing the status for all of its slots
     * @return true if the slot status has been updated successfully, otherwise false
     */
    @Override
    public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
        checkInit();

        LOG.debug("Received slot report from instance {}: {}.", instanceId, slotReport);

        if (taskExecutorManager.isTaskManagerRegistered(instanceId)) {
            // 更新slot的状态
            if (slotTracker.notifySlotStatus(slotReport)) {
                checkResourceRequirementsWithDelay();
            }
            return true;
        } else {
            LOG.debug(
                    "Received slot report for unknown task manager with instance id {}. Ignoring this report.",
                    instanceId);

            return false;
        }
    }

    /**
     * Free the given slot from the given allocation. If the slot is still allocated by the given
     * allocation id, then the slot will be marked as free and will be subject to new slot requests.
     *
     * @param slotId identifying the slot to free
     * @param allocationId with which the slot is presumably allocated
     *                     释放某个slot
     */
    @Override
    public void freeSlot(SlotID slotId, AllocationID allocationId) {
        checkInit();
        LOG.debug("Freeing slot {}.", slotId);

        slotTracker.notifyFree(slotId);
        checkResourceRequirementsWithDelay();
    }

    // ---------------------------------------------------------------------------------------------
    // Requirement matching
    // ---------------------------------------------------------------------------------------------

    /**
     * Depending on the implementation of {@link ResourceAllocationStrategy}, checking resource
     * requirements and potentially making a re-allocation can be heavy. In order to cover more
     * changes with each check, thus reduce the frequency of unnecessary re-allocations, the checks
     * are performed with a slight delay.
     *
     */
    private void checkResourceRequirementsWithDelay() {
        if (requirementsCheckDelay.toMillis() <= 0) {
            // 立即触发 or 延时触发资源检查
            checkResourceRequirements();
        } else {
            if (requirementsCheckFuture == null || requirementsCheckFuture.isDone()) {
                requirementsCheckFuture = new CompletableFuture<>();
                scheduledExecutor.schedule(
                        () ->
                                mainThreadExecutor.execute(
                                        () -> {
                                            checkResourceRequirements();
                                            Preconditions.checkNotNull(requirementsCheckFuture)
                                                    .complete(null);
                                        }),
                        requirementsCheckDelay.toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Matches resource requirements against available resources. In a first round requirements are
     * matched against free slot, and any match results in a slot allocation. The remaining
     * unfulfilled requirements are matched against pending slots, allocating more workers if no
     * matching pending slot could be found. If the requirements for a job could not be fulfilled
     * then a notification is sent to the job master informing it as such.
     *
     * <p>Performance notes: At it's core this method loops, for each job, over all free/pending
     * slots for each required slot, trying to find a matching slot. One should generally go in with
     * the assumption that this runs in numberOfJobsRequiringResources * numberOfRequiredSlots *
     * numberOfFreeOrPendingSlots. This is especially important when dealing with pending slots, as
     * matches between requirements and pending slots are not persisted and recomputed on each call.
     * This may required further refinements in the future; e.g., persisting the matches between
     * requirements and pending slots, or not matching against pending slots at all.
     *
     * <p>When dealing with unspecific resource profiles (i.e., {@link ResourceProfile#ANY}/{@link
     * ResourceProfile#UNKNOWN}), then the number of free/pending slots is not relevant because we
     * only need exactly 1 comparison to determine whether a slot can be fulfilled or not, since
     * they are all the same anyway.
     *
     * <p>When dealing with specific resource profiles things can be a lot worse, with the classical
     * cases where either no matches are found, or only at the very end of the iteration. In the
     * absolute worst case, with J jobs, requiring R slots each with a unique resource profile such
     * each pair of these profiles is not matching, and S free/pending slots that don't fulfill any
     * requirement, then this method does a total of J*R*S resource profile comparisons.
     *
     * <p>DO NOT call this method directly. Use {@link #checkResourceRequirementsWithDelay()}
     * instead.
     * 触发资源检查
     */
    private void checkResourceRequirements() {

        // 返回每个job需要的资源
        final Map<JobID, Collection<ResourceRequirement>> missingResources =
                resourceTracker.getMissingResources();

        // worker是为了job服务的 如果job资源足够 就不再需要worker了  所以清理pending的数据
        if (missingResources.isEmpty()) {
            taskExecutorManager.clearPendingTaskManagerSlots();
            return;
        }

        final Map<JobID, ResourceCounter> unfulfilledRequirements = new LinkedHashMap<>();
        for (Map.Entry<JobID, Collection<ResourceRequirement>> resourceRequirements :
                missingResources.entrySet()) {
            final JobID jobId = resourceRequirements.getKey();

            // 尝试为job分配资源  unfulfilledJobRequirements 表示这些资源没有空闲的slot可以调配
            final ResourceCounter unfulfilledJobRequirements =
                    tryAllocateSlotsForJob(jobId, resourceRequirements.getValue());
            if (!unfulfilledJobRequirements.isEmpty()) {
                unfulfilledRequirements.put(jobId, unfulfilledJobRequirements);
            }
        }
        if (unfulfilledRequirements.isEmpty()) {
            return;
        }

        // pending代表 已经发出的申请worker的请求资源 之后就会有新的worker注册上来  所以要排除pending的部分
        ResourceCounter freePendingSlots =
                ResourceCounter.withResources(
                        taskExecutorManager.getPendingTaskManagerSlots().stream()
                                .collect(
                                        Collectors.groupingBy(
                                                PendingTaskManagerSlot::getResourceProfile,
                                                Collectors.summingInt(x -> 1))));

        for (Map.Entry<JobID, ResourceCounter> unfulfilledRequirement :
                unfulfilledRequirements.entrySet()) {
            freePendingSlots =
                    tryFulfillRequirementsWithPendingSlots(
                            unfulfilledRequirement.getKey(),
                            unfulfilledRequirement.getValue().getResourcesWithCount(),
                            freePendingSlots);
        }

        if (!freePendingSlots.isEmpty()) {
            // 还有多出的请求资源
            taskExecutorManager.removePendingTaskManagerSlots(freePendingSlots);
        }
    }

    /**
     * 表示需要将这样的一组资源分配给job
     * @param jobId
     * @param missingResources  当前job缺失的资源
     * @return
     */
    private ResourceCounter tryAllocateSlotsForJob(
            JobID jobId, Collection<ResourceRequirement> missingResources) {
        ResourceCounter outstandingRequirements = ResourceCounter.empty();

        for (ResourceRequirement resourceRequirement : missingResources) {
            // numMissingSlots 表示此时还缺少的slot
            // internalTryAllocateSlots 只是负责将已经存在的slot和job绑定 (通过rpc通知TaskManager) 但是没有产生新的slot
            int numMissingSlots =
                    internalTryAllocateSlots(
                            jobId, jobMasterTargetAddresses.get(jobId), resourceRequirement);
            if (numMissingSlots > 0) {
                outstandingRequirements =
                        outstandingRequirements.add(
                                resourceRequirement.getResourceProfile(), numMissingSlots);
            }
        }
        return outstandingRequirements;
    }

    /**
     * Tries to allocate slots for the given requirement. If there are not enough slots available,
     * the resource manager is informed to allocate more resources.
     *
     * @param jobId job to allocate slots for
     * @param targetAddress address of the jobmaster
     * @param resourceRequirement required slots
     * @return the number of missing slots
     * 尝试为job分配资源 如果不足则要通知
     */
    private int internalTryAllocateSlots(
            JobID jobId, String targetAddress, ResourceRequirement resourceRequirement) {

        // 获取资源描述信息
        final ResourceProfile requiredResource = resourceRequirement.getResourceProfile();
        // Use LinkedHashMap to retain the original order
        final Map<SlotID, TaskManagerSlotInformation> availableSlots = new LinkedHashMap<>();

        // 先尝试整理出所有可用的slot   这些是TaskExecutor提供的
        for (TaskManagerSlotInformation freeSlot : slotTracker.getFreeSlots()) {
            // 如果发现该slot所在节点是阻塞节点  那么认为暂时不可用
            if (!isBlockedTaskManager(freeSlot.getTaskManagerConnection().getResourceID())) {
                availableSlots.put(freeSlot.getSlotId(), freeSlot);
            }
        }

        int numUnfulfilled = 0;
        for (int x = 0; x < resourceRequirement.getNumberOfRequiredSlots(); x++) {

            // 基于策略 找到一个合适的slot
            final Optional<TaskManagerSlotInformation> reservedSlot =
                    slotMatchingStrategy.findMatchingSlot(
                            requiredResource,
                            availableSlots.values(),
                            this::getNumberRegisteredSlotsOf);
            if (reservedSlot.isPresent()) {
                // 尝试将slot分配给某个job  涉及RPC调用
                allocateSlot(reservedSlot.get(), jobId, targetAddress, requiredResource);
                availableSlots.remove(reservedSlot.get().getSlotId());
            } else {
                // exit loop early; we won't find a matching slot for this requirement
                // 表示无法被分配的
                int numRemaining = resourceRequirement.getNumberOfRequiredSlots() - x;
                numUnfulfilled += numRemaining;
                break;
            }
        }
        return numUnfulfilled;
    }

    private boolean isBlockedTaskManager(ResourceID resourceID) {
        Preconditions.checkNotNull(blockedTaskManagerChecker);
        return blockedTaskManagerChecker.isBlockedTaskManager(resourceID);
    }

    /**
     * Allocates the given slot. This entails sending a registration message to the task manager and
     * treating failures.
     *
     * @param taskManagerSlot slot to allocate
     * @param jobId job for which the slot should be allocated for
     * @param targetAddress address of the job master   该job关联的jobMaster地址
     * @param resourceProfile resource profile for the requirement for which the slot is used
     *                        将某个slot 分配给job
     */
    private void allocateSlot(
            TaskManagerSlotInformation taskManagerSlot,
            JobID jobId,
            String targetAddress,
            ResourceProfile resourceProfile) {
        final SlotID slotId = taskManagerSlot.getSlotId();
        LOG.debug(
                "Starting allocation of slot {} for job {} with resource profile {}.",
                slotId,
                jobId,
                resourceProfile);

        final InstanceID instanceId = taskManagerSlot.getInstanceId();
        if (!taskExecutorManager.isTaskManagerRegistered(instanceId)) {
            throw new IllegalStateException(
                    "Could not find a registered task manager for instance id " + instanceId + '.');
        }

        // 通过网关发送请求
        final TaskExecutorConnection taskExecutorConnection =
                taskManagerSlot.getTaskManagerConnection();
        final TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

        final AllocationID allocationId = new AllocationID();

        slotTracker.notifyAllocationStart(slotId, jobId);
        taskExecutorManager.markUsed(instanceId);
        // 该容器存储分配中的slot
        pendingSlotAllocations.put(slotId, allocationId);

        // RPC call to the task manager
        CompletableFuture<Acknowledge> requestFuture =
                gateway.requestSlot(
                        slotId,
                        jobId,
                        allocationId,
                        resourceProfile,
                        targetAddress,
                        resourceManagerId,
                        taskManagerRequestTimeout);

        CompletableFuture<Void> slotAllocationResponseProcessingFuture =
                requestFuture.handleAsync(
                        (Acknowledge acknowledge, Throwable throwable) -> {
                            final AllocationID currentAllocationForSlot =
                                    pendingSlotAllocations.get(slotId);
                            // 忽略这种情况
                            if (currentAllocationForSlot == null
                                    || !currentAllocationForSlot.equals(allocationId)) {
                                LOG.debug(
                                        "Ignoring slot allocation update from task executor {} for slot {} and job {}, because the allocation was already completed or cancelled.",
                                        instanceId,
                                        slotId,
                                        jobId);
                                return null;
                            }
                            if (acknowledge != null) {
                                LOG.trace(
                                        "Completed allocation of slot {} for job {}.",
                                        slotId,
                                        jobId);
                                // 告知完成了
                                slotTracker.notifyAllocationComplete(slotId, jobId);
                            } else {
                                if (throwable instanceof SlotOccupiedException) {
                                    SlotOccupiedException exception =
                                            (SlotOccupiedException) throwable;
                                    LOG.debug(
                                            "Tried allocating slot {} for job {}, but it was already allocated for job {}.",
                                            slotId,
                                            jobId,
                                            exception.getJobId());
                                    // report as a slot status to force the state transition
                                    // this could be a problem if we ever assume that the task
                                    // executor always reports about all slots
                                    // 表示已经分配给别人了
                                    slotTracker.notifySlotStatus(
                                            Collections.singleton(
                                                    new SlotStatus(
                                                            slotId,
                                                            taskManagerSlot.getResourceProfile(),
                                                            exception.getJobId(),
                                                            exception.getAllocationId())));
                                } else {
                                    LOG.warn(
                                            "Slot allocation for slot {} for job {} failed.",
                                            slotId,
                                            jobId,
                                            throwable);
                                    // 分配失败 退回到free
                                    slotTracker.notifyFree(slotId);
                                }
                                checkResourceRequirementsWithDelay();
                            }
                            return null;
                        },
                        mainThreadExecutor);
        FutureUtils.assertNoException(slotAllocationResponseProcessingFuture);
    }

    /**
     * missing的部分要取消pending的部分
     * @param jobId
     * @param missingResources
     * @param pendingSlots
     * @return
     */
    private ResourceCounter tryFulfillRequirementsWithPendingSlots(
            JobID jobId,
            Collection<Map.Entry<ResourceProfile, Integer>> missingResources,
            ResourceCounter pendingSlots) {
        for (Map.Entry<ResourceProfile, Integer> missingResource : missingResources) {
            ResourceProfile profile = missingResource.getKey();
            for (int i = 0; i < missingResource.getValue(); i++) {

                // 将pending 与需要的资源比较
                final MatchingResult matchingResult =
                        tryFulfillWithPendingSlots(profile, pendingSlots);

                // 表示剩余可用的slot
                pendingSlots = matchingResult.getNewAvailableResources();

                // 表示找不到匹配的资源  需要分配新的 worker
                if (!matchingResult.isSuccessfulMatching()) {
                    final WorkerAllocationResult allocationResult =
                            tryAllocateWorkerAndReserveSlot(profile, pendingSlots);
                    pendingSlots = allocationResult.getNewAvailableResources();

                    // 分配失败了  通过监听器告知没有资源可用
                    if (!allocationResult.isSuccessfulAllocating()
                            && sendNotEnoughResourceNotifications) {
                        LOG.warn(
                                "Could not fulfill resource requirements of job {}. Free slots: {}",
                                jobId,
                                slotTracker.getFreeSlots().size());
                        resourceEventListener.notEnoughResourceAvailable(
                                jobId, resourceTracker.getAcquiredResources(jobId));
                        return pendingSlots;
                    }
                }
            }
        }
        return pendingSlots;
    }

    /**
     * 尝试用pending数据找到对应的resourceProfile
     * @param resourceProfile
     * @param pendingSlots
     * @return
     */
    private MatchingResult tryFulfillWithPendingSlots(
            ResourceProfile resourceProfile, ResourceCounter pendingSlots) {
        Set<ResourceProfile> pendingSlotProfiles = pendingSlots.getResources();

        // short-cut, pretty much only applicable to fine-grained resource management
        if (pendingSlotProfiles.contains(resourceProfile)) {
            pendingSlots = pendingSlots.subtract(resourceProfile, 1);
            return new MatchingResult(true, pendingSlots);
        }

        for (ResourceProfile pendingSlotProfile : pendingSlotProfiles) {
            if (pendingSlotProfile.isMatching(resourceProfile)) {
                pendingSlots = pendingSlots.subtract(pendingSlotProfile, 1);
                return new MatchingResult(true, pendingSlots);
            }
        }

        return new MatchingResult(false, pendingSlots);
    }

    /**
     * 产生一个表示 需要worker的结果
     * @param profile
     * @param pendingSlots
     * @return
     */
    private WorkerAllocationResult tryAllocateWorkerAndReserveSlot(
            ResourceProfile profile, ResourceCounter pendingSlots) {

        // 发出请求 并得到对应的预期资源
        Optional<ResourceRequirement> newlyFulfillableRequirements =
                taskExecutorManager.allocateWorker(profile);
        if (newlyFulfillableRequirements.isPresent()) {
            ResourceRequirement newSlots = newlyFulfillableRequirements.get();
            // reserve one of the new slots  需要给它至少保留一个
            if (newSlots.getNumberOfRequiredSlots() > 1) {
                pendingSlots =
                        pendingSlots.add(
                                newSlots.getResourceProfile(),
                                newSlots.getNumberOfRequiredSlots() - 1);
            }
            return new WorkerAllocationResult(true, pendingSlots);
        } else {
            return new WorkerAllocationResult(false, pendingSlots);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Legacy APIs
    // ---------------------------------------------------------------------------------------------

    @Override
    public int getNumberRegisteredSlots() {
        return taskExecutorManager.getNumberRegisteredSlots();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return taskExecutorManager.getNumberRegisteredSlotsOf(instanceId);
    }

    @Override
    public int getNumberFreeSlots() {
        return taskExecutorManager.getNumberFreeSlots();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return taskExecutorManager.getNumberFreeSlotsOf(instanceId);
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return taskExecutorManager.getTotalRegisteredResources();
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
        return taskExecutorManager.getTotalRegisteredResourcesOf(instanceID);
    }

    @Override
    public ResourceProfile getFreeResource() {
        return taskExecutorManager.getTotalFreeResources();
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
        return taskExecutorManager.getTotalFreeResourcesOf(instanceID);
    }

    @Override
    public Collection<SlotInfo> getAllocatedSlotsOf(InstanceID instanceID) {
        // This information is currently not supported for this slot manager.
        return Collections.emptyList();
    }

    // ---------------------------------------------------------------------------------------------
    // Internal utility methods
    // ---------------------------------------------------------------------------------------------

    private void checkInit() {
        Preconditions.checkState(started, "The slot manager has not been started.");
    }

    /**
     * 表示一个匹配结果
     */
    private static class MatchingResult {
        private final boolean isSuccessfulMatching;
        /**
         * 此时可用的资源
         */
        private final ResourceCounter newAvailableResources;

        private MatchingResult(
                boolean isSuccessfulMatching, ResourceCounter newAvailableResources) {
            this.isSuccessfulMatching = isSuccessfulMatching;
            this.newAvailableResources = Preconditions.checkNotNull(newAvailableResources);
        }

        private ResourceCounter getNewAvailableResources() {
            return newAvailableResources;
        }

        private boolean isSuccessfulMatching() {
            return isSuccessfulMatching;
        }
    }

    /**
     * 表示分配的结果
     */
    private static class WorkerAllocationResult {
        private final boolean isSuccessfulAllocating;
        private final ResourceCounter newAvailableResources;

        private WorkerAllocationResult(
                boolean isSuccessfulAllocating, ResourceCounter newAvailableResources) {
            this.isSuccessfulAllocating = isSuccessfulAllocating;
            this.newAvailableResources = Preconditions.checkNotNull(newAvailableResources);
        }

        private ResourceCounter getNewAvailableResources() {
            return newAvailableResources;
        }

        private boolean isSuccessfulAllocating() {
            return isSuccessfulAllocating;
        }
    }
}
