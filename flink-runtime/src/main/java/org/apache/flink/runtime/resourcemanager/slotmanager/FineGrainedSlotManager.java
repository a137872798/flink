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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.rest.messages.taskmanager.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Implementation of {@link SlotManager} supporting fine-grained resource management.
 * 进行细粒度的资源分配
 * */
public class FineGrainedSlotManager implements SlotManager {
    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedSlotManager.class);

    /**
     * 该对象用于追踪每个TM的资源使用量
     */
    private final TaskManagerTracker taskManagerTracker;

    /**
     * 追踪每个job的资源需求量
     */
    private final ResourceTracker resourceTracker;

    /**
     * 通过该对象进行资源分配
     */
    private final ResourceAllocationStrategy resourceAllocationStrategy;

    /**
     * 该对象负责同步 tracker对象
     */
    private final SlotStatusSyncer slotStatusSyncer;

    /** Scheduled executor for timeouts. */
    private final ScheduledExecutor scheduledExecutor;

    /** Timeout after which an unused TaskManager is released. */
    private final Time taskManagerTimeout;

    /** Delay of the requirement change check in the slot manager. */
    private final Duration requirementsCheckDelay;

    /** Delay of the resource declare in the slot manager. */
    private final Duration declareNeededResourceDelay;

    private final SlotManagerMetricGroup slotManagerMetricGroup;

    private final Map<JobID, String> jobMasterTargetAddresses = new HashMap<>();

    /**
     * Release task executor only when each produced result partition is either consumed or failed.
     */
    private final boolean waitResultConsumedBeforeRelease;

    private final CPUResource maxTotalCpu;
    private final MemorySize maxTotalMem;

    private boolean sendNotEnoughResourceNotifications = true;

    /**
     * 记录未满足的job
     */
    private final Set<JobID> unfulfillableJobs = new HashSet<>();

    /** ResourceManager's id. */
    @Nullable private ResourceManagerId resourceManagerId;

    /** Executor for future callbacks which have to be "synchronized". */
    @Nullable private Executor mainThreadExecutor;

    /** Callbacks for resource (de-)allocations.
     * 资源分配器对象  可以通过该对象声明需要的资源
     * */
    @Nullable private ResourceAllocator resourceAllocator;

    /** Callbacks for resource not enough.
     * 用于通知job资源不足
     * */
    @Nullable private ResourceEventListener resourceEventListener;

    @Nullable private ScheduledFuture<?> clusterReconciliationCheck;

    @Nullable private CompletableFuture<Void> requirementsCheckFuture;

    @Nullable private CompletableFuture<Void> declareNeededResourceFuture;

    /** Blocked task manager checker.
     * 检查该节点是否繁忙
     * */
    @Nullable private BlockedTaskManagerChecker blockedTaskManagerChecker;

    /** True iff the component has been started. */
    private boolean started;

    public FineGrainedSlotManager(
            ScheduledExecutor scheduledExecutor,
            SlotManagerConfiguration slotManagerConfiguration,
            SlotManagerMetricGroup slotManagerMetricGroup,
            ResourceTracker resourceTracker,
            TaskManagerTracker taskManagerTracker,
            SlotStatusSyncer slotStatusSyncer,
            ResourceAllocationStrategy resourceAllocationStrategy) {

        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);

        Preconditions.checkNotNull(slotManagerConfiguration);
        this.taskManagerTimeout = slotManagerConfiguration.getTaskManagerTimeout();
        this.waitResultConsumedBeforeRelease =
                slotManagerConfiguration.isWaitResultConsumedBeforeRelease();
        this.requirementsCheckDelay =
                Preconditions.checkNotNull(slotManagerConfiguration.getRequirementCheckDelay());
        this.declareNeededResourceDelay =
                Preconditions.checkNotNull(
                        slotManagerConfiguration.getDeclareNeededResourceDelay());

        this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);

        this.resourceTracker = Preconditions.checkNotNull(resourceTracker);
        this.taskManagerTracker = Preconditions.checkNotNull(taskManagerTracker);
        this.slotStatusSyncer = Preconditions.checkNotNull(slotStatusSyncer);
        this.resourceAllocationStrategy = Preconditions.checkNotNull(resourceAllocationStrategy);

        this.maxTotalCpu = Preconditions.checkNotNull(slotManagerConfiguration.getMaxTotalCpu());
        this.maxTotalMem = Preconditions.checkNotNull(slotManagerConfiguration.getMaxTotalMem());

        resourceManagerId = null;
        resourceAllocator = null;
        resourceEventListener = null;
        mainThreadExecutor = null;
        clusterReconciliationCheck = null;
        requirementsCheckFuture = null;

        started = false;
    }

    @Override
    public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
        checkInit();
        // this sets up a grace period, e.g., when the cluster was started, to give task executors
        // time to connect
        sendNotEnoughResourceNotifications = failUnfulfillableRequest;

        if (failUnfulfillableRequest && !unfulfillableJobs.isEmpty()) {
            for (JobID jobId : unfulfillableJobs) {
                // 通知资源不足
                resourceEventListener.notEnoughResourceAvailable(
                        jobId, resourceTracker.getAcquiredResources(jobId));
            }
        }
    }

    /**
     * 触发一次资源检查
     */
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
     */
    @Override
    public void start(
            ResourceManagerId newResourceManagerId,
            Executor newMainThreadExecutor,
            ResourceAllocator newResourceAllocator,
            ResourceEventListener newResourceEventListener,
            BlockedTaskManagerChecker newBlockedTaskManagerChecker) {
        LOG.info("Starting the slot manager.");

        // 设置各组件
        resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
        mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
        resourceAllocator = Preconditions.checkNotNull(newResourceAllocator);
        resourceEventListener = Preconditions.checkNotNull(newResourceEventListener);
        slotStatusSyncer.initialize(
                taskManagerTracker, resourceTracker, resourceManagerId, mainThreadExecutor);
        blockedTaskManagerChecker = Preconditions.checkNotNull(newBlockedTaskManagerChecker);

        started = true;

        // 支持分配资源 就周期性检查
        if (resourceAllocator.isSupported()) {
            clusterReconciliationCheck =
                    scheduledExecutor.scheduleWithFixedDelay(
                            () -> mainThreadExecutor.execute(this::checkClusterReconciliation),
                            0L,
                            taskManagerTimeout.toMilliseconds(),
                            TimeUnit.MILLISECONDS);
        }

        registerSlotManagerMetrics();
    }

    private void registerSlotManagerMetrics() {
        slotManagerMetricGroup.gauge(
                MetricNames.TASK_SLOTS_AVAILABLE, () -> (long) getNumberFreeSlots());
        slotManagerMetricGroup.gauge(
                MetricNames.TASK_SLOTS_TOTAL, () -> (long) getNumberRegisteredSlots());
    }

    /** Suspends the component. This clears the internal state of the slot manager. */
    @Override
    public void suspend() {
        if (!started) {
            return;
        }

        LOG.info("Suspending the slot manager.");

        slotManagerMetricGroup.close();

        // stop the timeout checks for the TaskManagers
        if (clusterReconciliationCheck != null) {
            clusterReconciliationCheck.cancel(false);
            clusterReconciliationCheck = null;
        }

        slotStatusSyncer.close();
        taskManagerTracker.clear();
        resourceTracker.clear();

        unfulfillableJobs.clear();
        resourceManagerId = null;
        resourceAllocator = null;
        resourceEventListener = null;
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
     * 清理某个job相关的资源
     * @param jobId job for which to clear the requirements
     */
    @Override
    public void clearResourceRequirements(JobID jobId) {
        maybeReclaimInactiveSlots(jobId);
        jobMasterTargetAddresses.remove(jobId);
        // 将维护的job需要的资源置空
        resourceTracker.notifyResourceRequirements(jobId, Collections.emptyList());
        if (resourceAllocator.isSupported()) {
            // 将资源归还到 taskManager
            taskManagerTracker.clearPendingAllocationsOfJob(jobId);
            checkResourcesNeedReconcile();
            // 调配资源
            declareNeededResourcesWithDelay();
        }
    }

    private void maybeReclaimInactiveSlots(JobID jobId) {
        // We don't notify the task manager tracker and resource tracker about the freeing of these
        // slots early, and rely on task managers to report the slots becoming available later to
        // keep the states consistent.
        if (!resourceTracker.getAcquiredResources(jobId).isEmpty()) {
            // 内部会通过gateway对象通知
            slotStatusSyncer.freeInactiveSlots(jobId);
        }
    }

    /**
     * 处理资源的请求
     * @param resourceRequirements resource requirements of a job
     */
    @Override
    public void processResourceRequirements(ResourceRequirements resourceRequirements) {
        checkInit();
        if (resourceRequirements.getResourceRequirements().isEmpty()
                && resourceTracker.isRequirementEmpty(resourceRequirements.getJobId())) {
            // Skip duplicate empty resource requirements.
            return;
        }

        if (resourceRequirements.getResourceRequirements().isEmpty()) {
            LOG.info("Clearing resource requirements of job {}", resourceRequirements.getJobId());
            jobMasterTargetAddresses.remove(resourceRequirements.getJobId());
            if (resourceAllocator.isSupported()) {
                taskManagerTracker.clearPendingAllocationsOfJob(resourceRequirements.getJobId());
            }
        } else {
            LOG.info(
                    "Received resource requirements from job {}: {}",
                    resourceRequirements.getJobId(),
                    resourceRequirements.getResourceRequirements());
            // 根据请求信息填充job地址
            jobMasterTargetAddresses.put(
                    resourceRequirements.getJobId(), resourceRequirements.getTargetAddress());
        }

        // 记录资源需求
        resourceTracker.notifyResourceRequirements(
                resourceRequirements.getJobId(), resourceRequirements.getResourceRequirements());
        checkResourceRequirementsWithDelay();
    }

    /**
     * 当产生了一个TM后  注册到本对象相关的组件
     * @param taskExecutorConnection for the new task manager
     * @param initialSlotReport for the new task manager
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
        LOG.info(
                "Registering task executor {} under {} at the slot manager.",
                taskExecutorConnection.getResourceID(),
                taskExecutorConnection.getInstanceID());

        // we identify task managers by their instance id
        if (taskManagerTracker
                .getRegisteredTaskManager(taskExecutorConnection.getInstanceID())
                .isPresent()) {
            LOG.debug(
                    "Task executor {} was already registered.",
                    taskExecutorConnection.getResourceID());
            // 更新slot状态
            reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
            return RegistrationResult.IGNORED;
        } else {
            Optional<PendingTaskManager> matchedPendingTaskManagerOptional =
                    initialSlotReport.hasAllocatedSlot()
                            ? Optional.empty()
                            : findMatchingPendingTaskManager(
                                    totalResourceProfile, defaultSlotResourceProfile);

            if (!matchedPendingTaskManagerOptional.isPresent()
                    && isMaxTotalResourceExceededAfterAdding(totalResourceProfile)) {

                LOG.info(
                        "Can not register task manager {}. The max total resource limitation <{}, {}> is reached.",
                        taskExecutorConnection.getResourceID(),
                        maxTotalCpu,
                        maxTotalMem.toHumanReadableString());
                return RegistrationResult.REJECTED;
            }

            // 注册该TM
            taskManagerTracker.addTaskManager(
                    taskExecutorConnection, totalResourceProfile, defaultSlotResourceProfile);

            // 表示该TM的一些资源被预先分配了  使用report信息进行同步
            if (initialSlotReport.hasAllocatedSlot()) {
                slotStatusSyncer.reportSlotStatus(
                        taskExecutorConnection.getInstanceID(), initialSlotReport);
            }

            if (matchedPendingTaskManagerOptional.isPresent()) {
                PendingTaskManager pendingTaskManager = matchedPendingTaskManagerOptional.get();
                allocateSlotsForRegisteredPendingTaskManager(
                        pendingTaskManager, taskExecutorConnection.getInstanceID());
                taskManagerTracker.removePendingTaskManager(
                        pendingTaskManager.getPendingTaskManagerId());
                return RegistrationResult.SUCCESS;
            }

            // 因为新得到了资源 之后可以检查资源是否足够了
            checkResourceRequirementsWithDelay();
            return RegistrationResult.SUCCESS;
        }
    }

    private void declareNeededResourcesWithDelay() {
        Preconditions.checkState(resourceAllocator.isSupported());

        if (declareNeededResourceDelay.toMillis() <= 0) {
            declareNeededResources();
        } else {
            if (declareNeededResourceFuture == null || declareNeededResourceFuture.isDone()) {
                declareNeededResourceFuture = new CompletableFuture<>();
                scheduledExecutor.schedule(
                        () ->
                                mainThreadExecutor.execute(
                                        () -> {
                                            declareNeededResources();
                                            Preconditions.checkNotNull(declareNeededResourceFuture)
                                                    .complete(null);
                                        }),
                        declareNeededResourceDelay.toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /** DO NOT call this method directly. Use {@link #declareNeededResourcesWithDelay()} instead.
     * 声明此时需要的资源
     * */
    private void declareNeededResources() {

        // 找到期望回收的TM
        Map<InstanceID, WorkerResourceSpec> unWantedTaskManagers =
                taskManagerTracker.getUnWantedTaskManager();

        // 按照资源量分组
        Map<WorkerResourceSpec, Set<InstanceID>> unWantedTaskManagerBySpec =
                unWantedTaskManagers.entrySet().stream()
                        .collect(
                                Collectors.groupingBy(
                                        Map.Entry::getValue,
                                        Collectors.mapping(Map.Entry::getKey, Collectors.toSet())));

        // registered TaskManagers except unwanted worker.
        // 找到当前TM能够提供的资源量
        Stream<WorkerResourceSpec> registeredTaskManagerStream =
                taskManagerTracker.getRegisteredTaskManagers().stream()
                        .filter(t -> !unWantedTaskManagers.containsKey(t.getInstanceId()))
                        .map(
                                t ->
                                        WorkerResourceSpec.fromTotalResourceProfile(
                                                t.getTotalResource(), t.getDefaultNumSlots()));
        // pending TaskManagers.
        // 计算pendingTM的资源量
        Stream<WorkerResourceSpec> pendingTaskManagerStream =
                taskManagerTracker.getPendingTaskManagers().stream()
                        .map(
                                t ->
                                        WorkerResourceSpec.fromTotalResourceProfile(
                                                t.getTotalResourceProfile(), t.getNumSlots()));

        // 这是现在需要的资源量
        Map<WorkerResourceSpec, Integer> requiredWorkers =
                Stream.concat(registeredTaskManagerStream, pendingTaskManagerStream)
                        .collect(
                                Collectors.groupingBy(
                                        Function.identity(), Collectors.summingInt(e -> 1)));

        Set<WorkerResourceSpec> workerResourceSpecs = new HashSet<>(requiredWorkers.keySet());
        workerResourceSpecs.addAll(unWantedTaskManagerBySpec.keySet());

        // 转换成资源声明对象 并通过资源分配器处理
        List<ResourceDeclaration> resourceDeclarations = new ArrayList<>();
        workerResourceSpecs.forEach(
                spec ->
                        resourceDeclarations.add(
                                new ResourceDeclaration(
                                        spec,
                                        requiredWorkers.getOrDefault(spec, 0),
                                        unWantedTaskManagerBySpec.getOrDefault(
                                                spec, Collections.emptySet()))));

        resourceAllocator.declareResourceNeeded(resourceDeclarations);
    }

    /**
     * 找到之前预分配的TM
     * @param pendingTaskManager
     * @param instanceId
     */
    private void allocateSlotsForRegisteredPendingTaskManager(
            PendingTaskManager pendingTaskManager, InstanceID instanceId) {
        Map<JobID, Map<InstanceID, ResourceCounter>> allocations =
                pendingTaskManager.getPendingSlotAllocationRecords().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        (e) -> Collections.singletonMap(instanceId, e.getValue())));
        allocateSlotsAccordingTo(allocations);
    }

    /**
     * 找到匹配的提前分配的
     * @param totalResourceProfile
     * @param defaultSlotResourceProfile
     * @return
     */
    private Optional<PendingTaskManager> findMatchingPendingTaskManager(
            ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile) {

        // 找到pendingTM
        Collection<PendingTaskManager> matchedPendingTaskManagers =
                taskManagerTracker.getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
                        totalResourceProfile, defaultSlotResourceProfile);

        // 找到预先分配过资源的 TM
        Optional<PendingTaskManager> matchedPendingTaskManagerIdsWithAllocatedSlots =
                matchedPendingTaskManagers.stream()
                        .filter(
                                (pendingTaskManager) ->
                                        !pendingTaskManager
                                                .getPendingSlotAllocationRecords()
                                                .isEmpty())
                        .findAny();

        if (matchedPendingTaskManagerIdsWithAllocatedSlots.isPresent()) {
            return matchedPendingTaskManagerIdsWithAllocatedSlots;
        } else {
            return matchedPendingTaskManagers.stream().findAny();
        }
    }

    /**
     * 注销某个TM
     * @param instanceId identifying the task manager to unregister
     * @param cause for unregistering the TaskManager
     * @return
     */
    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        checkInit();

        LOG.info("Unregistering task executor {} from the slot manager.", instanceId);

        if (taskManagerTracker.getRegisteredTaskManager(instanceId).isPresent()) {
            // 获取已经分配的slot
            Set<AllocationID> allocatedSlots =
                    new HashSet<>(
                            taskManagerTracker
                                    .getRegisteredTaskManager(instanceId)
                                    .get()
                                    .getAllocatedSlots()
                                    .keySet());
            for (AllocationID allocationId : allocatedSlots) {
                slotStatusSyncer.freeSlot(allocationId);
            }
            taskManagerTracker.removeTaskManager(instanceId);
            if (!allocatedSlots.isEmpty()) {
                checkResourceRequirementsWithDelay();
            }

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

        if (taskManagerTracker.getRegisteredTaskManager(instanceId).isPresent()) {
            if (!slotStatusSyncer.reportSlotStatus(instanceId, slotReport)) {
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
     * allocation id, then the slot will be freed.
     *
     * @param slotId identifying the slot to free, will be ignored
     * @param allocationId with which the slot is presumably allocated
     */
    @Override
    public void freeSlot(SlotID slotId, AllocationID allocationId) {
        checkInit();
        LOG.debug("Freeing slot {}.", allocationId);

        if (taskManagerTracker.getAllocatedOrPendingSlot(allocationId).isPresent()) {
            slotStatusSyncer.freeSlot(allocationId);
            checkResourceRequirementsWithDelay();
        } else {
            LOG.debug(
                    "Trying to free a slot {} which has not been allocated. Ignoring this message.",
                    allocationId);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Requirement matching
    // ---------------------------------------------------------------------------------------------

    /**
     * Depending on the implementation of {@link ResourceAllocationStrategy}, checking resource
     * requirements and potentially making a re-allocation can be heavy. In order to cover more
     * changes with each check, thus reduce the frequency of unnecessary re-allocations, the checks
     * are performed with a slight delay.
     */
    private void checkResourceRequirementsWithDelay() {
        if (requirementsCheckDelay.toMillis() <= 0) {
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
     * DO NOT call this method directly. Use {@link #checkResourceRequirementsWithDelay()} instead.
     * 检查资源是否足够 并进行分配
     */
    private void checkResourceRequirements() {
        if (!started) {
            return;
        }

        // 获取缺失的资源
        Map<JobID, Collection<ResourceRequirement>> missingResources =
                resourceTracker.getMissingResources();
        if (missingResources.isEmpty()) {
            if (resourceAllocator.isSupported()
                    && !taskManagerTracker.getPendingTaskManagers().isEmpty()) {
                // 因为资源足够了  不需要预支
                taskManagerTracker.replaceAllPendingAllocations(Collections.emptyMap());
                checkResourcesNeedReconcile();
                declareNeededResourcesWithDelay();
            }
            return;
        }

        // 日志
        logMissingAndAvailableResource(missingResources);

        missingResources =
                missingResources.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> new ArrayList<>(e.getValue())));

        // 尝试分配资源
        final ResourceAllocationResult result =
                resourceAllocationStrategy.tryFulfillRequirements(
                        missingResources, taskManagerTracker, this::isBlockedTaskManager);

        // Allocate slots according to the result
        // 此时产生了分配结果
        allocateSlotsAccordingTo(result.getAllocationsOnRegisteredResources());

        final Set<PendingTaskManagerId> failAllocations;
        if (resourceAllocator.isSupported()) {
            // Allocate task managers according to the result
            // 表示预支了TM
            failAllocations =
                    allocateTaskManagersAccordingTo(result.getPendingTaskManagersToAllocate());

            // Record slot allocation of pending task managers
            // 描述如何分配
            final Map<PendingTaskManagerId, Map<JobID, ResourceCounter>>
                    pendingResourceAllocationResult =
                            new HashMap<>(result.getAllocationsOnPendingResources());
            pendingResourceAllocationResult.keySet().removeAll(failAllocations);
            // 清理掉失败的
            taskManagerTracker.replaceAllPendingAllocations(pendingResourceAllocationResult);
        } else {
            failAllocations =
                    result.getPendingTaskManagersToAllocate().stream()
                            .map(PendingTaskManager::getPendingTaskManagerId)
                            .collect(Collectors.toSet());
        }

        unfulfillableJobs.clear();
        unfulfillableJobs.addAll(result.getUnfulfillableJobs());
        for (PendingTaskManagerId pendingTaskManagerId : failAllocations) {
            unfulfillableJobs.addAll(
                    result.getAllocationsOnPendingResources().get(pendingTaskManagerId).keySet());
        }
        // Notify jobs that can not be fulfilled
        // 通知资源不足
        if (sendNotEnoughResourceNotifications) {
            for (JobID jobId : unfulfillableJobs) {
                LOG.warn("Could not fulfill resource requirements of job {}.", jobId);
                resourceEventListener.notEnoughResourceAvailable(
                        jobId, resourceTracker.getAcquiredResources(jobId));
            }
        }

        if (resourceAllocator.isSupported()) {
            // 先调整 之后检测资源是否足够
            checkResourcesNeedReconcile();
            declareNeededResourcesWithDelay();
        }
    }

    private void logMissingAndAvailableResource(
            Map<JobID, Collection<ResourceRequirement>> missingResources) {
        final StringJoiner lines = new StringJoiner(System.lineSeparator());
        lines.add("Matching resource requirements against available resources.");
        lines.add("Missing resources:");
        missingResources.forEach(
                (jobId, resources) -> {
                    lines.add("\t Job " + jobId);
                    resources.forEach(resource -> lines.add(String.format("\t\t%s", resource)));
                });
        lines.add("Current resources:");
        if (taskManagerTracker.getRegisteredTaskManagers().isEmpty()) {
            lines.add("\t(none)");
        } else {
            for (TaskManagerInfo taskManager : taskManagerTracker.getRegisteredTaskManagers()) {
                final ResourceID resourceId =
                        taskManager.getTaskExecutorConnection().getResourceID();
                lines.add("\tTaskManager " + resourceId);
                lines.add("\t\tAvailable: " + taskManager.getAvailableResource());
                lines.add("\t\tTotal:     " + taskManager.getTotalResource());
            }
        }
        LOG.info(lines.toString());
    }

    /**
     * 根据结果分配资源
     * @param result
     */
    private void allocateSlotsAccordingTo(Map<JobID, Map<InstanceID, ResourceCounter>> result) {
        final List<CompletableFuture<Void>> allocationFutures = new ArrayList<>();
        for (Map.Entry<JobID, Map<InstanceID, ResourceCounter>> jobEntry : result.entrySet()) {
            final JobID jobID = jobEntry.getKey();
            for (Map.Entry<InstanceID, ResourceCounter> tmEntry : jobEntry.getValue().entrySet()) {
                final InstanceID instanceID = tmEntry.getKey();
                for (Map.Entry<ResourceProfile, Integer> slotEntry :
                        tmEntry.getValue().getResourcesWithCount()) {
                    for (int i = 0; i < slotEntry.getValue(); ++i) {
                        allocationFutures.add(
                                // 这里会发起 rpc调用
                                slotStatusSyncer.allocateSlot(
                                        instanceID,
                                        jobID,
                                        jobMasterTargetAddresses.get(jobID),
                                        slotEntry.getKey()));
                    }
                }
            }
        }
        FutureUtils.combineAll(allocationFutures)
                .whenCompleteAsync(
                        (s, t) -> {
                            if (t != null) {
                                // If there is allocation failure, we need to trigger it again.
                                checkResourceRequirementsWithDelay();
                            }
                        },
                        mainThreadExecutor);
    }

    /**
     * Allocate pending task managers, returns the ids of pending task managers that can not be
     * allocated.
     * 使用 pending TM 分配资源
     */
    private Set<PendingTaskManagerId> allocateTaskManagersAccordingTo(
            List<PendingTaskManager> pendingTaskManagers) {
        Preconditions.checkState(resourceAllocator.isSupported());
        final Set<PendingTaskManagerId> failedAllocations = new HashSet<>();
        for (PendingTaskManager pendingTaskManager : pendingTaskManagers) {
            if (!allocateResource(pendingTaskManager)) {
                failedAllocations.add(pendingTaskManager.getPendingTaskManagerId());
            }
        }
        return failedAllocations;
    }

    // ---------------------------------------------------------------------------------------------
    // Legacy APIs
    // ---------------------------------------------------------------------------------------------

    @Override
    public int getNumberRegisteredSlots() {
        return taskManagerTracker.getNumberRegisteredSlots();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return taskManagerTracker.getNumberRegisteredSlotsOf(instanceId);
    }

    @Override
    public int getNumberFreeSlots() {
        return taskManagerTracker.getNumberFreeSlots();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return taskManagerTracker.getNumberFreeSlotsOf(instanceId);
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return taskManagerTracker.getRegisteredResource();
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
        return taskManagerTracker.getRegisteredResourceOf(instanceID);
    }

    @Override
    public ResourceProfile getFreeResource() {
        return taskManagerTracker.getFreeResource();
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
        return taskManagerTracker.getFreeResourceOf(instanceID);
    }

    @Override
    public Collection<SlotInfo> getAllocatedSlotsOf(InstanceID instanceID) {
        return taskManagerTracker.getRegisteredTaskManager(instanceID)
                .map(TaskManagerInfo::getAllocatedSlots).map(Map::values)
                .orElse(Collections.emptyList()).stream()
                .map(slot -> new SlotInfo(slot.getJobId(), slot.getResourceProfile()))
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------------------------------------
    // Internal periodic check methods
    // ---------------------------------------------------------------------------------------------

    private void checkClusterReconciliation() {
        if (checkResourcesNeedReconcile()) {
            // only declare on needed.
            declareNeededResourcesWithDelay();
        }
    }

    /**
     * 检查资源是否需要调整
     * @return
     */
    private boolean checkResourcesNeedReconcile() {
        ResourceReconcileResult reconcileResult =
                resourceAllocationStrategy.tryReconcileClusterResources(taskManagerTracker);

        // 这些是需要释放的TM
        reconcileResult.getPendingTaskManagersToRelease().stream()
                .map(PendingTaskManager::getPendingTaskManagerId)
                .forEach(taskManagerTracker::removePendingTaskManager);

        // 需要释放的TM
        for (TaskManagerInfo taskManagerToRelease : reconcileResult.getTaskManagersToRelease()) {
            if (waitResultConsumedBeforeRelease) {
                releaseIdleTaskExecutorIfPossible(taskManagerToRelease);
            } else {
                releaseIdleTaskExecutor(taskManagerToRelease.getInstanceId());
            }
        }

        // 表示还需要申请的TM
        reconcileResult.getPendingTaskManagersToAllocate().forEach(this::allocateResource);

        return reconcileResult.needReconcile();
    }

    /**
     * 释放TM 先检测能否释放
     * @param taskManagerInfo
     */
    private void releaseIdleTaskExecutorIfPossible(TaskManagerInfo taskManagerInfo) {
        final long idleSince = taskManagerInfo.getIdleSince();
        taskManagerInfo
                .getTaskExecutorConnection()
                .getTaskExecutorGateway()
                .canBeReleased()
                .thenAcceptAsync(
                        canBeReleased -> {
                            boolean stillIdle = idleSince == taskManagerInfo.getIdleSince();
                            if (stillIdle && canBeReleased) {
                                releaseIdleTaskExecutor(taskManagerInfo.getInstanceId());
                                declareNeededResourcesWithDelay();
                            }
                        },
                        mainThreadExecutor);
    }

    /**
     * 释放TM
     * @param taskManagerToRelease
     */
    private void releaseIdleTaskExecutor(InstanceID taskManagerToRelease) {
        Preconditions.checkState(resourceAllocator.isSupported());
        taskManagerTracker.addUnWantedTaskManager(taskManagerToRelease);
    }

    /**
     * 需要再申请TM
     * @param pendingTaskManager
     * @return
     */
    private boolean allocateResource(PendingTaskManager pendingTaskManager) {
        Preconditions.checkState(resourceAllocator.isSupported());
        if (isMaxTotalResourceExceededAfterAdding(pendingTaskManager.getTotalResourceProfile())) {
            LOG.info(
                    "Could not allocate {}. Max total resource limitation <{}, {}> is reached.",
                    pendingTaskManager,
                    maxTotalCpu,
                    maxTotalMem.toHumanReadableString());
            return false;
        }

        taskManagerTracker.addPendingTaskManager(pendingTaskManager);
        return true;
    }

    @VisibleForTesting
    public long getTaskManagerIdleSince(InstanceID instanceId) {
        return taskManagerTracker
                .getRegisteredTaskManager(instanceId)
                .map(TaskManagerInfo::getIdleSince)
                .orElse(0L);
    }

    // ---------------------------------------------------------------------------------------------
    // Internal utility methods
    // ---------------------------------------------------------------------------------------------

    private void checkInit() {
        Preconditions.checkState(started, "The slot manager has not been started.");
        Preconditions.checkNotNull(resourceManagerId);
        Preconditions.checkNotNull(mainThreadExecutor);
        Preconditions.checkNotNull(resourceAllocator);
        Preconditions.checkNotNull(resourceEventListener);
    }

    /**
     * 申请量不能超过总量
     * @param newResource
     * @return
     */
    private boolean isMaxTotalResourceExceededAfterAdding(ResourceProfile newResource) {
        final ResourceProfile totalResourceAfterAdding =
                newResource
                        .merge(taskManagerTracker.getRegisteredResource())
                        .merge(taskManagerTracker.getPendingResource());
        return totalResourceAfterAdding.getCpuCores().compareTo(maxTotalCpu) > 0
                || totalResourceAfterAdding.getTotalMemory().compareTo(maxTotalMem) > 0;
    }

    private boolean isBlockedTaskManager(ResourceID resourceID) {
        Preconditions.checkNotNull(blockedTaskManagerChecker);
        return blockedTaskManagerChecker.isBlockedTaskManager(resourceID);
    }
}
