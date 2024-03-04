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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * SlotManager component for various task executor related responsibilities of the slot manager,
 * including:
 *
 * <ul>
 *   <li>tracking registered task executors
 *   <li>allocating new task executors (both on-demand, and for redundancy)
 *   <li>releasing idle task executors
 *   <li>tracking pending slots (expected slots from executors that are currently being allocated
 *   <li>tracking how many slots are used on each task executor
 * </ul>
 *
 * <p>Dev note: This component only exists to keep the code out of the slot manager. It covers many
 * aspects that aren't really the responsibility of the slot manager, and should be refactored to
 * live outside the slot manager and split into multiple parts.
 *
 * 任务执行器管理器
 */
class TaskExecutorManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorManager.class);

    /**
     * 默认给每个slot的资源
     */
    private final ResourceProfile defaultSlotResourceProfile;

    /** The default resource spec of workers to request.
     * 这个是对于worker的资源描述
     * */
    private final WorkerResourceSpec defaultWorkerResourceSpec;

    /**
     * 每个worker分配多少slot
     */
    private final int numSlotsPerWorker;

    /** Defines the max limitation of the total number of slots.
     * 能够维护的slot总数
     * */
    private final int maxSlotNum;

    /**
     * Release task executor only when each produced result partition is either consumed or failed.
     */
    private final boolean waitResultConsumedBeforeRelease;

    /** Defines the number of redundant taskmanagers.
     * 多余的TM数量
     * */
    private final int redundantTaskManagerNum;

    /** Timeout after which an unused TaskManager is released. */
    private final Time taskManagerTimeout;

    /** Callbacks for resource (de-)allocations.
     * 这是一个资源分配器
     * */
    private final ResourceAllocator resourceAllocator;

    /** All currently registered task managers.
     * 此时注册的所有TM  每个TM对应一个worker
     * */
    private final Map<InstanceID, TaskManagerRegistration> taskManagerRegistrations =
            new HashMap<>();

    /**
     * 分配中的slot
     */
    private final Map<TaskManagerSlotId, PendingTaskManagerSlot> pendingSlots = new HashMap<>();

    private final Executor mainThreadExecutor;

    @Nullable private final ScheduledFuture<?> taskManagerTimeoutsAndRedundancyCheck;

    /**
     * 多余的worker
     */
    private final Set<InstanceID> unWantedWorkers;
    private final ScheduledExecutor scheduledExecutor;
    private final Duration declareNeededResourceDelay;
    private CompletableFuture<Void> declareNeededResourceFuture;

    TaskExecutorManager(
            WorkerResourceSpec defaultWorkerResourceSpec,
            int numSlotsPerWorker,
            int maxNumSlots,
            boolean waitResultConsumedBeforeRelease,
            int redundantTaskManagerNum,
            Time taskManagerTimeout,
            Duration declareNeededResourceDelay,
            ScheduledExecutor scheduledExecutor,
            Executor mainThreadExecutor,
            ResourceAllocator resourceAllocator) {

        this.defaultWorkerResourceSpec = defaultWorkerResourceSpec;
        this.numSlotsPerWorker = numSlotsPerWorker;
        this.maxSlotNum = maxNumSlots;
        this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
        this.redundantTaskManagerNum = redundantTaskManagerNum;
        this.taskManagerTimeout = taskManagerTimeout;
        this.defaultSlotResourceProfile =
                SlotManagerUtils.generateDefaultSlotResourceProfile(
                        defaultWorkerResourceSpec, numSlotsPerWorker);
        this.scheduledExecutor = scheduledExecutor;
        this.declareNeededResourceDelay = declareNeededResourceDelay;
        this.unWantedWorkers = new HashSet<>();
        this.resourceAllocator = Preconditions.checkNotNull(resourceAllocator);
        this.mainThreadExecutor = mainThreadExecutor;
        if (resourceAllocator.isSupported()) {

            // 开启定时任务
            taskManagerTimeoutsAndRedundancyCheck =
                    scheduledExecutor.scheduleWithFixedDelay(
                            () ->
                                    mainThreadExecutor.execute(
                                            this::checkTaskManagerTimeoutsAndRedundancy),
                            0L,
                            taskManagerTimeout.toMilliseconds(),
                            TimeUnit.MILLISECONDS);
        } else {
            taskManagerTimeoutsAndRedundancyCheck = null;
        }
    }

    /**
     * 取消定时任务
     */
    @Override
    public void close() {
        if (taskManagerTimeoutsAndRedundancyCheck != null) {
            taskManagerTimeoutsAndRedundancyCheck.cancel(false);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // TaskExecutor (un)registration
    // ---------------------------------------------------------------------------------------------

    public boolean isTaskManagerRegistered(InstanceID instanceId) {
        return taskManagerRegistrations.containsKey(instanceId);
    }

    /**
     * 申请worker成功后会触发该方法
     * @param taskExecutorConnection
     * @param initialSlotReport
     * @param totalResourceProfile
     * @param defaultSlotResourceProfile
     * @return
     */
    public boolean registerTaskManager(
            final TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {

        // slot数量太多 不能处理
        if (isMaxSlotNumExceededAfterRegistration(initialSlotReport)) {
            LOG.info(
                    "The total number of slots exceeds the max limitation {}, could not register the excess task executor {}.",
                    maxSlotNum,
                    taskExecutorConnection.getInstanceID());
            return false;
        }

        // 根据相关信息 产生TM
        TaskManagerRegistration taskManagerRegistration =
                new TaskManagerRegistration(
                        taskExecutorConnection,
                        StreamSupport.stream(initialSlotReport.spliterator(), false)
                                .map(SlotStatus::getSlotID)
                                .collect(Collectors.toList()),
                        totalResourceProfile,
                        defaultSlotResourceProfile);

        taskManagerRegistrations.put(
                taskExecutorConnection.getInstanceID(), taskManagerRegistration);

        // next register the new slots
        for (SlotStatus slotStatus : initialSlotReport) {
            if (slotStatus.getJobID() == null) {
                // 表示之前申请的slot已经成功了  移除pending
                findAndRemoveExactlyMatchingPendingTaskManagerSlot(slotStatus.getResourceProfile());
            }
        }

        return true;
    }

    /**
     * 判断注册时 slot数量是否过多
     * @param initialSlotReport
     * @return
     */
    private boolean isMaxSlotNumExceededAfterRegistration(SlotReport initialSlotReport) {
        // check if the total number exceed before matching pending slot.
        // 表示维护的slot数量过多
        if (!isMaxSlotNumExceededAfterAdding(initialSlotReport.getNumSlotStatus())) {
            return false;
        }

        // check if the total number exceed slots after consuming pending slot.
        // 不考虑pending中已经出现的slot    尽可能不让slot超过 maxSlot
        return isMaxSlotNumExceededAfterAdding(getNumNonPendingReportedNewSlots(initialSlotReport));
    }

    /**
     *
     * @param slotReport
     * @return
     */
    private int getNumNonPendingReportedNewSlots(SlotReport slotReport) {
        final Set<TaskManagerSlotId> matchingPendingSlots = new HashSet<>();

        for (SlotStatus slotStatus : slotReport) {
            if (slotStatus.getAllocationID() != null) {
                // only empty registered slots can match pending slots
                continue;
            }

            // 找到资源精准匹配的 slot
            for (PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
                if (!matchingPendingSlots.contains(pendingTaskManagerSlot.getTaskManagerSlotId())
                        && isPendingSlotExactlyMatchingResourceProfile(
                                pendingTaskManagerSlot, slotStatus.getResourceProfile())) {
                    matchingPendingSlots.add(pendingTaskManagerSlot.getTaskManagerSlotId());
                    break; // pendingTaskManagerSlot loop
                }
            }
        }

        // 把pending的部分去掉
        return slotReport.getNumSlotStatus() - matchingPendingSlots.size();
    }

    /**
     * 将命中的slot从pending移除
     * @param resourceProfile
     */
    private void findAndRemoveExactlyMatchingPendingTaskManagerSlot(
            ResourceProfile resourceProfile) {
        for (PendingTaskManagerSlot pendingTaskManagerSlot : pendingSlots.values()) {
            if (isPendingSlotExactlyMatchingResourceProfile(
                    pendingTaskManagerSlot, resourceProfile)) {
                pendingSlots.remove(pendingTaskManagerSlot.getTaskManagerSlotId());
                return;
            }
        }
    }

    /**
     * 2个资源是否匹配
     * @param pendingTaskManagerSlot
     * @param resourceProfile
     * @return
     */
    private boolean isPendingSlotExactlyMatchingResourceProfile(
            PendingTaskManagerSlot pendingTaskManagerSlot, ResourceProfile resourceProfile) {
        return pendingTaskManagerSlot.getResourceProfile().equals(resourceProfile);
    }

    /**
     * 移除某个TM
     * @param instanceId
     */
    public void unregisterTaskExecutor(InstanceID instanceId) {
        taskManagerRegistrations.remove(instanceId);
        unWantedWorkers.remove(instanceId);
    }

    public Collection<InstanceID> getTaskExecutors() {
        return new ArrayList<>(taskManagerRegistrations.keySet());
    }

    // ---------------------------------------------------------------------------------------------
    // TaskExecutor allocation
    // ---------------------------------------------------------------------------------------------

    /**
     * Tries to allocate a worker that can provide a slot with the given resource profile.
     *
     * @param requestedSlotResourceProfile desired slot profile
     * @return an upper bound resource requirement that can be fulfilled by the new worker, if one
     *     was allocated
     *     分配一个 worker
     */
    public Optional<ResourceRequirement> allocateWorker(
            ResourceProfile requestedSlotResourceProfile) {
        // 表示不支持分配资源
        if (!resourceAllocator.isSupported()) {
            // resource cannot be allocated
            return Optional.empty();
        }

        // 在TM报告时 会携带一些slot
        final int numRegisteredSlots = getNumberRegisteredSlots();
        // 提示有多少pending中的slot
        final int numPendingSlots = getNumberPendingTaskManagerSlots();
        // 本次操作后 会需要更多的slot 如果超出上限就无法分配
        if (isMaxSlotNumExceededAfterAdding(numSlotsPerWorker)) {
            LOG.warn(
                    "Could not allocate {} more slots. The number of registered and pending slots is {}, while the maximum is {}.",
                    numSlotsPerWorker,
                    numPendingSlots + numRegisteredSlots,
                    maxSlotNum);
            return Optional.empty();
        }

        // 资源不匹配  无法分配
        if (!defaultSlotResourceProfile.isMatching(requestedSlotResourceProfile)) {
            // requested resource profile is unfulfillable
            return Optional.empty();
        }

        // 开始产生pendingslot
        for (int i = 0; i < numSlotsPerWorker; ++i) {
            PendingTaskManagerSlot pendingTaskManagerSlot =
                    new PendingTaskManagerSlot(defaultSlotResourceProfile);
            pendingSlots.put(pendingTaskManagerSlot.getTaskManagerSlotId(), pendingTaskManagerSlot);
        }

        // 向其他组件声明需要的资源
        declareNeededResourcesWithDelay();

        return Optional.of(
                ResourceRequirement.create(defaultSlotResourceProfile, numSlotsPerWorker));
    }

    private boolean isMaxSlotNumExceededAfterAdding(int numNewSlot) {
        return getNumberRegisteredSlots() + getNumberPendingTaskManagerSlots() + numNewSlot
                > maxSlotNum;
    }

    /**
     * 声明需要的资源
     * @return
     */
    private Collection<ResourceDeclaration> getResourceDeclaration() {
        // 计算多少个worker
        final int pendingWorkerNum =
                MathUtils.divideRoundUp(getNumberPendingTaskManagerSlots(), numSlotsPerWorker);
        Set<InstanceID> neededRegisteredWorkers = new HashSet<>(taskManagerRegistrations.keySet());
        neededRegisteredWorkers.removeAll(unWantedWorkers);
        final int totalWorkerNum = pendingWorkerNum + neededRegisteredWorkers.size();

        // 更新现在需要的worker数量
        return Collections.singleton(
                new ResourceDeclaration(
                        defaultWorkerResourceSpec, totalWorkerNum, new HashSet<>(unWantedWorkers)));
    }

    /**
     * 向其他的组件声明需要的资源
     */
    private void declareNeededResourcesWithDelay() {
        Preconditions.checkState(resourceAllocator.isSupported());

        if (declareNeededResourceDelay.toMillis() <= 0) {
            // 表示立即触发
            declareNeededResources();
        } else {
            if (declareNeededResourceFuture == null || declareNeededResourceFuture.isDone()) {
                declareNeededResourceFuture = new CompletableFuture<>();
                scheduledExecutor.schedule(
                        () ->
                                mainThreadExecutor.execute(
                                        () -> {
                                            // 一定延时后触发
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
     * 声明需要的资源
     * */
    private void declareNeededResources() {
        resourceAllocator.declareResourceNeeded(getResourceDeclaration());
    }

    @VisibleForTesting
    int getNumberPendingTaskManagerSlots() {
        return pendingSlots.size();
    }

    // ---------------------------------------------------------------------------------------------
    // TaskExecutor idleness / redundancy
    // ---------------------------------------------------------------------------------------------

    /**
     * 周期性触发的方法
     */
    private void checkTaskManagerTimeoutsAndRedundancy() {
        // taskManager 就是 worker
        if (!taskManagerRegistrations.isEmpty()) {
            long currentTime = System.currentTimeMillis();

            ArrayList<TaskManagerRegistration> timedOutTaskManagers =
                    new ArrayList<>(taskManagerRegistrations.size());

            // first retrieve the timed out TaskManagers
            for (TaskManagerRegistration taskManagerRegistration :
                    taskManagerRegistrations.values()) {
                if (currentTime - taskManagerRegistration.getIdleSince()
                        >= taskManagerTimeout.toMilliseconds()) {
                    // we collect the instance ids first in order to avoid concurrent modifications
                    // by the
                    // ResourceAllocator.releaseResource call
                    timedOutTaskManagers.add(taskManagerRegistration);
                }
            }

            // redundantTaskManagerNum * numSlotsPerWorker 这个应该是期望保留的空闲数量
            // 也就是一旦空闲slot低于该值  就申请worker
            int slotsDiff = redundantTaskManagerNum * numSlotsPerWorker - getNumberFreeSlots();
            if (slotsDiff > 0) {
                if (pendingSlots.isEmpty()) {
                    // Keep enough redundant taskManagers from time to time.
                    int requiredTaskManagers =
                            MathUtils.divideRoundUp(slotsDiff, numSlotsPerWorker);
                    // 根据需要 分配多余的worker
                    allocateRedundantTaskManagers(requiredTaskManagers);
                } else {
                    LOG.debug(
                            "There are some pending slots, skip allocate redundant task manager and wait them fulfilled.");
                }
            } else {
                // second we trigger the release resource callback which can decide upon the
                // resource release
                // 此时slot太多了   表示要释放一定量的 worker
                int maxReleaseNum = (-slotsDiff) / numSlotsPerWorker;
                releaseIdleTaskExecutors(
                        // timedOutTaskManagers 是长时间未使用的worker
                        timedOutTaskManagers, Math.min(maxReleaseNum, timedOutTaskManagers.size()));
            }
        }
    }

    /**
     * 分配多余的worker
     * @param number
     */
    private void allocateRedundantTaskManagers(int number) {
        LOG.debug("Allocating {} task executors for redundancy.", number);
        int allocatedNumber = allocateWorkers(number);
        if (number != allocatedNumber) {
            LOG.warn(
                    "Expect to allocate {} taskManagers. Actually allocate {} taskManagers.",
                    number,
                    allocatedNumber);
        }
    }

    /**
     * Allocate a number of workers based on the input param.
     *
     * @param workerNum the number of workers to allocate
     * @return the number of successfully allocated workers
     */
    private int allocateWorkers(int workerNum) {
        int allocatedWorkerNum = 0;
        for (int i = 0; i < workerNum; ++i) {
            // 分配一定数量的worker
            if (allocateWorker(defaultSlotResourceProfile).isPresent()) {
                ++allocatedWorkerNum;
            } else {
                break;
            }
        }
        return allocatedWorkerNum;
    }

    /**
     * 当空闲slot太多的时候 就尝试释放worker
     * @param timedOutTaskManagers
     * @param releaseNum
     */
    private void releaseIdleTaskExecutors(
            ArrayList<TaskManagerRegistration> timedOutTaskManagers, int releaseNum) {
        for (int index = 0; index < releaseNum; ++index) {
            if (waitResultConsumedBeforeRelease) {
                releaseIdleTaskExecutorIfPossible(timedOutTaskManagers.get(index));
            } else {
                releaseIdleTaskExecutor(timedOutTaskManagers.get(index).getInstanceId());
            }
        }
    }

    /**
     * 在释放前 访问 TaskExecutor 能否释放
     * @param taskManagerRegistration
     */
    private void releaseIdleTaskExecutorIfPossible(
            TaskManagerRegistration taskManagerRegistration) {
        long idleSince = taskManagerRegistration.getIdleSince();
        taskManagerRegistration
                .getTaskManagerConnection()
                .getTaskExecutorGateway()
                .canBeReleased()
                .thenAcceptAsync(
                        canBeReleased -> {
                            InstanceID timedOutTaskManagerId =
                                    taskManagerRegistration.getInstanceId();
                            boolean stillIdle = idleSince == taskManagerRegistration.getIdleSince();
                            if (stillIdle && canBeReleased) {
                                releaseIdleTaskExecutor(timedOutTaskManagerId);
                            }
                        },
                        mainThreadExecutor);
    }

    private void releaseIdleTaskExecutor(InstanceID timedOutTaskManagerId) {
        Preconditions.checkState(resourceAllocator.isSupported());
        LOG.debug(
                "Release TaskExecutor {} because it exceeded the idle timeout.",
                timedOutTaskManagerId);
        // 在声明需要的资源时 就不会包含 unWantedWorkers
        unWantedWorkers.add(timedOutTaskManagerId);
        declareNeededResourcesWithDelay();
    }

    // ---------------------------------------------------------------------------------------------
    // slot / resource counts
    // ---------------------------------------------------------------------------------------------

    public ResourceProfile getTotalRegisteredResources() {
        return taskManagerRegistrations.values().stream()
                .map(TaskManagerRegistration::getTotalResource)
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    public ResourceProfile getTotalRegisteredResourcesOf(InstanceID instanceID) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceID))
                .map(TaskManagerRegistration::getTotalResource)
                .orElse(ResourceProfile.ZERO);
    }

    public ResourceProfile getTotalFreeResources() {
        return taskManagerRegistrations.values().stream()
                .map(
                        taskManagerRegistration ->
                                taskManagerRegistration
                                        .getDefaultSlotResourceProfile()
                                        .multiply(taskManagerRegistration.getNumberFreeSlots()))
                .reduce(ResourceProfile.ZERO, ResourceProfile::merge);
    }

    public ResourceProfile getTotalFreeResourcesOf(InstanceID instanceID) {
        return Optional.ofNullable(taskManagerRegistrations.get(instanceID))
                .map(
                        taskManagerRegistration ->
                                taskManagerRegistration
                                        .getDefaultSlotResourceProfile()
                                        .multiply(taskManagerRegistration.getNumberFreeSlots()))
                .orElse(ResourceProfile.ZERO);
    }

    public Iterable<SlotID> getSlotsOf(InstanceID instanceId) {
        return taskManagerRegistrations.get(instanceId).getSlots();
    }

    /**
     * 获取注册的slot总数
     * @return
     */
    public int getNumberRegisteredSlots() {
        return taskManagerRegistrations.values().stream()
                .map(TaskManagerRegistration::getNumberRegisteredSlots)  // 这个是在收到报告时产生的
                .reduce(0, Integer::sum);
    }

    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if (taskManagerRegistration != null) {
            return taskManagerRegistration.getNumberRegisteredSlots();
        } else {
            return 0;
        }
    }

    public int getNumberFreeSlots() {
        return taskManagerRegistrations.values().stream()
                .map(TaskManagerRegistration::getNumberFreeSlots)
                .reduce(0, Integer::sum);
    }

    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

        if (taskManagerRegistration != null) {
            return taskManagerRegistration.getNumberFreeSlots();
        } else {
            return 0;
        }
    }

    public Collection<PendingTaskManagerSlot> getPendingTaskManagerSlots() {
        return pendingSlots.values();
    }

    /**
     * remove unused pending task manager slots.
     *
     * @param unusedResourceCounter the count of unused resources.
     *                              表示要释放这么多资源
     */
    public void removePendingTaskManagerSlots(ResourceCounter unusedResourceCounter) {
        if (!resourceAllocator.isSupported()) {
            return;
        }
        Preconditions.checkState(unusedResourceCounter.getResources().size() == 1);
        Preconditions.checkState(
                unusedResourceCounter.getResources().contains(defaultSlotResourceProfile));

        int wantedPendingSlotsNumber =
                pendingSlots.size()
                        - unusedResourceCounter.getResourceCount(defaultSlotResourceProfile);
        pendingSlots.entrySet().removeIf(ignore -> pendingSlots.size() > wantedPendingSlotsNumber);

        declareNeededResourcesWithDelay();
    }

    /** clear all pending task manager slots. */
    public void clearPendingTaskManagerSlots() {
        if (!resourceAllocator.isSupported()) {
            return;
        }
        if (!pendingSlots.isEmpty()) {
            this.pendingSlots.clear();
            declareNeededResourcesWithDelay();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // TaskExecutor slot book-keeping
    // ---------------------------------------------------------------------------------------------

    public void occupySlot(InstanceID instanceId) {
        taskManagerRegistrations.get(instanceId).occupySlot();
    }

    public void freeSlot(InstanceID instanceId) {
        taskManagerRegistrations.get(instanceId).freeSlot();
    }

    public void markUsed(InstanceID instanceID) {
        taskManagerRegistrations.get(instanceID).markUsed();
    }
}
