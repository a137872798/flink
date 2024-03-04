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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceBudgetManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor.DummyComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/** Default implementation of {@link TaskSlotTable}.
 * 通过该对象来管理一组slot  并提供分配api
 * */
public class TaskSlotTableImpl<T extends TaskSlotPayload> implements TaskSlotTable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(TaskSlotTableImpl.class);

    /**
     * Number of slots in static slot allocation. If slot is requested with an index, the requested
     * index must within the range of [0, numberSlots). When generating slot report, we should
     * always generate slots with index in [0, numberSlots) even the slot does not exist.
     * 记录总数
     */
    private final int numberSlots;

    /** Slot resource profile for static slot allocation.
     * 默认slot所持有的资源
     * */
    private final ResourceProfile defaultSlotResourceProfile;

    /** Page size for memory manager.
     * 表示内存管理器中一个page的大小
     * */
    private final int memoryPageSize;

    /** Timer service used to time out allocated slots.
     * 通过该服务监控 slot是否在指定的时间内完成分配
     * */
    private final TimerService<AllocationID> timerService;

    /** The list of all task slots.
     * 通过编号 检索 TaskSlot
     * */
    private final Map<Integer, TaskSlot<T>> taskSlots;

    /** Mapping from allocation id to task slot.
     * 通过 AllocationID 检索 TaskSlot
     * */
    private final Map<AllocationID, TaskSlot<T>> allocatedSlots;

    /** Mapping from execution attempt id to task and task slot.
     * 通过 Execution id  找到 Task 以及 TaskSlot
     * */
    private final Map<ExecutionAttemptID, TaskSlotMapping<T>> taskSlotMappings;

    /** Mapping from job id to allocated slots for a job.
     * 找到分配给该job的slot
     * */
    private final Map<JobID, Set<AllocationID>> slotsPerJob;

    /** Interface for slot actions, such as freeing them or timing them out.
     * 通过该对象可以释放slot  还可以处理slot的超时事件
     * */
    @Nullable private SlotActions slotActions;

    /** The table state.
     * 描述表的状态
     * */
    private volatile State state;

    /** Current index for dynamic slot, should always not less than numberSlots */
    private int dynamicSlotIndex;

    /**
     * 记录资源
     */
    private final ResourceBudgetManager budgetManager;

    /** The closing future is completed when all slot are freed and state is closed. */
    private final CompletableFuture<Void> closingFuture;

    /** {@link ComponentMainThreadExecutor} to schedule internal calls to the main thread. */
    private ComponentMainThreadExecutor mainThreadExecutor =
            new DummyComponentMainThreadExecutor(
                    "TaskSlotTableImpl is not initialized with proper main thread executor, "
                            + "call to TaskSlotTableImpl#start is required");

    /** {@link Executor} for background actions, e.g. verify all managed memory released. */
    private final Executor memoryVerificationExecutor;

    public TaskSlotTableImpl(
            final int numberSlots,
            final ResourceProfile totalAvailableResourceProfile,
            final ResourceProfile defaultSlotResourceProfile,
            final int memoryPageSize,
            final TimerService<AllocationID> timerService,
            final Executor memoryVerificationExecutor) {
        Preconditions.checkArgument(
                0 < numberSlots, "The number of task slots must be greater than 0.");

        this.numberSlots = numberSlots;
        this.dynamicSlotIndex = numberSlots;
        this.defaultSlotResourceProfile = Preconditions.checkNotNull(defaultSlotResourceProfile);
        this.memoryPageSize = memoryPageSize;

        this.taskSlots = CollectionUtil.newHashMapWithExpectedSize(numberSlots);

        this.timerService = Preconditions.checkNotNull(timerService);

        budgetManager =
                new ResourceBudgetManager(
                        Preconditions.checkNotNull(totalAvailableResourceProfile));

        allocatedSlots = CollectionUtil.newHashMapWithExpectedSize(numberSlots);

        taskSlotMappings = CollectionUtil.newHashMapWithExpectedSize(4 * numberSlots);

        slotsPerJob = CollectionUtil.newHashMapWithExpectedSize(4);

        slotActions = null;
        state = State.CREATED;
        closingFuture = new CompletableFuture<>();

        this.memoryVerificationExecutor = memoryVerificationExecutor;
    }

    /**
     * @param initialSlotActions to use for slot actions   为给对象设置可以操纵slot的组件
     * @param mainThreadExecutor {@link ComponentMainThreadExecutor} to schedule internal calls to
     */
    @Override
    public void start(
            SlotActions initialSlotActions, ComponentMainThreadExecutor mainThreadExecutor) {
        Preconditions.checkState(
                state == State.CREATED,
                "The %s has to be just created before starting",
                TaskSlotTableImpl.class.getSimpleName());
        this.slotActions = Preconditions.checkNotNull(initialSlotActions);
        this.mainThreadExecutor = Preconditions.checkNotNull(mainThreadExecutor);

        timerService.start(this);

        state = State.RUNNING;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (state == State.CREATED) {
            state = State.CLOSED;
            closingFuture.complete(null);
        } else if (state == State.RUNNING) {
            state = State.CLOSING;
            final FlinkException cause = new FlinkException("Closing task slot table");
            CompletableFuture<Void> cleanupFuture =
                    FutureUtils.waitForAll(
                            // 如果已经在运行状态了 就要先处理之前分配的slot
                                    new ArrayList<>(allocatedSlots.values())
                                            .stream()
                                                    .map(slot -> freeSlotInternal(slot, cause))
                                                    .collect(Collectors.toList()))
                            .thenRunAsync(
                                    () -> {
                                        state = State.CLOSED;
                                        timerService.stop();
                                    },
                                    mainThreadExecutor);
            FutureUtils.forward(cleanupFuture, closingFuture);
        }
        return closingFuture;
    }

    @VisibleForTesting
    public boolean isClosed() {
        return state == State.CLOSED;
    }

    @Override
    public Set<AllocationID> getAllocationIdsPerJob(JobID jobId) {
        final Set<AllocationID> allocationIds = slotsPerJob.get(jobId);

        if (allocationIds == null) {
            return Collections.emptySet();
        } else {
            return Collections.unmodifiableSet(allocationIds);
        }
    }

    @Override
    public Set<AllocationID> getActiveTaskSlotAllocationIds() {
        // 在迭代器初始化时  指定 state  能够过滤其他状态
        return createAllocationIdSet(new TaskSlotIterator(TaskSlotState.ACTIVE));
    }

    @Override
    public Set<AllocationID> getActiveTaskSlotAllocationIdsPerJob(JobID jobId) {
        return createAllocationIdSet(new TaskSlotIterator(jobId, TaskSlotState.ACTIVE));
    }

    private Set<AllocationID> createAllocationIdSet(Iterator<TaskSlot<T>> taskSlotIterator) {
        Set<AllocationID> allocationIds = new HashSet<>();
        while (taskSlotIterator.hasNext()) {
            allocationIds.add(taskSlotIterator.next().getAllocationId());
        }

        return allocationIds;
    }

    // ---------------------------------------------------------------------
    // Slot report methods
    // ---------------------------------------------------------------------

    /**
     * 将该TM下的slot信息 做成报告
     * @param resourceId 当前TM的id
     * @return
     */
    @Override
    public SlotReport createSlotReport(ResourceID resourceId) {
        List<SlotStatus> slotStatuses = new ArrayList<>();

        // 这里是前面的部分
        for (int i = 0; i < numberSlots; i++) {
            SlotID slotId = new SlotID(resourceId, i);
            SlotStatus slotStatus;
            if (taskSlots.containsKey(i)) {
                TaskSlot<T> taskSlot = taskSlots.get(i);

                slotStatus =
                        new SlotStatus(
                                slotId,
                                taskSlot.getResourceProfile(),
                                taskSlot.getJobId(),
                                taskSlot.getAllocationId());
            } else {
                slotStatus = new SlotStatus(slotId, defaultSlotResourceProfile, null, null);
            }

            slotStatuses.add(slotStatus);
        }

        for (TaskSlot<T> taskSlot : allocatedSlots.values()) {
            // 这里获取的是超过numberSlots的部分
            if (isDynamicIndex(taskSlot.getIndex())) {
                SlotStatus slotStatus =
                        new SlotStatus(
                                new SlotID(resourceId, taskSlot.getIndex()),
                                taskSlot.getResourceProfile(),
                                taskSlot.getJobId(),
                                taskSlot.getAllocationId());
                slotStatuses.add(slotStatus);
            }
        }

        final SlotReport slotReport = new SlotReport(slotStatuses);

        return slotReport;
    }

    // ---------------------------------------------------------------------
    // Slot methods
    // ---------------------------------------------------------------------

    @VisibleForTesting
    @Override
    public boolean allocateSlot(
            int index, JobID jobId, AllocationID allocationId, Time slotTimeout) {
        return allocateSlot(index, jobId, allocationId, defaultSlotResourceProfile, slotTimeout);
    }

    /**
     * 将某个slot 分配给某个job
     * @param requestedIndex
     * @param jobId to allocate the task slot for
     * @param allocationId identifying the allocation
     * @param resourceProfile of the requested slot, used only for dynamic slot allocation and will
     *     be ignored otherwise
     * @param slotTimeout until the slot times out
     * @return
     */
    @Override
    public boolean allocateSlot(
            int requestedIndex,
            JobID jobId,
            AllocationID allocationId,
            ResourceProfile resourceProfile,
            Time slotTimeout) {
        checkRunning();

        Preconditions.checkArgument(requestedIndex < numberSlots);

        // The negative requestIndex indicate that the SlotManager allocate a dynamic slot, we
        // transfer the index to an increasing number not less than the numberSlots.
        int index = requestedIndex < 0 ? nextDynamicSlotIndex() : requestedIndex;
        ResourceProfile effectiveResourceProfile =
                resourceProfile.equals(ResourceProfile.UNKNOWN)
                        ? defaultSlotResourceProfile
                        : resourceProfile;

        // 发现已经被分配过了
        TaskSlot<T> taskSlot = allocatedSlots.get(allocationId);
        if (taskSlot != null) {
            return isDuplicatedSlot(taskSlot, jobId, effectiveResourceProfile, index);
            // 表示编号已被使用
        } else if (isIndexAlreadyTaken(index)) {
            LOG.info(
                    "The slot with index {} is already assigned to another allocation with id {}.",
                    index,
                    taskSlots.get(index).getAllocationId());
            return false;
        }

        // 资源不够分配了
        if (!budgetManager.reserve(effectiveResourceProfile)) {
            LOG.info(
                    "Cannot allocate the requested resources. Trying to allocate {}, "
                            + "while the currently remaining available resources are {}, total is {}.",
                    effectiveResourceProfile,
                    budgetManager.getAvailableBudget(),
                    budgetManager.getTotalBudget());
            return false;
        }
        LOG.info(
                "Allocated slot for {} with resources {}.", allocationId, effectiveResourceProfile);

        // 现在将index job slot 信息包装起来
        taskSlot =
                new TaskSlot<>(
                        index,
                        effectiveResourceProfile,
                        memoryPageSize,
                        jobId,
                        allocationId,
                        memoryVerificationExecutor);
        // 加入分配过的容器

        taskSlots.put(index, taskSlot);

        // update the allocation id to task slot map
        allocatedSlots.put(allocationId, taskSlot);

        // register a timeout for this slot since it's in state allocated
        // 将slot注册到超时对象上
        timerService.registerTimeout(allocationId, slotTimeout.getSize(), slotTimeout.getUnit());

        // add this slot to the set of job slots
        Set<AllocationID> slots = slotsPerJob.get(jobId);

        if (slots == null) {
            slots = CollectionUtil.newHashSetWithExpectedSize(4);
            // 更新job下的slot
            slotsPerJob.put(jobId, slots);
        }

        slots.add(allocationId);

        return true;
    }

    private boolean isDuplicatedSlot(
            TaskSlot taskSlot, JobID jobId, ResourceProfile resourceProfile, int index) {
        LOG.info(
                "Slot with allocationId {} already exist, with resource profile {}, job id {} and index {}. The required index is {}.",
                taskSlot.getAllocationId(),
                taskSlot.getResourceProfile(),
                taskSlot.getJobId(),
                taskSlot.getIndex(),
                index);
        return taskSlot.getJobId().equals(jobId)
                && taskSlot.getResourceProfile().equals(resourceProfile)
                && (isDynamicIndex(index) || taskSlot.getIndex() == index);
    }

    private boolean isIndexAlreadyTaken(int index) {
        return taskSlots.get(index) != null;
    }

    private boolean isDynamicIndex(int index) {
        return index >= numberSlots;
    }

    /**
     *
     * @param allocationId to identify the task slot to mark as active
     * @return
     * @throws SlotNotFoundException
     */
    @Override
    public boolean markSlotActive(AllocationID allocationId) throws SlotNotFoundException {
        checkRunning();

        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            return markExistingSlotActive(taskSlot);
        } else {
            throw new SlotNotFoundException(allocationId);
        }
    }

    private boolean markExistingSlotActive(TaskSlot<T> taskSlot) {
        // 修改成活跃状态 也就是说不再属于某个job了
        if (taskSlot.markActive()) {
            // unregister a potential timeout
            LOG.info("Activate slot {}.", taskSlot.getAllocationId());

            // 就可以取消它的定时任务了  也就是说期望某个slot在一定时间内被归还  否则就会触发超时错误
            timerService.unregisterTimeout(taskSlot.getAllocationId());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean markSlotInactive(AllocationID allocationId, Time slotTimeout)
            throws SlotNotFoundException {
        checkStarted();

        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            // 标记成已分配
            if (taskSlot.markInactive()) {
                // register a timeout to free the slot
                // 开启定时任务
                timerService.registerTimeout(
                        allocationId, slotTimeout.getSize(), slotTimeout.getUnit());

                return true;
            } else {
                return false;
            }
        } else {
            throw new SlotNotFoundException(allocationId);
        }
    }

    @Override
    public int freeSlot(AllocationID allocationId, Throwable cause) throws SlotNotFoundException {
        checkStarted();

        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            return freeSlotInternal(taskSlot, cause).isDone() ? taskSlot.getIndex() : -1;
        } else {
            throw new SlotNotFoundException(allocationId);
        }
    }

    /**
     * 释放已分配的slot
     * @param taskSlot
     * @param cause
     * @return
     */
    private CompletableFuture<Void> freeSlotInternal(TaskSlot<T> taskSlot, Throwable cause) {
        AllocationID allocationId = taskSlot.getAllocationId();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Free slot {}.", taskSlot, cause);
        } else {
            LOG.info("Free slot {}.", taskSlot);
        }

        if (taskSlot.isEmpty()) {
            // remove the allocation id to task slot mapping
            allocatedSlots.remove(allocationId);

            // unregister a potential timeout 取消超时任务 注意这不同于slot被归还 而是完全释放slot占有的资源 比如内存
            timerService.unregisterTimeout(allocationId);

            JobID jobId = taskSlot.getJobId();
            Set<AllocationID> slots = slotsPerJob.get(jobId);

            if (slots == null) {
                throw new IllegalStateException(
                        "There are no more slots allocated for the job "
                                + jobId
                                + ". This indicates a programming bug.");
            }

            slots.remove(allocationId);

            if (slots.isEmpty()) {
                slotsPerJob.remove(jobId);
            }

            taskSlots.remove(taskSlot.getIndex());
            // 归还内存
            budgetManager.release(taskSlot.getResourceProfile());
        }
        return taskSlot.closeAsync(cause);
    }

    /**
     * 检查是否为slot设定了超时任务
     * @param allocationId to check against
     * @param ticket of the timeout
     * @return
     */
    @Override
    public boolean isValidTimeout(AllocationID allocationId, UUID ticket) {
        checkStarted();

        return state == State.RUNNING && timerService.isValid(allocationId, ticket);
    }

    @Override
    public boolean isAllocated(int index, JobID jobId, AllocationID allocationId) {
        TaskSlot<T> taskSlot = taskSlots.get(index);
        if (taskSlot != null) {
            return taskSlot.isAllocated(jobId, allocationId);
        } else {
            return false;
        }
    }

    @Override
    public boolean tryMarkSlotActive(JobID jobId, AllocationID allocationId) {
        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null && taskSlot.isAllocated(jobId, allocationId)) {
            return markExistingSlotActive(taskSlot);
        } else {
            return false;
        }
    }

    @Override
    public boolean isSlotFree(int index) {
        return !taskSlots.containsKey(index);
    }

    @Override
    public boolean hasAllocatedSlots(JobID jobId) {
        return getAllocatedSlots(jobId).hasNext();
    }

    @Override
    public Iterator<TaskSlot<T>> getAllocatedSlots(JobID jobId) {
        return new TaskSlotIterator(jobId, TaskSlotState.ALLOCATED);
    }

    @Override
    @Nullable
    public JobID getOwningJob(AllocationID allocationId) {
        final TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            return taskSlot.getJobId();
        } else {
            return null;
        }
    }

    // ---------------------------------------------------------------------
    // Task methods
    // ---------------------------------------------------------------------

    /**
     * 添加一个task
     * @param task to add to the task slot with the respective allocation id
     * @return
     * @throws SlotNotFoundException
     * @throws SlotNotActiveException
     */
    @Override
    public boolean addTask(T task) throws SlotNotFoundException, SlotNotActiveException {
        checkRunning();
        Preconditions.checkNotNull(task);

        TaskSlot<T> taskSlot = getTaskSlot(task.getAllocationId());

        if (taskSlot != null) {
            if (taskSlot.isActive(task.getJobID(), task.getAllocationId())) {
                // 为slot 多绑定一个task 这种是共享slot的情况吧
                if (taskSlot.add(task)) {
                    taskSlotMappings.put(
                            task.getExecutionId(), new TaskSlotMapping<>(task, taskSlot));

                    return true;
                } else {
                    return false;
                }
            } else {
                throw new SlotNotActiveException(task.getJobID(), task.getAllocationId());
            }
        } else {
            throw new SlotNotFoundException(task.getAllocationId());
        }
    }

    /**
     * 移除某个task
     * @param executionAttemptID identifying the task to remove
     * @return
     */
    @Override
    public T removeTask(ExecutionAttemptID executionAttemptID) {
        checkStarted();

        TaskSlotMapping<T> taskSlotMapping = taskSlotMappings.remove(executionAttemptID);

        if (taskSlotMapping != null) {
            T task = taskSlotMapping.getTask();
            TaskSlot<T> taskSlot = taskSlotMapping.getTaskSlot();

            // 表示该task不再依赖slot
            taskSlot.remove(task.getExecutionId());

            if (taskSlot.isReleasing() && taskSlot.isEmpty()) {
                // 被释放后 且没有依赖它的 task  就释放slot
                slotActions.freeSlot(taskSlot.getAllocationId());
            }

            return task;
        } else {
            return null;
        }
    }

    @Override
    public T getTask(ExecutionAttemptID executionAttemptID) {
        TaskSlotMapping<T> taskSlotMapping = taskSlotMappings.get(executionAttemptID);

        if (taskSlotMapping != null) {
            return taskSlotMapping.getTask();
        } else {
            return null;
        }
    }

    /**
     * 遍历job相关的task
     * @param jobId identifying the job of the requested tasks
     * @return
     */
    @Override
    public Iterator<T> getTasks(JobID jobId) {
        return new PayloadIterator(jobId);
    }

    @Override
    public AllocationID getCurrentAllocation(int index) {
        TaskSlot<T> taskSlot = taskSlots.get(index);
        if (taskSlot == null) {
            return null;
        }
        return taskSlot.getAllocationId();
    }

    @Override
    public MemoryManager getTaskMemoryManager(AllocationID allocationID)
            throws SlotNotFoundException {
        TaskSlot<T> taskSlot = getTaskSlot(allocationID);
        if (taskSlot != null) {
            return taskSlot.getMemoryManager();
        } else {
            throw new SlotNotFoundException(allocationID);
        }
    }

    // ---------------------------------------------------------------------
    // TimeoutListener methods
    // ---------------------------------------------------------------------

    /**
     * 当 timeoutServices 通知本对象某个 slot 长时间未归还  再转发给 slotActions
     * @param key identifying the timed out event
     * @param ticket used to check whether the timeout is still valid
     */
    @Override
    public void notifyTimeout(AllocationID key, UUID ticket) {
        checkStarted();

        if (slotActions != null) {
            slotActions.timeoutSlot(key, ticket);
        }
    }

    // ---------------------------------------------------------------------
    // Internal methods
    // ---------------------------------------------------------------------

    @Nullable
    private TaskSlot<T> getTaskSlot(AllocationID allocationId) {
        Preconditions.checkNotNull(allocationId);
        return allocatedSlots.get(allocationId);
    }

    private int nextDynamicSlotIndex() {
        return dynamicSlotIndex++;
    }

    private void checkRunning() {
        Preconditions.checkState(
                state == State.RUNNING,
                "The %s has to be running.",
                TaskSlotTableImpl.class.getSimpleName());
    }

    private void checkStarted() {
        Preconditions.checkState(
                state != State.CREATED,
                "The %s has to be started (not created).",
                TaskSlotTableImpl.class.getSimpleName());
    }

    // ---------------------------------------------------------------------
    // Static utility classes
    // ---------------------------------------------------------------------

    /** Mapping class between a {@link TaskSlotPayload} and a {@link TaskSlot}.
     * 将T 和 TaskSlot 关联起来 目前 T 就是 Task
     * */
    private static final class TaskSlotMapping<T extends TaskSlotPayload> {
        private final T task;
        private final TaskSlot<T> taskSlot;

        private TaskSlotMapping(T task, TaskSlot<T> taskSlot) {
            this.task = Preconditions.checkNotNull(task);
            this.taskSlot = Preconditions.checkNotNull(taskSlot);
        }

        public T getTask() {
            return task;
        }

        public TaskSlot<T> getTaskSlot() {
            return taskSlot;
        }
    }

    /**
     * Iterator over {@link TaskSlot} which fulfill a given state condition and belong to the given
     * job.
     * 可以迭代TaskSlot
     */
    private final class TaskSlotIterator implements Iterator<TaskSlot<T>> {
        private final Iterator<AllocationID> allSlots;
        private final TaskSlotState state;

        private TaskSlot<T> currentSlot;

        private TaskSlotIterator(TaskSlotState state) {
            this(
                    slotsPerJob.values().stream()
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet())
                            .iterator(),
                    state);
        }

        /**
         * 可以指定获取某个job相关的slot
         * @param jobId
         * @param state
         */
        private TaskSlotIterator(JobID jobId, TaskSlotState state) {
            this(
                    slotsPerJob.get(jobId) == null
                            ? Collections.emptyIterator()
                            : slotsPerJob.get(jobId).iterator(),
                    state);
        }

        /**
         * 手动指定 slot
         * @param allocationIDIterator
         * @param state
         */
        private TaskSlotIterator(Iterator<AllocationID> allocationIDIterator, TaskSlotState state) {
            this.allSlots = Preconditions.checkNotNull(allocationIDIterator);
            this.state = Preconditions.checkNotNull(state);
            this.currentSlot = null;
        }

        @Override
        public boolean hasNext() {
            while (currentSlot == null && allSlots.hasNext()) {
                AllocationID tempSlot = allSlots.next();

                TaskSlot<T> taskSlot = getTaskSlot(tempSlot);

                // 只会找到 state 匹配的 slot
                if (taskSlot != null && taskSlot.getState() == state) {
                    currentSlot = taskSlot;
                }
            }

            return currentSlot != null;
        }

        @Override
        public TaskSlot<T> next() {
            if (currentSlot != null) {
                TaskSlot<T> result = currentSlot;

                currentSlot = null;

                return result;
            } else {
                while (true) {
                    AllocationID tempSlot;

                    try {
                        tempSlot = allSlots.next();
                    } catch (NoSuchElementException e) {
                        throw new NoSuchElementException("No more task slots.");
                    }

                    TaskSlot<T> taskSlot = getTaskSlot(tempSlot);

                    if (taskSlot != null && taskSlot.getState() == state) {
                        return taskSlot;
                    }
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove task slots via this iterator.");
        }
    }

    /** Iterator over all {@link TaskSlotPayload} for a given job.
     * 遍历T
     * */
    private final class PayloadIterator implements Iterator<T> {
        private final Iterator<TaskSlot<T>> taskSlotIterator;

        private Iterator<T> currentTasks;

        private PayloadIterator(JobID jobId) {
            this.taskSlotIterator = new TaskSlotIterator(jobId, TaskSlotState.ACTIVE);

            this.currentTasks = null;
        }

        @Override
        public boolean hasNext() {
            while ((currentTasks == null || !currentTasks.hasNext())
                    && taskSlotIterator.hasNext()) {
                TaskSlot<T> taskSlot = taskSlotIterator.next();

                currentTasks = taskSlot.getTasks();
            }

            return (currentTasks != null && currentTasks.hasNext());
        }

        @Override
        public T next() {
            while ((currentTasks == null || !currentTasks.hasNext())) {
                TaskSlot<T> taskSlot;

                try {
                    taskSlot = taskSlotIterator.next();
                } catch (NoSuchElementException e) {
                    throw new NoSuchElementException("No more tasks.");
                }

                currentTasks = taskSlot.getTasks();
            }

            return currentTasks.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove tasks via this iterator.");
        }
    }

    /**
     * 描述表的状态
     */
    private enum State {
        CREATED,
        RUNNING,
        CLOSING,
        CLOSED
    }
}
