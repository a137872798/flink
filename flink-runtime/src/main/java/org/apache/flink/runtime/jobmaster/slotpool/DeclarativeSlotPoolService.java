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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** {@link SlotPoolService} implementation for the {@link DeclarativeSlotPool}.
 * 为slot池开放一些服务接口
 * */
public class DeclarativeSlotPoolService implements SlotPoolService {

    private final JobID jobId;

    private final Time rpcTimeout;

    /**
     * 这个是slot池
     */
    private final DeclarativeSlotPool declarativeSlotPool;

    private final Clock clock;

    /**
     * 每个slot 通过ResourceID 关联一个 TaskManager
     */
    private final Set<ResourceID> registeredTaskManagers;

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * 该对象可以为设置的服务声明资源开销
     */
    private DeclareResourceRequirementServiceConnectionManager
            resourceRequirementServiceConnectionManager =
                    NoOpDeclareResourceRequirementServiceConnectionManager.INSTANCE;

    @Nullable private JobMasterId jobMasterId;

    @Nullable private String jobManagerAddress;

    private State state = State.CREATED;

    public DeclarativeSlotPoolService(
            JobID jobId,
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
            Clock clock,
            Time idleSlotTimeout,
            Time rpcTimeout) {
        this.jobId = jobId;
        this.clock = clock;
        this.rpcTimeout = rpcTimeout;
        this.registeredTaskManagers = new HashSet<>();

        this.declarativeSlotPool =
                declarativeSlotPoolFactory.create(
                        jobId, this::declareResourceRequirements, idleSlotTimeout, rpcTimeout);
    }

    protected DeclarativeSlotPool getDeclarativeSlotPool() {
        return declarativeSlotPool;
    }

    protected long getRelativeTimeMillis() {
        return clock.relativeTimeMillis();
    }

    @Override
    public <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(declarativeSlotPool.getClass())) {
            return Optional.of(clazz.cast(declarativeSlotPool));
        }

        return Optional.empty();
    }

    /**
     * 根据启动的参数  初始化 jobMasterId/jobManagerAddress
     * @param jobMasterId jobMasterId to start the service with
     * @param address address of the owner
     * @param mainThreadExecutor mainThreadExecutor to run actions in the main thread  可以简单看作一个线程池
     * @throws Exception
     */
    @Override
    public final void start(
            JobMasterId jobMasterId, String address, ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception {
        Preconditions.checkState(
                state == State.CREATED, "The DeclarativeSlotPoolService can only be started once.");

        this.jobMasterId = Preconditions.checkNotNull(jobMasterId);
        this.jobManagerAddress = Preconditions.checkNotNull(address);

        this.resourceRequirementServiceConnectionManager =
                DefaultDeclareResourceRequirementServiceConnectionManager.create(
                        mainThreadExecutor);

        onStart(mainThreadExecutor);

        state = State.STARTED;
    }

    /**
     * This method is called when the slot pool service is started. It can be overridden by
     * subclasses.
     *
     * @param componentMainThreadExecutor componentMainThreadExecutor used by this slot pool service
     */
    protected void onStart(ComponentMainThreadExecutor componentMainThreadExecutor) {}

    protected void assertHasBeenStarted() {
        Preconditions.checkState(
                state == State.STARTED, "The DeclarativeSlotPoolService has to be started.");
    }

    @Override
    public final void close() {
        if (state != State.CLOSED) {

            // 触发关闭钩子
            onClose();

            // 这里会将service置空
            resourceRequirementServiceConnectionManager.close();
            resourceRequirementServiceConnectionManager =
                    NoOpDeclareResourceRequirementServiceConnectionManager.INSTANCE;

            releaseAllTaskManagers(
                    new FlinkException("The DeclarativeSlotPoolService is being closed."));

            state = State.CLOSED;
        }
    }

    /**
     * This method is called when the slot pool service is closed. It can be overridden by
     * subclasses.
     */
    protected void onClose() {}

    /**
     * 往pool中添加slot
     * @param taskManagerLocation from which the slot offers originate  表示slot的来源
     * @param taskManagerGateway to talk to the slot offerer   用于与slot的提供者 TaskManager通讯
     * @param offers slot offers which are offered to the {@link SlotPoolService}
     * @return
     */
    @Override
    public Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers) {
        assertHasBeenStarted();

        // 要求该taskManager提前注册
        if (!isTaskManagerRegistered(taskManagerLocation.getResourceID())) {
            log.debug(
                    "Ignoring offered slots from unknown task manager {}.",
                    taskManagerLocation.getResourceID());
            return Collections.emptyList();
        }

        return declarativeSlotPool.offerSlots(
                offers, taskManagerLocation, taskManagerGateway, clock.relativeTimeMillis());
    }

    boolean isTaskManagerRegistered(ResourceID taskManagerId) {
        return registeredTaskManagers.contains(taskManagerId);
    }

    /**
     * 释放掉某个slot
     * @param taskManagerId taskManagerId is non-null if the signal comes from a TaskManager; if the
     *     signal comes from the ResourceManager, then it is null
     * @param allocationId allocationId identifies which allocation to fail
     * @param cause cause why the allocation failed
     * @return
     */
    @Override
    public Optional<ResourceID> failAllocation(
            @Nullable ResourceID taskManagerId, AllocationID allocationId, Exception cause) {
        assertHasBeenStarted();
        Preconditions.checkNotNull(allocationId);
        Preconditions.checkNotNull(
                taskManagerId,
                "This slot pool only supports failAllocation calls coming from the TaskExecutor.");

        // 释放slot
        final ResourceCounter previouslyFulfilledRequirements =
                declarativeSlotPool.releaseSlot(allocationId, cause);

        // previouslyFulfilledRequirements 表示被释放掉的资源 (原本正在被使用中)
        onFailAllocation(previouslyFulfilledRequirements);

        // 当该taskManager相关的所有slot都被释放时 返回taskManagerId
        if (declarativeSlotPool.containsSlots(taskManagerId)) {
            return Optional.empty();
        } else {
            return Optional.of(taskManagerId);
        }
    }

    /**
     * This method is called when an allocation fails. It can be overridden by subclasses.
     *
     * @param previouslyFulfilledRequirements previouslyFulfilledRequirements by the failed
     *     allocation
     */
    protected void onFailAllocation(ResourceCounter previouslyFulfilledRequirements) {}

    @Override
    public boolean registerTaskManager(ResourceID taskManagerId) {
        assertHasBeenStarted();

        log.debug("Register new TaskExecutor {}.", taskManagerId);
        return registeredTaskManagers.add(taskManagerId);
    }

    @Override
    public boolean releaseTaskManager(ResourceID taskManagerId, Exception cause) {
        assertHasBeenStarted();

        if (registeredTaskManagers.remove(taskManagerId)) {
            // 释放taskManager相关的所有slot
            internalReleaseTaskManager(taskManagerId, cause);
            return true;
        }

        return false;
    }

    /**
     *
     * @param taskManagerId identifying the TaskExecutor
     * @param cause cause for failing the slots
     */
    @Override
    public void releaseFreeSlotsOnTaskManager(ResourceID taskManagerId, Exception cause) {
        assertHasBeenStarted();
        if (isTaskManagerRegistered(taskManagerId)) {

            Collection<AllocationID> freeSlots =
                    // 找到匹配的所有slot
                    declarativeSlotPool.getFreeSlotInfoTracker().getFreeSlotsInformation().stream()
                            .filter(
                                    slotInfo ->
                                            slotInfo.getTaskManagerLocation()
                                                    .getResourceID()
                                                    .equals(taskManagerId))
                            .map(SlotInfo::getAllocationId)
                            .collect(Collectors.toSet());

            for (AllocationID allocationId : freeSlots) {
                // 释放slot
                final ResourceCounter previouslyFulfilledRequirement =
                        declarativeSlotPool.releaseSlot(allocationId, cause);
                // release free slots, previously fulfilled requirement should be empty.
                // 这表明之前slot应当的资源应当未被使用  只是回收slot而没有回收slot的资源
                Preconditions.checkState(
                        previouslyFulfilledRequirement.equals(ResourceCounter.empty()));
            }
        }
    }

    /**
     * 释放所有的taskManager
     * @param cause
     */
    private void releaseAllTaskManagers(Exception cause) {
        for (ResourceID registeredTaskManager : registeredTaskManagers) {
            internalReleaseTaskManager(registeredTaskManager, cause);
        }

        registeredTaskManagers.clear();
    }

    /**
     * 释放taskManager
     * @param taskManagerId
     * @param cause
     */
    private void internalReleaseTaskManager(ResourceID taskManagerId, Exception cause) {
        assertHasBeenStarted();

        // 释放taskManager相关的所有slot   返回的是被释放的总资源
        final ResourceCounter previouslyFulfilledRequirement =
                declarativeSlotPool.releaseSlots(taskManagerId, cause);

        onReleaseTaskManager(previouslyFulfilledRequirement);
    }

    /**
     * This method is called when a TaskManager is released. It can be overridden by subclasses.
     *
     * @param previouslyFulfilledRequirement previouslyFulfilledRequirement by the released
     *     TaskManager
     *                                       当taskManager关联的资源被释放时
     */
    protected void onReleaseTaskManager(ResourceCounter previouslyFulfilledRequirement) {}

    @Override
    public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
        assertHasBeenStarted();

        resourceRequirementServiceConnectionManager.connect(
                // 表示该服务可以声明资源开销
                resourceRequirements ->
                        // 此时通过网关告知resourceManager资源开销
                        resourceManagerGateway.declareRequiredResources(
                                jobMasterId, resourceRequirements, rpcTimeout));

        // 立即触发 声明资源开销的api
        declareResourceRequirements(declarativeSlotPool.getResourceRequirements());
    }

    /**
     * 每当声明的资源开销发生变化时  触发该方法
     * @param resourceRequirements
     */
    private void declareResourceRequirements(Collection<ResourceRequirement> resourceRequirements) {
        assertHasBeenStarted();

        resourceRequirementServiceConnectionManager.declareResourceRequirements(
                ResourceRequirements.create(jobId, jobManagerAddress, resourceRequirements));
    }

    @Override
    public void disconnectResourceManager() {
        assertHasBeenStarted();

        resourceRequirementServiceConnectionManager.disconnect();
    }

    /**
     * 将该taskManager相关的slot信息返回
     * @param taskManagerId identifies the task manager
     * @return
     */
    @Override
    public AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId) {
        assertHasBeenStarted();

        final Collection<AllocatedSlotInfo> allocatedSlotInfos = new ArrayList<>();

        for (SlotInfo slotInfo : declarativeSlotPool.getAllSlotsInformation()) {
            if (slotInfo.getTaskManagerLocation().getResourceID().equals(taskManagerId)) {
                allocatedSlotInfos.add(
                        new AllocatedSlotInfo(
                                slotInfo.getPhysicalSlotNumber(), slotInfo.getAllocationId()));
            }
        }
        return new AllocatedSlotReport(jobId, allocatedSlotInfos);
    }

    private enum State {
        CREATED,
        STARTED,
        CLOSED,
    }

    protected String getSlotServiceStatus() {
        return String.format(
                "Registered TMs: %d, registered slots: %d free slots: %d",
                registeredTaskManagers.size(),
                declarativeSlotPool.getAllSlotsInformation().size(),
                declarativeSlotPool.getFreeSlotInfoTracker().getAvailableSlots().size());
    }
}
