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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.slots.DefaultRequirementMatcher;
import org.apache.flink.runtime.slots.RequirementMatcher;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Default {@link DeclarativeSlotPool} implementation.
 *
 * <p>The implementation collects the current resource requirements and declares them at the
 * ResourceManager. Whenever new slots are offered, the slot pool compares the offered slots to the
 * set of available and required resources and only accepts those slots which are required.
 *
 * <p>Slots which are released won't be returned directly to their owners. Instead, the slot pool
 * implementation will only return them after the idleSlotTimeout has been exceeded by a free slot.
 *
 * <p>The slot pool will call {@link #newSlotsListener} whenever newly offered slots are accepted or
 * if an allocated slot should become free after it is being {@link #freeReservedSlot freed}.
 *
 * <p>This class expects 1 of 2 access patterns for changing requirements, which should not be
 * mixed:
 *
 * <p>1) the legacy approach (used by the DefaultScheduler) tightly couples requirements to reserved
 * slots. When a slot is requested it increases the requirements, when the slot is freed they are
 * decreased again. In the general case what happens is that requirements are increased, a free slot
 * is reserved, (the slot is used for a bit,) the slot is freed, the requirements are reduced. To
 * this end {@link #freeReservedSlot}, {@link #releaseSlot} and {@link #releaseSlots} return a
 * {@link ResourceCounter} describing which requirement the slot(s) were fulfilling, with the
 * expectation that the scheduler will subsequently decrease the requirements by that amount.
 *
 * <p>2) The declarative approach (used by the AdaptiveScheduler) in contrast derives requirements
 * exclusively based on what a given job currently requires. It may repeatedly reserve/free slots
 * without any modifications to the requirements.
 *
 * 使用该对象记录资源开销
 */
public class DefaultDeclarativeSlotPool implements DeclarativeSlotPool {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * 当发现需要一组新的资源时触发该函数
     */
    private final Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements;

    private final Time idleSlotTimeout;
    private final Time rpcTimeout;

    private final JobID jobId;

    /**
     * 该对象是一个 slotPool 可以添加/移除/借用/归还 slot
     */
    protected final AllocatedSlotPool slotPool;

    /**
     * 记录哪些slot 提供了该资源
     */
    private final Map<AllocationID, ResourceProfile> slotToRequirementProfileMappings;

    /**
     * ResourceCounter 内部是一份资源以及数量  表示总计需要多少资源  资源*数量
     */
    private ResourceCounter totalResourceRequirements;

    /**
     * 记录当前已经提供了多少资源
     */
    private ResourceCounter fulfilledResourceRequirements;

    /**
     * 当有新的slot可用时触发
     */
    private NewSlotsListener newSlotsListener = NoOpNewSlotsListener.INSTANCE;

    /**
     * 资源匹配对象
     */
    private final RequirementMatcher requirementMatcher = new DefaultRequirementMatcher();

    /**
     *
     * @param jobId
     * @param slotPool
     * @param notifyNewResourceRequirements  当发现需要一组新的资源时触发
     * @param idleSlotTimeout
     * @param rpcTimeout
     */
    public DefaultDeclarativeSlotPool(
            JobID jobId,
            AllocatedSlotPool slotPool,
            Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements,
            Time idleSlotTimeout,
            Time rpcTimeout) {

        this.jobId = jobId;
        this.slotPool = slotPool;
        this.notifyNewResourceRequirements = notifyNewResourceRequirements;
        this.idleSlotTimeout = idleSlotTimeout;
        this.rpcTimeout = rpcTimeout;
        this.totalResourceRequirements = ResourceCounter.empty();
        this.fulfilledResourceRequirements = ResourceCounter.empty();
        this.slotToRequirementProfileMappings = new HashMap<>();
    }

    @Override
    public void increaseResourceRequirementsBy(ResourceCounter increment) {
        if (increment.isEmpty()) {
            return;
        }
        // 追加资源开销
        totalResourceRequirements = totalResourceRequirements.add(increment);

        declareResourceRequirements();
    }

    @Override
    public void decreaseResourceRequirementsBy(ResourceCounter decrement) {
        if (decrement.isEmpty()) {
            return;
        }
        // 减少资源开销
        totalResourceRequirements = totalResourceRequirements.subtract(decrement);

        declareResourceRequirements();
    }

    @Override
    public void setResourceRequirements(ResourceCounter resourceRequirements) {
        // 直接指定资源开销
        totalResourceRequirements = resourceRequirements;

        declareResourceRequirements();
    }

    /**
     * 声明需要的资源
     */
    private void declareResourceRequirements() {
        final Collection<ResourceRequirement> resourceRequirements = getResourceRequirements();

        log.debug(
                "Declare new resource requirements for job {}.{}\trequired resources: {}{}\tacquired resources: {}",
                jobId,
                System.lineSeparator(),
                resourceRequirements,
                System.lineSeparator(),
                fulfilledResourceRequirements);

        // 每当需要的资源发生变化时  获取最新资源 并触发钩子
        notifyNewResourceRequirements.accept(resourceRequirements);
    }

    /**
     * 获取当前资源开销
     * @return
     */
    @Override
    public Collection<ResourceRequirement> getResourceRequirements() {
        // 记录所有需要的资源
        final Collection<ResourceRequirement> currentResourceRequirements = new ArrayList<>();

        for (Map.Entry<ResourceProfile, Integer> resourceRequirement :
                totalResourceRequirements.getResourcesWithCount()) {
            currentResourceRequirements.add(
                    ResourceRequirement.create(
                            resourceRequirement.getKey(), resourceRequirement.getValue()));
        }

        return currentResourceRequirements;
    }

    /**
     * 添加slots
     * @param offers offers containing the list of slots offered to this slot pool
     * @param taskManagerLocation taskManagerLocation is the location of the offering TaskExecutor
     * @param taskManagerGateway taskManagerGateway is the gateway to talk to the offering
     *     TaskExecutor
     * @param currentTime currentTime is the time the slots are being offered
     * @return  返回已经加入的slot
     *
     * 以满足requirement为前提 补充slot
     */
    @Override
    public Collection<SlotOffer> offerSlots(
            Collection<? extends SlotOffer> offers,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime) {

        log.debug("Received {} slot offers from TaskExecutor {}.", offers, taskManagerLocation);

        return internalOfferSlots(
                offers,
                taskManagerLocation,
                taskManagerGateway,
                currentTime,
                this::matchWithOutstandingRequirement);
    }

    /**
     * 添加slot
     * @param offers
     * @param taskManagerLocation
     * @param taskManagerGateway
     * @param currentTime
     * @param matchingCondition  判断当前ResourceProfile资源是否已满足requirement
     * @return  返回此时有效的slot 不被需要的slot就不用考虑
     */
    private Collection<SlotOffer> internalOfferSlots(
            Collection<? extends SlotOffer> offers,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime,
            Function<ResourceProfile, Optional<ResourceProfile>> matchingCondition) {

        // 记录已经加入过的slot
        final Collection<SlotOffer> acceptedSlotOffers = new ArrayList<>();
        // 这个是便于批量加入的
        final Collection<AllocatedSlot> acceptedSlots = new ArrayList<>();

        // 遍历每个提供的slot
        for (SlotOffer offer : offers) {
            if (slotPool.containsSlot(offer.getAllocationId())) {
                // we have already accepted this offer
                acceptedSlotOffers.add(offer);
            } else {
                // 此时该slot还未加入 将它封装成 AllocatedSlot
                Optional<AllocatedSlot> acceptedSlot =
                        matchOfferWithOutstandingRequirements(
                                offer, taskManagerLocation, taskManagerGateway, matchingCondition);
                // 表示提供的资源与某个 requirement 匹配上了
                if (acceptedSlot.isPresent()) {
                    acceptedSlotOffers.add(offer);
                    acceptedSlots.add(acceptedSlot.get());
                } else {
                    log.debug(
                            "Could not match offer {} to any outstanding requirement.",
                            offer.getAllocationId());
                }
            }
        }

        slotPool.addSlots(acceptedSlots, currentTime);

        // 表示因为接受了新的slot  触发钩子
        if (!acceptedSlots.isEmpty()) {
            log.debug(
                    "Acquired new resources; new total acquired resources: {}",
                    fulfilledResourceRequirements);
            newSlotsListener.notifyNewSlotsAreAvailable(acceptedSlots);
        }

        return acceptedSlotOffers;
    }

    /**
     * 注册一组slot
     * @param slots slots to register
     * @param taskManagerLocation taskManagerLocation is the location of the offering TaskExecutor
     * @param taskManagerGateway taskManagerGateway is the gateway to talk to the offering
     *     TaskExecutor
     * @param currentTime currentTime is the time the slots are being offered
     * @return
     */
    @Override
    public Collection<SlotOffer> registerSlots(
            Collection<? extends SlotOffer> slots,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime) {
        // This method exists to allow slots to be re-offered by recovered TMs while the job is in a
        // restarting state (where it usually hasn't set any requirements).
        // For this to work with the book-keeping of this class these slots are "matched" against
        // the ResourceProfile.ANY, which in practice isn't used.
        // If such a slot is then later reserved the mapping is updated accordingly.
        // While this approach does have the downside of somewhat hiding this special case, it
        // does allow the slot timeouts or releases to work as if the case didn't exist at all.

        log.debug("Register slots {} from TaskManager {}.", slots, taskManagerLocation);
        internalOfferSlots(
                slots,
                taskManagerLocation,
                taskManagerGateway,
                currentTime,
                this::matchWithOutstandingRequirementOrWildcard);
        return new ArrayList<>(slots);
    }

    private Optional<ResourceProfile> matchWithOutstandingRequirementOrWildcard(
            ResourceProfile resourceProfile) {
        // 先查看资源是否被满足
        final Optional<ResourceProfile> match = matchWithOutstandingRequirement(resourceProfile);

        // 表示需要该资源
        if (match.isPresent()) {
            return match;
        } else {
            // use ANY as wildcard as there is no practical purpose for a slot with 0 resources
            // TODO 先忽略ANY的逻辑
            return Optional.of(ResourceProfile.ANY);
        }
    }

    /**
     * 将一个 SlotOffer 变成 AllocatedSlot
     * @param slotOffer
     * @param taskManagerLocation
     * @param taskManagerGateway
     * @param matchingCondition 该函数是用来判断资源是否足够的
     * @return
     */
    private Optional<AllocatedSlot> matchOfferWithOutstandingRequirements(
            SlotOffer slotOffer,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Function<ResourceProfile, Optional<ResourceProfile>> matchingCondition) {

        final Optional<ResourceProfile> match =
                matchingCondition.apply(slotOffer.getResourceProfile());

        // 表示当前资源还是不足
        if (match.isPresent()) {
            final ResourceProfile matchedRequirement = match.get();
            log.debug(
                    "Matched slot offer {} to requirement {}.",
                    slotOffer.getAllocationId(),
                    matchedRequirement);

            // 将该资源作为提供方给出
            increaseAvailableResources(ResourceCounter.withResource(matchedRequirement, 1));

            final AllocatedSlot allocatedSlot =
                    createAllocatedSlot(slotOffer, taskManagerLocation, taskManagerGateway);

            // store the ResourceProfile against which the given slot has matched for future
            // book-keeping
            slotToRequirementProfileMappings.put(
                    allocatedSlot.getAllocationId(), matchedRequirement);

            return Optional.of(allocatedSlot);
        }
        return Optional.empty();
    }

    /**
     * 判断是否还需要资源
     * @param resourceProfile
     * @return
     */
    private Optional<ResourceProfile> matchWithOutstandingRequirement(
            ResourceProfile resourceProfile) {
        return requirementMatcher.match(
                resourceProfile,
                totalResourceRequirements,
                fulfilledResourceRequirements::getResourceCount);
    }

    /**
     * 返回还未满足的资源 就是总资源量-此时已经提供的资源量
     * @return
     */
    @VisibleForTesting
    ResourceCounter calculateUnfulfilledResources() {
        return totalResourceRequirements.subtract(fulfilledResourceRequirements);
    }

    /**
     * 根据相关信息 产生AllocatedSlot
     * @param slotOffer
     * @param taskManagerLocation
     * @param taskManagerGateway
     * @return
     */
    private AllocatedSlot createAllocatedSlot(
            SlotOffer slotOffer,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway) {
        return new AllocatedSlot(
                slotOffer.getAllocationId(),
                taskManagerLocation,
                slotOffer.getSlotIndex(),
                slotOffer.getResourceProfile(),
                taskManagerGateway);
    }

    /**
     * 增加可用的资源
     * @param acceptedResources
     */
    private void increaseAvailableResources(ResourceCounter acceptedResources) {
        fulfilledResourceRequirements = fulfilledResourceRequirements.add(acceptedResources);
    }

    /**
     * 获取该分配id对应的slot提供的资源
     * @param allocationId
     * @return
     */
    @Nonnull
    private ResourceProfile getMatchingResourceProfile(AllocationID allocationId) {
        return Preconditions.checkNotNull(
                slotToRequirementProfileMappings.get(allocationId),
                "No matching resource profile found for %s",
                allocationId);
    }

    /**
     * 申请一个空闲的资源
     * @param allocationId allocationId identifies the free slot to allocate
     * @param requiredSlotProfile requiredSlotProfile specifying the resource requirement
     * @return
     */
    @Override
    public PhysicalSlot reserveFreeSlot(
            AllocationID allocationId, ResourceProfile requiredSlotProfile) {
        // 从池中借走slot
        final AllocatedSlot allocatedSlot = slotPool.reserveFreeSlot(allocationId);

        Preconditions.checkState(
                allocatedSlot.getResourceProfile().isMatching(requiredSlotProfile),
                "Slot {} cannot fulfill the given requirement. SlotProfile={} Requirement={}",
                allocationId,
                allocatedSlot.getResourceProfile(),
                requiredSlotProfile);

        // 获取该slot提供的资源
        ResourceProfile previouslyMatchedResourceProfile =
                Preconditions.checkNotNull(slotToRequirementProfileMappings.get(allocationId));

        // 表示本次申请的资源与slot拥有的资源不匹配
        if (!previouslyMatchedResourceProfile.equals(requiredSlotProfile)) {
            // slots can be reserved for a requirement that is not in line with the mapping we
            // computed when the slot was offered, so we have to update the mapping
            // 更新slot拥有的资源量
            updateSlotToRequirementProfileMapping(allocationId, requiredSlotProfile);
            if (previouslyMatchedResourceProfile == ResourceProfile.ANY) {
                log.debug(
                        "Re-matched slot offer {} to requirement {}.",
                        allocationId,
                        requiredSlotProfile);
            } else {
                // adjust the requirements accordingly to ensure we still request enough slots to
                // be able to fulfill the total requirements
                // If the previous profile was ANY, then the slot was accepted without
                // being matched against a resource requirement; thus no update is needed.

                log.debug(
                        "Adjusting requirements because a slot was reserved for a different requirement than initially assumed. Slot={} assumedRequirement={} actualRequirement={}",
                        allocationId,
                        previouslyMatchedResourceProfile,
                        requiredSlotProfile);
                adjustRequirements(previouslyMatchedResourceProfile, requiredSlotProfile);
            }
        }

        return allocatedSlot;
    }

    /**
     * 归还一个slot
     * @param allocationId allocationId identifying the slot to release
     * @param cause cause for releasing the slot; can be {@code null}
     * @param currentTime currentTime when the slot was released
     * @return
     */
    @Override
    public ResourceCounter freeReservedSlot(
            AllocationID allocationId, @Nullable Throwable cause, long currentTime) {
        log.debug("Free reserved slot {}.", allocationId);

        // 将slot归还到池中
        final Optional<AllocatedSlot> freedSlot =
                slotPool.freeReservedSlot(allocationId, currentTime);

        // 得到该slot对应的资源
        Optional<ResourceCounter> previouslyFulfilledRequirement =
                freedSlot.map(Collections::singleton).map(this::getFulfilledRequirements);

        freedSlot.ifPresent(
                allocatedSlot -> {
                    // 释放payload
                    releasePayload(Collections.singleton(allocatedSlot), cause);
                    // 因为此时该slot可用 触发监听器
                    newSlotsListener.notifyNewSlotsAreAvailable(
                            Collections.singletonList(allocatedSlot));
                });

        return previouslyFulfilledRequirement.orElseGet(ResourceCounter::empty);
    }

    /**
     * 更新某个slot拥有的资源
     * @param allocationId
     * @param matchedResourceProfile
     */
    private void updateSlotToRequirementProfileMapping(
            AllocationID allocationId, ResourceProfile matchedResourceProfile) {
        final ResourceProfile oldResourceProfile =
                Preconditions.checkNotNull(
                        slotToRequirementProfileMappings.put(allocationId, matchedResourceProfile),
                        "Expected slot profile matching to be non-empty.");

        // 这里就是调整
        fulfilledResourceRequirements =
                fulfilledResourceRequirements.add(matchedResourceProfile, 1);
        fulfilledResourceRequirements =
                fulfilledResourceRequirements.subtract(oldResourceProfile, 1);
    }

    /**
     * 调整资源的需求量
     * @param oldResourceProfile
     * @param newResourceProfile
     */
    private void adjustRequirements(
            ResourceProfile oldResourceProfile, ResourceProfile newResourceProfile) {
        // slots can be reserved for a requirement that is not in line with the mapping we computed
        // when the slot was
        // offered, so we have to adjust the requirements accordingly to ensure we still request
        // enough slots to
        // be able to fulfill the total requirements
        decreaseResourceRequirementsBy(ResourceCounter.withResource(newResourceProfile, 1));
        increaseResourceRequirementsBy(ResourceCounter.withResource(oldResourceProfile, 1));
    }

    @Override
    public void registerNewSlotsListener(NewSlotsListener newSlotsListener) {
        Preconditions.checkState(
                this.newSlotsListener == NoOpNewSlotsListener.INSTANCE,
                "DefaultDeclarativeSlotPool only supports a single slot listener.");
        this.newSlotsListener = newSlotsListener;
    }

    /**
     * 释放某个资源id相关的所有slot
     * @param owner owner identifying the owning TaskExecutor
     * @param cause cause for failing the slots
     * @return
     */
    @Override
    public ResourceCounter releaseSlots(ResourceID owner, Exception cause) {
        final AllocatedSlotPool.AllocatedSlotsAndReservationStatus removedSlots =
                slotPool.removeSlots(owner);

        final Collection<AllocatedSlot> slotsToFree = new ArrayList<>();
        for (AllocatedSlot removedSlot : removedSlots.getAllocatedSlots()) {
            if (!removedSlots.wasFree(removedSlot.getAllocationId())) {
                // 收集需要释放的slot
                slotsToFree.add(removedSlot);
            }
        }

        return freeAndReleaseSlots(slotsToFree, removedSlots.getAllocatedSlots(), cause);
    }

    @Override
    public ResourceCounter releaseSlot(AllocationID allocationId, Exception cause) {
        final boolean wasSlotFree = slotPool.containsFreeSlot(allocationId);
        final Optional<AllocatedSlot> removedSlot = slotPool.removeSlot(allocationId);

        if (removedSlot.isPresent()) {
            // 释放单个slot
            final AllocatedSlot slot = removedSlot.get();

            final Collection<AllocatedSlot> slotAsCollection = Collections.singleton(slot);
            return freeAndReleaseSlots(
                    wasSlotFree ? Collections.emptySet() : slotAsCollection,
                    slotAsCollection,
                    cause);
        } else {
            return ResourceCounter.empty();
        }
    }

    /**
     *
     * @param currentlyReservedSlots  需要释放的slot
     * @param slots  所有slot
     * @param cause
     * @return
     */
    private ResourceCounter freeAndReleaseSlots(
            Collection<AllocatedSlot> currentlyReservedSlots,
            Collection<AllocatedSlot> slots,
            Exception cause) {

        // 计算这组slot拥有的资源
        ResourceCounter previouslyFulfilledRequirements =
                getFulfilledRequirements(currentlyReservedSlots);

        releasePayload(currentlyReservedSlots, cause);
        releaseSlots(slots, cause);

        return previouslyFulfilledRequirements;
    }

    /**
     * 归还资源前释放payload
     * @param allocatedSlots
     * @param cause
     */
    private void releasePayload(Iterable<? extends AllocatedSlot> allocatedSlots, Throwable cause) {
        for (AllocatedSlot allocatedSlot : allocatedSlots) {
            allocatedSlot.releasePayload(cause);
        }
    }

    /**
     * 释放空闲的slot
     * @param currentTimeMillis current time
     */
    @Override
    public void releaseIdleSlots(long currentTimeMillis) {
        final Collection<AllocatedSlotPool.FreeSlotInfo> freeSlotsInformation =
                slotPool.getFreeSlotInfoTracker().getFreeSlotsWithIdleSinceInformation();

        // 表示多出的资源
        ResourceCounter excessResources =
                fulfilledResourceRequirements.subtract(totalResourceRequirements);

        // 当前拥有的所有空闲资源
        final Iterator<AllocatedSlotPool.FreeSlotInfo> freeSlotIterator =
                freeSlotsInformation.iterator();

        final Collection<AllocatedSlot> slotsToReturnToOwner = new ArrayList<>();

        // 表示只要资源还有多出来的  且 还有空闲slot
        while (!excessResources.isEmpty() && freeSlotIterator.hasNext()) {
            final AllocatedSlotPool.FreeSlotInfo idleSlot = freeSlotIterator.next();

            if (currentTimeMillis >= idleSlot.getFreeSince() + idleSlotTimeout.toMilliseconds()) {
                // 获取slot的资源
                final ResourceProfile matchingProfile =
                        getMatchingResourceProfile(idleSlot.getAllocationId());

                // 表示包含该资源
                if (excessResources.containsResource(matchingProfile)) {
                    // 那么可以将空闲资源移除
                    excessResources = excessResources.subtract(matchingProfile, 1);
                    // 同时从pool中移除 slot
                    final Optional<AllocatedSlot> removedSlot =
                            slotPool.removeSlot(idleSlot.getAllocationId());

                    final AllocatedSlot allocatedSlot =
                            removedSlot.orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    String.format(
                                                            "Could not find slot for allocation id %s.",
                                                            idleSlot.getAllocationId())));
                    slotsToReturnToOwner.add(allocatedSlot);
                }
            }
        }

        // 表示要释放这些slot
        releaseSlots(
                slotsToReturnToOwner, new FlinkException("Returning idle slots to their owners."));
        log.debug(
                "Idle slots have been returned; new total acquired resources: {}",
                fulfilledResourceRequirements);
    }

    /**
     * 释放这组slot
     * @param slotsToReturnToOwner
     * @param cause
     */
    private void releaseSlots(Iterable<AllocatedSlot> slotsToReturnToOwner, Throwable cause) {
        for (AllocatedSlot slotToReturn : slotsToReturnToOwner) {
            Preconditions.checkState(!slotToReturn.isUsed(), "Free slot must not be used.");

            if (log.isDebugEnabled()) {
                log.info("Releasing slot [{}].", slotToReturn.getAllocationId(), cause);
            } else {
                log.info("Releasing slot [{}].", slotToReturn.getAllocationId());
            }

            final ResourceProfile matchingResourceProfile =
                    getMatchingResourceProfile(slotToReturn.getAllocationId());
            // 从提供的资源中移除
            fulfilledResourceRequirements =
                    fulfilledResourceRequirements.subtract(matchingResourceProfile, 1);
            // 不再维护allocationId与资源的映射关系
            slotToRequirementProfileMappings.remove(slotToReturn.getAllocationId());

            // 发起远程请求 完成释放
            final CompletableFuture<Acknowledge> freeSlotFuture =
                    slotToReturn
                            .getTaskManagerGateway()
                            .freeSlot(slotToReturn.getAllocationId(), cause, rpcTimeout);

            freeSlotFuture.whenComplete(
                    (Acknowledge ignored, Throwable throwable) -> {
                        if (throwable != null) {
                            // The slot status will be synced to task manager in next heartbeat.
                            log.debug(
                                    "Releasing slot [{}] of registered TaskExecutor {} failed. Discarding slot.",
                                    slotToReturn.getAllocationId(),
                                    slotToReturn.getTaskManagerId(),
                                    throwable);
                        }
                    });
        }
    }

    @Override
    public FreeSlotInfoTracker getFreeSlotInfoTracker() {
        return slotPool.getFreeSlotInfoTracker();
    }

    @Override
    public Collection<? extends SlotInfo> getAllSlotsInformation() {
        return slotPool.getAllSlotsInformation();
    }

    @Override
    public boolean containsFreeSlot(AllocationID allocationId) {
        return slotPool.containsFreeSlot(allocationId);
    }

    @Override
    public boolean containsSlots(ResourceID owner) {
        return slotPool.containsSlots(owner);
    }

    /**
     * 将这组slot的资源累加后返回
     * @param allocatedSlots
     * @return
     */
    private ResourceCounter getFulfilledRequirements(
            Iterable<? extends AllocatedSlot> allocatedSlots) {
        ResourceCounter resourceDecrement = ResourceCounter.empty();

        for (AllocatedSlot allocatedSlot : allocatedSlots) {
            final ResourceProfile matchingResourceProfile =
                    getMatchingResourceProfile(allocatedSlot.getAllocationId());
            resourceDecrement = resourceDecrement.add(matchingResourceProfile, 1);
        }

        return resourceDecrement;
    }

    @VisibleForTesting
    ResourceCounter getFulfilledResourceRequirements() {
        return fulfilledResourceRequirements;
    }
}
