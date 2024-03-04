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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/** Implementation of the {@link LogicalSlot}.
 * 表示逻辑层面的slot
 * */
public class SingleLogicalSlot implements LogicalSlot, PhysicalSlot.Payload {

    private static final AtomicReferenceFieldUpdater<SingleLogicalSlot, Payload> PAYLOAD_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    SingleLogicalSlot.class, Payload.class, "payload");

    private static final AtomicReferenceFieldUpdater<SingleLogicalSlot, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(SingleLogicalSlot.class, State.class, "state");

    /**
     * 关联的 ReqId
     */
    private final SlotRequestId slotRequestId;

    /**
     * 这是一个 PhysicalSlot
     */
    private final SlotContext slotContext;

    // locality of this slot wrt the requested preferred locations
    private final Locality locality;

    // owner of this slot to which it is returned upon release
    // 使用该对象可以回收slot
    private final SlotOwner slotOwner;

    private final CompletableFuture<Void> releaseFuture;

    /**
     * 表示当前slot的状态
     */
    private volatile State state;

    // LogicalSlot.Payload of this slot
    // 占据该slot的负载对象
    private volatile Payload payload;

    /** Whether this logical slot will be occupied indefinitely. */
    private boolean willBeOccupiedIndefinitely;

    @VisibleForTesting
    public SingleLogicalSlot(
            SlotRequestId slotRequestId,
            SlotContext slotContext,
            Locality locality,
            SlotOwner slotOwner) {

        this(slotRequestId, slotContext, locality, slotOwner, true);
    }

    public SingleLogicalSlot(
            SlotRequestId slotRequestId,
            SlotContext slotContext,
            Locality locality,
            SlotOwner slotOwner,
            boolean willBeOccupiedIndefinitely) {
        this.slotRequestId = Preconditions.checkNotNull(slotRequestId);
        this.slotContext = Preconditions.checkNotNull(slotContext);
        this.locality = Preconditions.checkNotNull(locality);
        this.slotOwner = Preconditions.checkNotNull(slotOwner);
        this.willBeOccupiedIndefinitely = willBeOccupiedIndefinitely;
        this.releaseFuture = new CompletableFuture<>();

        this.state = State.ALIVE;
        this.payload = null;
    }

    // TaskManager相关的属性从context中获得

    @Override
    public TaskManagerLocation getTaskManagerLocation() {
        return slotContext.getTaskManagerLocation();
    }

    @Override
    public TaskManagerGateway getTaskManagerGateway() {
        return slotContext.getTaskManagerGateway();
    }

    @Override
    public Locality getLocality() {
        return locality;
    }

    @Override
    public boolean isAlive() {
        return state == State.ALIVE;
    }

    @Override
    public boolean tryAssignPayload(Payload payload) {
        return PAYLOAD_UPDATER.compareAndSet(this, null, payload);
    }

    @Nullable
    @Override
    public Payload getPayload() {
        return payload;
    }

    /**
     * 将本对象标记为释放
     * @param cause why the slot was released or null if none
     * @return
     */
    @Override
    public CompletableFuture<?> releaseSlot(@Nullable Throwable cause) {
        if (STATE_UPDATER.compareAndSet(this, State.ALIVE, State.RELEASING)) {
            signalPayloadRelease(cause);
            returnSlotToOwner(payload.getTerminalStateFuture());
        }

        return releaseFuture;
    }

    @Override
    public AllocationID getAllocationId() {
        return slotContext.getAllocationId();
    }

    @Override
    public SlotRequestId getSlotRequestId() {
        return slotRequestId;
    }

    /**
     * 通过 physicalSlot 创建本对象
     * @param slotRequestId
     * @param physicalSlot
     * @param locality
     * @param slotOwner
     * @param slotWillBeOccupiedIndefinitely
     * @return
     */
    public static SingleLogicalSlot allocateFromPhysicalSlot(
            final SlotRequestId slotRequestId,
            final PhysicalSlot physicalSlot,
            final Locality locality,
            final SlotOwner slotOwner,
            final boolean slotWillBeOccupiedIndefinitely) {

        final SingleLogicalSlot singleTaskSlot =
                new SingleLogicalSlot(
                        slotRequestId,
                        physicalSlot,
                        locality,
                        slotOwner,
                        slotWillBeOccupiedIndefinitely);

        // 本对象变成了PhysicalSlot的payload
        if (physicalSlot.tryAssignPayload(singleTaskSlot)) {
            return singleTaskSlot;
        } else {
            throw new IllegalStateException(
                    "BUG: Unexpected physical slot payload assignment failure!");
        }
    }

    // -------------------------------------------------------------------------
    // AllocatedSlot.Payload implementation
    // -------------------------------------------------------------------------

    /**
     * A release of the payload by the {@link AllocatedSlot} triggers a release of the payload of
     * the logical slot.
     *
     * @param cause of the payload release
     */
    @Override
    public void release(Throwable cause) {
        if (STATE_UPDATER.compareAndSet(this, State.ALIVE, State.RELEASING)) {
            signalPayloadRelease(cause);
        }
        markReleased();
        releaseFuture.complete(null);
    }

    @Override
    public boolean willOccupySlotIndefinitely() {
        return willBeOccupiedIndefinitely;
    }

    private void signalPayloadRelease(Throwable cause) {
        // 将payload置空  同时用异常通知
        tryAssignPayload(TERMINATED_PAYLOAD);
        payload.fail(cause);
    }

    /**
     * 将本对象归还
     * @param terminalStateFuture
     */
    private void returnSlotToOwner(CompletableFuture<?> terminalStateFuture) {
        FutureUtils.assertNoException(
                terminalStateFuture.thenRun(
                        () -> {
                            if (state == State.RELEASING) {
                                slotOwner.returnLogicalSlot(this);
                            }

                            markReleased();

                            // 唤醒阻塞对象
                            releaseFuture.complete(null);
                        }));
    }

    /**
     * 修改成已释放
     */
    private void markReleased() {
        state = State.RELEASED;
    }

    // -------------------------------------------------------------------------
    // Internal classes
    // -------------------------------------------------------------------------

    enum State {
        ALIVE,
        RELEASING,
        RELEASED
    }
}
