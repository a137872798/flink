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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.util.IncompleteFuturesTracker;
import org.apache.flink.runtime.util.Runnables;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of the {@link OperatorCoordinator.SubtaskGateway} interface that access to
 * subtasks for status and event sending via {@link SubtaskAccess}.
 *
 * <p>Instances of this class can be closed, blocking events from going through, buffering them, and
 * releasing them later. If the instance is closed for a specific checkpoint, events arrived after
 * that would be blocked temporarily, and released after the checkpoint finishes. If an event is
 * blocked & buffered when there are multiple ongoing checkpoints, the event would be released after
 * all these checkpoints finish.
 *
 * <p>The methods on the critical communication path, including closing/reopening the gateway and
 * sending the operator events, are required to be used in a single-threaded context specified by
 * the {@link #mainThreadExecutor} in {@link #SubtaskGatewayImpl(SubtaskAccess,
 * ComponentMainThreadExecutor, IncompleteFuturesTracker)} in order to avoid race condition.
 * 作为网关 可以给子任务发送事件
 */
class SubtaskGatewayImpl implements OperatorCoordinator.SubtaskGateway {

    private static final String EVENT_LOSS_ERROR_MESSAGE =
            "An OperatorEvent from an OperatorCoordinator to a task was lost. "
                    + "Triggering task failover to ensure consistency. Event: '%s', targetTask: %s";

    private static final long NO_CHECKPOINT = Long.MIN_VALUE;

    /**
     * 通过该对象可以访问 子任务的属性
     */
    private final SubtaskAccess subtaskAccess;

    private final ComponentMainThreadExecutor mainThreadExecutor;

    /**
     * 使用该对象来管理未完成的future
     */
    private final IncompleteFuturesTracker incompleteFuturesTracker;

    /**
     * 维护一些事件
     */
    private final TreeMap<Long, List<BlockedEvent>> blockedEventsMap;

    /** The ids of the checkpoints that have been marked but not unmarked yet.
     * 当前标记的一些检查点id
     * */
    private final TreeSet<Long> currentMarkedCheckpointIds;

    /** The id of the latest checkpoint that has ever been marked.
     * 最后标记的检查点id
     * */
    private long latestAttemptedCheckpointId;

    SubtaskGatewayImpl(
            SubtaskAccess subtaskAccess,
            ComponentMainThreadExecutor mainThreadExecutor,
            IncompleteFuturesTracker incompleteFuturesTracker) {
        this.subtaskAccess = subtaskAccess;
        this.mainThreadExecutor = mainThreadExecutor;
        this.incompleteFuturesTracker = incompleteFuturesTracker;
        this.blockedEventsMap = new TreeMap<>();
        this.currentMarkedCheckpointIds = new TreeSet<>();
        this.latestAttemptedCheckpointId = NO_CHECKPOINT;
    }

    /**
     * 使用网关发送事件 并返回一个future
     * @param evt
     * @return
     */
    @Override
    public CompletableFuture<Acknowledge> sendEvent(OperatorEvent evt) {
        if (!isReady()) {
            throw new FlinkRuntimeException("SubtaskGateway is not ready, task not yet running.");
        }

        final SerializedValue<OperatorEvent> serializedEvent;
        try {
            serializedEvent = new SerializedValue<>(evt);
        } catch (IOException e) {
            // we do not expect that this exception is handled by the caller, so we make it
            // unchecked so that it can bubble up
            throw new FlinkRuntimeException("Cannot serialize operator event", e);
        }

        // 内部通过 Execution 来发送事件  注意这是一个call对象 也就是 还没发送事件
        final Callable<CompletableFuture<Acknowledge>> sendAction =
                subtaskAccess.createEventSendAction(serializedEvent);

        final CompletableFuture<Acknowledge> sendResult = new CompletableFuture<>();
        final CompletableFuture<Acknowledge> result =
                sendResult.whenCompleteAsync(
                        (success, failure) -> {
                            // 发送失败时 要通知 Execution 失败了
                            if (failure != null && subtaskAccess.isStillRunning()) {
                                String msg =
                                        String.format(
                                                EVENT_LOSS_ERROR_MESSAGE,
                                                evt,
                                                subtaskAccess.subtaskName());
                                Runnables.assertNoException(
                                        () ->
                                                subtaskAccess.triggerTaskFailover(
                                                        new FlinkException(msg, failure)));
                            }
                        },
                        mainThreadExecutor);

        mainThreadExecutor.execute(
                () -> {
                    sendEventInternal(sendAction, sendResult);
                    incompleteFuturesTracker.trackFutureWhileIncomplete(result);
                });

        return result;
    }

    /**
     * 所有要发送的事件需要排队 如果直接发送可能会由于网络问题 产生乱序问题
     * @param sendAction
     * @param result
     */
    private void sendEventInternal(
            Callable<CompletableFuture<Acknowledge>> sendAction,
            CompletableFuture<Acknowledge> result) {
        checkRunsInMainThread();

        // 代表之前的事件还未处理完 追加一个
        if (!blockedEventsMap.isEmpty()) {
            // 默认是加入到最后一个检查点
            blockedEventsMap.lastEntry().getValue().add(new BlockedEvent(sendAction, result));
        } else {
            callSendAction(sendAction, result);
        }
    }

    private void callSendAction(
            Callable<CompletableFuture<Acknowledge>> sendAction,
            CompletableFuture<Acknowledge> result) {
        try {
            // 这里才是真正发送事件
            final CompletableFuture<Acknowledge> sendResult = sendAction.call();
            FutureUtils.forward(sendResult, result);
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalError(t);
            result.completeExceptionally(t);
        }
    }

    /**
     * 获取关联子任务的尝试次数
     * @return
     */
    @Override
    public ExecutionAttemptID getExecution() {
        return subtaskAccess.currentAttempt();
    }

    /**
     * 获取子任务对应的编号  任务有并行度 就体现为多个子任务
     * @return
     */
    @Override
    public int getSubtask() {
        return subtaskAccess.getSubtaskIndex();
    }

    private boolean isReady() {
        return subtaskAccess.hasSwitchedToRunning().isDone();
    }

    /**
     * Marks the gateway for the next checkpoint. This remembers the checkpoint ID and will only
     * allow closing the gateway for this specific checkpoint.
     *
     * <p>This is the gateway's mechanism to detect situations where multiple coordinator
     * checkpoints would be attempted overlapping, which is currently not supported (the gateway
     * doesn't keep a list of events blocked per checkpoint). It also helps to identify situations
     * where the checkpoint was aborted even before the gateway was closed (by finding out that the
     * {@code currentCheckpointId} was already reset to {@code NO_CHECKPOINT}.
     * 表示又要增加一个检查点
     */
    void markForCheckpoint(long checkpointId) {
        checkRunsInMainThread();

        // 只有递增的检查点才被承认
        if (checkpointId > latestAttemptedCheckpointId) {
            currentMarkedCheckpointIds.add(checkpointId);
            latestAttemptedCheckpointId = checkpointId;
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Regressing checkpoint IDs. Previous checkpointId = %d, new checkpointId = %d",
                            latestAttemptedCheckpointId, checkpointId));
        }
    }

    /**
     * Closes the gateway. All events sent through this gateway are blocked until the gateway is
     * re-opened. If the gateway is already closed, this does nothing.
     *
     * @return True if the gateway is closed, false if the checkpointId is incorrect.
     * 尝试关闭某个网关
     */
    boolean tryCloseGateway(long checkpointId) {
        checkRunsInMainThread();

        if (currentMarkedCheckpointIds.contains(checkpointId)) {
            // 清空之前待发送的事件
            blockedEventsMap.putIfAbsent(checkpointId, new LinkedList<>());
            return true;
        }

        return false;
    }

    /**
     * 当协调者收到AcknowledgeCheckpointEvent时  触发该方法
     * 检查点完成/禁止时 也触发该方法
     * @param checkpointId
     */
    void openGatewayAndUnmarkCheckpoint(long checkpointId) {
        checkRunsInMainThread();

        // Gateways should always be marked and closed for a specific checkpoint before it can be
        // reopened for that checkpoint. If a gateway is to be opened for an unforeseen checkpoint,
        // exceptions should be thrown.
        // 这个检查点不能超前
        if (latestAttemptedCheckpointId < checkpointId) {
            throw new IllegalStateException(
                    String.format(
                            "Trying to open gateway for unseen checkpoint: "
                                    + "latest known checkpoint = %d, incoming checkpoint = %d",
                            latestAttemptedCheckpointId, checkpointId));
        }

        // The message to open gateway with a specific checkpoint id might arrive after the
        // checkpoint has been aborted, or even after a new checkpoint has started. In these cases
        // this message should be ignored.
        // 该检查点需要被提前报备
        if (!currentMarkedCheckpointIds.contains(checkpointId)) {
            return;
        }

        // 表示该检查点还有事件未处理
        if (blockedEventsMap.containsKey(checkpointId)) {
            // TODO 为什么会出现不同的情况呢
            if (blockedEventsMap.firstKey() == checkpointId) {
                // 全部发送
                for (BlockedEvent blockedEvent : blockedEventsMap.firstEntry().getValue()) {
                    callSendAction(blockedEvent.sendAction, blockedEvent.future);
                }
            } else {
                // 将当前检查点事件 移动到前一个检查点中
                blockedEventsMap
                        .floorEntry(checkpointId - 1)
                        .getValue()
                        .addAll(blockedEventsMap.get(checkpointId));
            }
            blockedEventsMap.remove(checkpointId);
        }

        currentMarkedCheckpointIds.remove(checkpointId);
    }

    /** Opens the gateway, releasing all buffered events.
     * 发送所有囤积的事件 并清空相关数据
     * */
    void openGatewayAndUnmarkAllCheckpoint() {
        checkRunsInMainThread();

        for (List<BlockedEvent> blockedEvents : blockedEventsMap.values()) {
            for (BlockedEvent blockedEvent : blockedEvents) {
                callSendAction(blockedEvent.sendAction, blockedEvent.future);
            }
        }

        blockedEventsMap.clear();
        currentMarkedCheckpointIds.clear();
    }

    /**
     * 处理最后的检查点
     */
    void openGatewayAndUnmarkLastCheckpointIfAny() {
        if (!currentMarkedCheckpointIds.isEmpty()) {
            openGatewayAndUnmarkCheckpoint(currentMarkedCheckpointIds.last());
        }
    }

    private void checkRunsInMainThread() {
        mainThreadExecutor.assertRunningInMainThread();
    }

    private static final class BlockedEvent {

        /**
         * 表示在调用后 会返回一个结果 并且是异步操作
         */
        private final Callable<CompletableFuture<Acknowledge>> sendAction;
        private final CompletableFuture<Acknowledge> future;

        BlockedEvent(
                Callable<CompletableFuture<Acknowledge>> sendAction,
                CompletableFuture<Acknowledge> future) {
            this.sendAction = sendAction;
            this.future = future;
        }
    }
}
