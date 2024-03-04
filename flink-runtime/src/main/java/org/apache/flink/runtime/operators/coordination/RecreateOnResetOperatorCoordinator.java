/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.groups.OperatorCoordinatorMetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.runtime.operators.coordination.ComponentClosingUtils.closeAsyncWithTimeout;

/**
 * A class that will recreate a new {@link OperatorCoordinator} instance when reset to checkpoint.
 */
public class RecreateOnResetOperatorCoordinator implements OperatorCoordinator {
    private static final Logger LOG =
            LoggerFactory.getLogger(RecreateOnResetOperatorCoordinator.class);
    private static final long CLOSING_TIMEOUT_MS = 60000L;

    /**
     * 该对象可以产生协调者
     */
    private final Provider provider;
    private final long closingTimeoutMs;

    /**
     * 提供一些辅助能力
     */
    private final OperatorCoordinator.Context context;

    /**
     * 可以随时替换内部的协调者
     */
    private DeferrableCoordinator coordinator;
    private boolean started;
    private volatile boolean closed;

    private RecreateOnResetOperatorCoordinator(
            OperatorCoordinator.Context context, Provider provider, long closingTimeoutMs)
            throws Exception {
        this.context = context;
        this.provider = provider;
        this.coordinator = new DeferrableCoordinator(context.getOperatorId());
        this.coordinator.createNewInternalCoordinator(context, provider);
        this.coordinator.processPendingCalls();
        this.closingTimeoutMs = closingTimeoutMs;
        this.started = false;
        this.closed = false;
    }

    @Override
    public void start() throws Exception {
        Preconditions.checkState(!started, "coordinator already started");
        started = true;
        coordinator.applyCall("start", OperatorCoordinator::start);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        coordinator.closeAsync(closingTimeoutMs);
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event)
            throws Exception {
        coordinator.applyCall(
                "handleEventFromOperator",
                c -> c.handleEventFromOperator(subtask, attemptNumber, event));
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        coordinator.applyCall(
                "executionAttemptFailed",
                c -> c.executionAttemptFailed(subtask, attemptNumber, reason));
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        coordinator.applyCall("subtaskReset", c -> c.subtaskReset(subtask, checkpointId));
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        coordinator.applyCall(
                "executionAttemptReady",
                c -> c.executionAttemptReady(subtask, attemptNumber, gateway));
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {
        coordinator.applyCall(
                "checkpointCoordinator", c -> c.checkpointCoordinator(checkpointId, resultFuture));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        coordinator.applyCall("checkpointComplete", c -> c.notifyCheckpointComplete(checkpointId));
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        coordinator.applyCall("checkpointAborted", c -> c.notifyCheckpointAborted(checkpointId));
    }

    /**
     * 包装该方法
     * @param checkpointId
     * @param checkpointData
     */
    @Override
    public void resetToCheckpoint(final long checkpointId, @Nullable final byte[] checkpointData) {
        // First bump up the coordinator epoch to fence out the active coordinator.
        LOG.info("Resetting coordinator to checkpoint.");
        // Replace the coordinator variable with a new DeferrableCoordinator instance.
        // At this point the internal coordinator of the new coordinator has not been created.
        // After this point all the subsequent calls will be made to the new coordinator.
        final DeferrableCoordinator oldCoordinator = coordinator;
        final DeferrableCoordinator newCoordinator =
                new DeferrableCoordinator(context.getOperatorId());
        coordinator = newCoordinator;
        // Close the old coordinator asynchronously in a separate closing thread.
        // The future will be completed when the old coordinator closes.
        // 关闭原协调者
        CompletableFuture<Void> closingFuture = oldCoordinator.closeAsync(closingTimeoutMs);

        // Create and possibly start the coordinator and apply all meanwhile deferred calls
        // capture the status whether the coordinator was started when this method was called
        final boolean wasStarted = this.started;

        closingFuture.whenComplete(
                (ignored, e) -> {
                    if (e != null) {
                        LOG.warn(
                                String.format(
                                        "Received exception when closing "
                                                + "operator coordinator for %s.",
                                        oldCoordinator.operatorId),
                                e);
                    }
                    if (!closed) {
                        // The previous coordinator has closed. Create a new one.
                        // 重启一个新的协调者
                        newCoordinator.createNewInternalCoordinator(context, provider);
                        newCoordinator.resetAndStart(checkpointId, checkpointData, wasStarted);
                        newCoordinator.processPendingCalls();
                    }
                });
    }

    // ---------------------

    @VisibleForTesting
    public OperatorCoordinator getInternalCoordinator() throws Exception {
        waitForAllAsyncCallsFinish();
        return coordinator.internalCoordinator;
    }

    @VisibleForTesting
    QuiesceableContext getQuiesceableContext() throws Exception {
        waitForAllAsyncCallsFinish();
        return coordinator.internalQuiesceableContext;
    }

    /**
     * 等待之前的任务结束
     * @throws Exception
     */
    @VisibleForTesting
    void waitForAllAsyncCallsFinish() throws Exception {
        CompletableFuture<Void> future = new CompletableFuture<>();
        coordinator.applyCall("waitForAllAsyncCallsFinish", c -> future.complete(null));
        future.get();
    }

    // ---------------------

    /** The provider for a private RecreateOnResetOperatorCoordinator.
     * 该对象用于产生协调者
     * */
    public abstract static class Provider implements OperatorCoordinator.Provider {
        private static final long serialVersionUID = 3002837631612629071L;
        private final OperatorID operatorID;

        public Provider(OperatorID operatorID) {
            this.operatorID = operatorID;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) throws Exception {
            return create(context, CLOSING_TIMEOUT_MS);
        }

        @VisibleForTesting
        protected OperatorCoordinator create(Context context, long closingTimeoutMs)
                throws Exception {
            return new RecreateOnResetOperatorCoordinator(context, this, closingTimeoutMs);
        }

        protected abstract OperatorCoordinator getCoordinator(OperatorCoordinator.Context context)
                throws Exception;
    }

    // ----------------------

    /**
     * A wrapper class around the operator coordinator context to allow quiescence. When a new
     * operator coordinator is created, we need to quiesce the old operator coordinator to prevent
     * it from making any further impact to the job master. This is done by quiesce the operator
     * coordinator context. After the quiescence, the "reading" methods will still work, but the
     * "writing" methods will become a no-op or fail immediately.
     * 静默上下文
     */
    @VisibleForTesting
    static class QuiesceableContext implements OperatorCoordinator.Context {
        private final OperatorCoordinator.Context context;
        private volatile boolean quiesced;

        QuiesceableContext(OperatorCoordinator.Context context) {
            this.context = context;
            quiesced = false;
        }

        @Override
        public OperatorID getOperatorId() {
            return context.getOperatorId();
        }

        @Override
        public OperatorCoordinatorMetricGroup metricGroup() {
            return context.metricGroup();
        }

        /**
         * 本对象被静默后  就不会处理异常了
         * @param cause
         */
        @Override
        public synchronized void failJob(Throwable cause) {
            if (quiesced) {
                return;
            }
            context.failJob(cause);
        }

        @Override
        public int currentParallelism() {
            return context.currentParallelism();
        }

        @Override
        public ClassLoader getUserCodeClassloader() {
            return context.getUserCodeClassloader();
        }

        @Override
        public CoordinatorStore getCoordinatorStore() {
            return context.getCoordinatorStore();
        }

        @Override
        public boolean isConcurrentExecutionAttemptsSupported() {
            return context.isConcurrentExecutionAttemptsSupported();
        }

        @VisibleForTesting
        synchronized void quiesce() {
            quiesced = true;
        }

        @VisibleForTesting
        boolean isQuiesced() {
            return quiesced;
        }

        private OperatorCoordinator.Context getContext() {
            return context;
        }
    }

    /**
     * A class that helps realize the fully async {@link #resetToCheckpoint(long, byte[])} behavior.
     * The class wraps an {@link OperatorCoordinator} instance. It is going to be accessed by two
     * different thread: the scheduler thread and the closing thread created in {@link
     * #closeAsync(long)}. A DeferrableCoordinator could be in three states:
     *
     * <ul>
     *   <li><b>deferred:</b> The internal {@link OperatorCoordinator} has not been created and all
     *       the method calls to the RecreateOnResetOperatorCoordinator are added to a Queue.
     *   <li><b>catching up:</b> The internal {@link OperatorCoordinator} has been created and is
     *       processing the queued up method calls. In this state, all the method calls to the
     *       RecreateOnResetOperatorCoordinator are still going to be enqueued to ensure the correct
     *       execution order.
     *   <li><b>caught up:</b> The internal {@link OperatorCoordinator} has finished processing all
     *       the queued up method calls. From this point on, the method calls to this coordinator
     *       will be executed in the caller thread directly instead of being put into the queue.
     * </ul>
     *
     * 表示一个可延期的对象
     */
    private static class DeferrableCoordinator {
        private final OperatorID operatorId;

        /**
         * 维护一组待作用在协调者上的函数
         */
        private final BlockingQueue<NamedCall> pendingCalls;

        /**
         * 上下文被静默后 就不会处理异常了
         */
        private QuiesceableContext internalQuiesceableContext;

        /**
         * 内部包含的协调者对象
         */
        private OperatorCoordinator internalCoordinator;

        /**
         * 表示追赶上来了
         */
        private boolean hasCaughtUp;
        private boolean closed;
        private volatile boolean failed;

        private DeferrableCoordinator(OperatorID operatorId) {
            this.operatorId = operatorId;
            this.pendingCalls = new LinkedBlockingQueue<>();
            this.hasCaughtUp = false;
            this.closed = false;
            this.failed = false;
        }

        synchronized <T extends Exception> void applyCall(
                String name, ThrowingConsumer<OperatorCoordinator, T> call) throws T {
            synchronized (this) {
                if (hasCaughtUp) {
                    // The new coordinator has caught up.
                    // 表示队列已清空  直接作用即可
                    call.accept(internalCoordinator);
                } else {
                    // 否则暂存
                    pendingCalls.add(new NamedCall(name, call));
                }
            }
        }

        synchronized void createNewInternalCoordinator(
                OperatorCoordinator.Context context, Provider provider) {
            if (closed) {
                return;
            }
            // Create a new internal coordinator and a new quiesceable context.
            // We assume that the coordinator creation is fast. Otherwise the creation
            // of the new internal coordinator may block the applyCall() method
            // which is invoked in the scheduler main thread.
            try {
                // 更新内部的协调者
                internalQuiesceableContext = new QuiesceableContext(context);
                internalCoordinator = provider.getCoordinator(internalQuiesceableContext);
            } catch (Exception e) {
                LOG.error("Failed to create new internal coordinator due to ", e);
                cleanAndFailJob(e);
            }
        }

        synchronized CompletableFuture<Void> closeAsync(long timeoutMs) {
            closed = true;
            if (internalCoordinator != null) {
                internalQuiesceableContext.quiesce();
                // 处理所有消费者
                pendingCalls.clear();
                return closeAsyncWithTimeout(
                                "SourceCoordinator for " + operatorId,
                                (ThrowingRunnable<Exception>) internalCoordinator::close,
                                Duration.ofMillis(timeoutMs))
                        .exceptionally(
                                e -> {
                                    cleanAndFailJob(e);
                                    return null;
                                });
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }

        /**
         * 处理囤积的消费者
         */
        void processPendingCalls() {
            if (failed || closed || internalCoordinator == null) {
                return;
            }
            String name = "Unknown Call Name";
            try {
                while (!hasCaughtUp) {
                    while (!pendingCalls.isEmpty()) {
                        NamedCall namedCall = pendingCalls.poll();
                        if (namedCall != null) {
                            name = namedCall.name;
                            namedCall.getConsumer().accept(internalCoordinator);
                        }
                    }
                    synchronized (this) {
                        // We need to check the pending calls queue again in case a new
                        // pending call is added after we process the last one and before
                        // we grab the lock.
                        if (pendingCalls.isEmpty()) {
                            hasCaughtUp = true;
                        }
                    }
                }
            } catch (Throwable t) {
                LOG.error("Failed to process pending calls {} on coordinator.", name, t);
                cleanAndFailJob(t);
            }
        }

        void start() throws Exception {
            internalCoordinator.start();
        }

        void resetAndStart(
                final long checkpointId,
                @Nullable final byte[] checkpointData,
                final boolean started) {

            if (failed || closed || internalCoordinator == null) {
                return;
            }
            try {
                internalCoordinator.resetToCheckpoint(checkpointId, checkpointData);
                // Start the new coordinator if this coordinator has been started before reset to
                // the checkpoint.
                if (started) {
                    internalCoordinator.start();
                }
            } catch (Exception e) {
                LOG.error("Failed to reset the coordinator to checkpoint and start.", e);
                cleanAndFailJob(e);
            }
        }

        private void cleanAndFailJob(Throwable t) {
            // Don't repeatedly fail the job.
            if (!failed) {
                failed = true;
                internalQuiesceableContext.getContext().failJob(t);
                pendingCalls.clear();
            }
        }
    }

    /**
     * 包装消费者 并给予一个名字
     */
    private static class NamedCall {
        private final String name;
        private final ThrowingConsumer<OperatorCoordinator, ?> consumer;

        private NamedCall(String name, ThrowingConsumer<OperatorCoordinator, ?> consumer) {
            this.name = name;
            this.consumer = consumer;
        }

        public String getName() {
            return name;
        }

        public ThrowingConsumer<OperatorCoordinator, ?> getConsumer() {
            return consumer;
        }
    }
}
