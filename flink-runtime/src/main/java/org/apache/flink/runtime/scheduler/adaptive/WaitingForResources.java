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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

/**
 * State which describes that the scheduler is waiting for resources in order to execute the job.
 * 继Created后 进入等待资源的状态
 */
class WaitingForResources implements State, ResourceListener {

    /**
     * 上下文中包含了切换到其他状态的方法
     */
    private final Context context;

    private final Logger log;

    private final Clock clock;

    /** If set, there's an ongoing deadline waiting for a resource stabilization.
     * 表示等待的截止时间
     * */
    @Nullable private Deadline resourceStabilizationDeadline;

    private final Duration resourceStabilizationTimeout;

    @Nullable private ScheduledFuture<?> resourceTimeoutFuture;

    /**
     * 上一个执行图
     */
    @Nullable private final ExecutionGraph previousExecutionGraph;

    @VisibleForTesting
    WaitingForResources(
            Context context,
            Logger log,
            Duration initialResourceAllocationTimeout,
            Duration resourceStabilizationTimeout) {
        this(
                context,
                log,
                initialResourceAllocationTimeout,
                resourceStabilizationTimeout,
                SystemClock.getInstance(),
                null);
    }

    /**
     *
     * @param context
     * @param log
     * @param initialResourceAllocationTimeout
     * @param resourceStabilizationTimeout
     * @param clock
     * @param previousExecutionGraph
     */
    WaitingForResources(
            Context context,
            Logger log,
            Duration initialResourceAllocationTimeout,
            Duration resourceStabilizationTimeout,
            Clock clock,
            @Nullable ExecutionGraph previousExecutionGraph) {
        this.context = Preconditions.checkNotNull(context);
        this.log = Preconditions.checkNotNull(log);
        this.resourceStabilizationTimeout =
                Preconditions.checkNotNull(resourceStabilizationTimeout);
        this.clock = clock;
        Preconditions.checkNotNull(initialResourceAllocationTimeout);

        Preconditions.checkArgument(
                !resourceStabilizationTimeout.isNegative(),
                "Resource stabilization timeout must not be negative");

        // since state transitions are not allowed in state constructors, schedule calls for later.
        if (!initialResourceAllocationTimeout.isNegative()) {
            // 此时处于等待资源时  调用 resourceTimeout
            resourceTimeoutFuture =
                    context.runIfState(
                            this, this::resourceTimeout, initialResourceAllocationTimeout);
        }
        this.previousExecutionGraph = previousExecutionGraph;
        // 立即触发检查
        context.runIfState(this, this::checkDesiredOrSufficientResourcesAvailable, Duration.ZERO);
    }

    /**
     * 离开此状态时 关闭超时任务
     * @param newState newState is the state into which the scheduler transitions
     */
    @Override
    public void onLeave(Class<? extends State> newState) {
        if (resourceTimeoutFuture != null) {
            resourceTimeoutFuture.cancel(false);
        }
    }

    /**
     * 进入取消状态
     */
    @Override
    public void cancel() {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.CANCELED, null));
    }

    @Override
    public void suspend(Throwable cause) {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.SUSPENDED, cause));
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.CREATED;
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return context.getArchivedExecutionGraph(getJobStatus(), null);
    }

    @Override
    public void handleGlobalFailure(
            Throwable cause, CompletableFuture<Map<String, String>> failureLabels) {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.FAILED, cause));
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    /**
     * 当有新的资源可用时
     */
    @Override
    public void onNewResourcesAvailable() {
        checkDesiredOrSufficientResourcesAvailable();
    }

    @Override
    public void onNewResourceRequirements() {
        checkDesiredOrSufficientResourcesAvailable();
    }

    /**
     * 检查是否还需要资源
     */
    private void checkDesiredOrSufficientResourcesAvailable() {
        // 有足够的资源
        if (context.hasDesiredResources()) {
            createExecutionGraphWithAvailableResources();
            return;
        }

        // 代表满足最小需求
        if (context.hasSufficientResources()) {
            if (resourceStabilizationDeadline == null) {
                resourceStabilizationDeadline =
                        Deadline.fromNowWithClock(resourceStabilizationTimeout, clock);
            }
            // 已经超时了
            if (resourceStabilizationDeadline.isOverdue()) {
                createExecutionGraphWithAvailableResources();
            } else {
                // schedule next resource check  过一会儿再检查
                context.runIfState(
                        this,
                        this::checkDesiredOrSufficientResourcesAvailable,
                        resourceStabilizationDeadline.timeLeft());
            }
        } else {
            // clear deadline due to insufficient resources
            resourceStabilizationDeadline = null;
        }
    }

    /**
     * 当请求资源超时时  也只会按照当前资源创建执行图
     */
    private void resourceTimeout() {
        log.debug(
                "Initial resource allocation timeout triggered: Creating ExecutionGraph with available resources.");
        createExecutionGraphWithAvailableResources();
    }

    /**
     * 使用当前资源执行任务
     */
    private void createExecutionGraphWithAvailableResources() {
        context.goToCreatingExecutionGraph(previousExecutionGraph);
    }

    /** Context of the {@link WaitingForResources} state.
     * 表示可以从该状态进入 结束 和 创建执行图
     * */
    interface Context
            extends StateTransitions.ToCreatingExecutionGraph, StateTransitions.ToFinished {

        /**
         * Creates the {@link ArchivedExecutionGraph} for the given job status and cause. Cause can
         * be null if there is no failure.
         *
         * @param jobStatus jobStatus to initialize the {@link ArchivedExecutionGraph} with
         * @param cause cause describing a failure cause; {@code null} if there is none
         * @return the created {@link ArchivedExecutionGraph}
         *
         * 根据Job状态 来获取归档执行图
         */
        ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause);

        /**
         * Checks whether we have the desired resources.
         *
         * @return {@code true} if we have enough resources; otherwise {@code false}
         * 表示有足够的资源
         */
        boolean hasDesiredResources();

        /**
         * Checks if we currently have sufficient resources for executing the job.
         *
         * @return {@code true} if we have sufficient resources; otherwise {@code false}
         * 此时是否有足够的资源
         */
        boolean hasSufficientResources();

        /**
         * Runs the given action after a delay if the state at this time equals the expected state.
         *
         * @param expectedState expectedState describes the required state at the time of running
         *     the action
         * @param action action to run if the expected state equals the actual state
         * @param delay delay after which to run the action
         * @return a ScheduledFuture representing pending completion of the task
         * 在期望的状态下执行action
         */
        ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay);
    }

    static class Factory implements StateFactory<WaitingForResources> {

        private final Context context;
        private final Logger log;
        private final Duration initialResourceAllocationTimeout;
        private final Duration resourceStabilizationTimeout;
        @Nullable private final ExecutionGraph previousExecutionGraph;

        public Factory(
                Context context,
                Logger log,
                Duration initialResourceAllocationTimeout,
                Duration resourceStabilizationTimeout,
                ExecutionGraph previousExecutionGraph) {
            this.context = context;
            this.log = log;
            this.initialResourceAllocationTimeout = initialResourceAllocationTimeout;
            this.resourceStabilizationTimeout = resourceStabilizationTimeout;
            this.previousExecutionGraph = previousExecutionGraph;
        }

        public Class<WaitingForResources> getStateClass() {
            return WaitingForResources.class;
        }

        public WaitingForResources getState() {
            return new WaitingForResources(
                    context,
                    log,
                    initialResourceAllocationTimeout,
                    resourceStabilizationTimeout,
                    SystemClock.getInstance(),
                    previousExecutionGraph);
        }
    }

    @Nullable
    public ExecutionGraph getPreviousExecutionGraph() {
        return previousExecutionGraph;
    }
}
