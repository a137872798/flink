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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricher.Context;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.throwable.ThrowableClassifier;
import org.apache.flink.runtime.throwable.ThrowableType;
import org.apache.flink.util.IterableUtils;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This handler deals with task failures to return a {@link FailureHandlingResult} which contains
 * tasks to restart to recover from failures.
 * 当执行失败时 使用该对象处理
 */
public class ExecutionFailureHandler {

    /**
     * 此时的拓扑图
     */
    private final SchedulingTopology schedulingTopology;

    /** Strategy to judge which tasks should be restarted.
     * 采用的故障转移策略 用于重新执行task
     * */
    private final FailoverStrategy failoverStrategy;

    /** Strategy to judge whether and when a restarting should be done.
     * 补偿策略相关的   控制任务的重启
     * */
    private final RestartBackoffTimeStrategy restartBackoffTimeStrategy;

    /** Number of all restarts happened since this job is submitted.
     * 记录失败了多少次
     * */
    private long numberOfRestarts;

    // 提供一些上下文信息  主要是表示当前哪个job
    private final Context taskFailureCtx;
    private final Context globalFailureCtx;

    /**
     * 这组enricher对象可以获得更详细的错误信息
     */
    private final Collection<FailureEnricher> failureEnrichers;

    /**
     * 该执行器可以判断当前是否在主线程
     */
    private final ComponentMainThreadExecutor mainThreadExecutor;

    /**
     * Creates the handler to deal with task failures.
     *
     * @param schedulingTopology contains the topology info for failover
     * @param failoverStrategy helps to decide tasks to restart on task failures
     * @param restartBackoffTimeStrategy helps to decide whether to restart failed tasks and the
     *     restarting delay
     * @param mainThreadExecutor the main thread executor of the job master
     * @param failureEnrichers a collection of {@link FailureEnricher} that enrich failures
     * @param taskFailureCtx Task failure Context used by FailureEnrichers
     * @param globalFailureCtx Global failure Context used by FailureEnrichers
     */
    public ExecutionFailureHandler(
            final SchedulingTopology schedulingTopology,
            final FailoverStrategy failoverStrategy,
            final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final Collection<FailureEnricher> failureEnrichers,
            final Context taskFailureCtx,
            final Context globalFailureCtx) {

        this.schedulingTopology = checkNotNull(schedulingTopology);
        this.failoverStrategy = checkNotNull(failoverStrategy);
        this.restartBackoffTimeStrategy = checkNotNull(restartBackoffTimeStrategy);
        this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
        this.failureEnrichers = checkNotNull(failureEnrichers);
        this.taskFailureCtx = taskFailureCtx;
        this.globalFailureCtx = globalFailureCtx;
    }

    /**
     * Return result of failure handling. Can be a set of task vertices to restart and a delay of
     * the restarting. Or that the failure is not recoverable and the reason for it.
     *
     * @param failedExecution is the failed execution  表示一个顶点的执行对象
     * @param cause of the task failure
     * @param timestamp of the task failure
     * @return result of the failure handling
     */
    public FailureHandlingResult getFailureHandlingResult(
            Execution failedExecution, Throwable cause, long timestamp) {
        return handleFailure(
                failedExecution,
                cause,
                timestamp,
                failoverStrategy.getTasksNeedingRestart(failedExecution.getVertex().getID(), cause),
                false);
    }

    /**
     * Return result of failure handling on a global failure. Can be a set of task vertices to
     * restart and a delay of the restarting. Or that the failure is not recoverable and the reason
     * for it.
     *
     * @param cause of the task failure
     * @param timestamp of the task failure
     * @return result of the failure handling
     * 处理全局异常
     */
    public FailureHandlingResult getGlobalFailureHandlingResult(
            final Throwable cause, long timestamp) {
        return handleFailure(
                null,
                cause,
                timestamp,
                // 所有顶点都需要处理
                IterableUtils.toStream(schedulingTopology.getVertices())
                        .map(SchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet()),
                true);
    }

    /**
     * 获取错误信息
     * @param cause
     * @param isGlobal
     * @return
     */
    private CompletableFuture<Map<String, String>> labelFailure(Throwable cause, boolean isGlobal) {
        if (failureEnrichers.isEmpty()) {
            return FailureEnricherUtils.EMPTY_FAILURE_LABELS;
        }
        final Context ctx = isGlobal ? globalFailureCtx : taskFailureCtx;
        return FailureEnricherUtils.labelFailure(cause, ctx, mainThreadExecutor, failureEnrichers);
    }

    /**
     * 某个顶点执行时出现错误 需要进行处理
     * @param failedExecution
     * @param cause
     * @param timestamp
     * @param verticesToRestart
     * @param globalFailure
     * @return
     */
    private FailureHandlingResult handleFailure(
            @Nullable final Execution failedExecution,
            final Throwable cause,
            long timestamp,
            final Set<ExecutionVertexID> verticesToRestart,
            final boolean globalFailure) {

        // 得到错误信息
        final CompletableFuture<Map<String, String>> failureLabels =
                labelFailure(cause, globalFailure);

        // 表示产生的是不可恢复异常
        if (isUnrecoverableError(cause)) {
            return FailureHandlingResult.unrecoverable(
                    failedExecution,
                    new JobException("The failure is not recoverable", cause),
                    timestamp,
                    failureLabels,
                    globalFailure);
        }

        // 通知策略对象产生了异常
        restartBackoffTimeStrategy.notifyFailure(cause);
        // 通过策略判断能否重启  以及获取延迟
        if (restartBackoffTimeStrategy.canRestart()) {
            // 表示该handler处理的错误数量
            numberOfRestarts++;

            return FailureHandlingResult.restartable(
                    failedExecution,
                    cause,
                    timestamp,
                    failureLabels,
                    verticesToRestart,
                    restartBackoffTimeStrategy.getBackoffTime(),
                    globalFailure);
        } else {
            return FailureHandlingResult.unrecoverable(
                    failedExecution,
                    new JobException(
                            "Recovery is suppressed by " + restartBackoffTimeStrategy, cause),
                    timestamp,
                    failureLabels,
                    globalFailure);
        }
    }

    /**
     * 判断是否是不可恢复异常
     * @param cause
     * @return
     */
    public static boolean isUnrecoverableError(Throwable cause) {
        Optional<Throwable> unrecoverableError =
                ThrowableClassifier.findThrowableOfThrowableType(
                        cause, ThrowableType.NonRecoverableError);
        return unrecoverableError.isPresent();
    }

    public long getNumberOfRestarts() {
        return numberOfRestarts;
    }
}
