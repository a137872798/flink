/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.slowtaskdetector;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SlowTaskDetectorOptions;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.IterableUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.ExecutionVertex.NUM_BYTES_UNKNOWN;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The slow task detector which detects slow tasks based on their execution time.
 * 基于执行时间来检测慢任务
 * */
public class ExecutionTimeBasedSlowTaskDetector implements SlowTaskDetector {

    private final long checkIntervalMillis;

    // 这是基线

    private final long baselineLowerBoundMillis;

    private final double baselineRatio;

    private final double baselineMultiplier;

    private ScheduledFuture<?> scheduledDetectionFuture;

    private FatalErrorHandler fatalErrorHandler =
            throwable ->
                    FatalExitExceptionHandler.INSTANCE.uncaughtException(
                            Thread.currentThread(), throwable);

    public ExecutionTimeBasedSlowTaskDetector(Configuration configuration) {
        // 检查间隔
        this.checkIntervalMillis =
                configuration.get(SlowTaskDetectorOptions.CHECK_INTERVAL).toMillis();
        checkArgument(
                this.checkIntervalMillis > 0,
                "The configuration {} should be positive, but is {}.",
                SlowTaskDetectorOptions.CHECK_INTERVAL.key(),
                this.checkIntervalMillis);

        this.baselineLowerBoundMillis =
                configuration
                        .get(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_LOWER_BOUND)
                        .toMillis();
        checkArgument(
                this.baselineLowerBoundMillis >= 0,
                "The configuration {} cannot be negative, but is {}.",
                SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_LOWER_BOUND.key(),
                this.baselineLowerBoundMillis);

        this.baselineRatio =
                configuration.getDouble(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_RATIO);
        checkArgument(
                baselineRatio >= 0 && this.baselineRatio < 1,
                "The configuration {} should be in [0, 1), but is {}.",
                SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_RATIO.key(),
                this.baselineRatio);

        this.baselineMultiplier =
                configuration.getDouble(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_MULTIPLIER);
        checkArgument(
                baselineMultiplier > 0,
                "The configuration {} should be positive, but is {}.",
                SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_MULTIPLIER.key(),
                this.baselineMultiplier);
    }

    @VisibleForTesting
    ExecutionTimeBasedSlowTaskDetector(
            Configuration configuration, FatalErrorHandler fatalErrorHandler) {
        this(configuration);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
    }

    /**
     * 启动探测器
     * @param executionGraph
     * @param listener 当发现慢任务时 会进行通知
     * @param mainThreadExecutor
     */
    @Override
    public void start(
            final ExecutionGraph executionGraph,
            final SlowTaskDetectorListener listener,
            final ComponentMainThreadExecutor mainThreadExecutor) {
        scheduleTask(executionGraph, listener, mainThreadExecutor);
    }

    /** Schedule periodical slow task detection. */
    private void scheduleTask(
            final ExecutionGraph executionGraph,
            final SlowTaskDetectorListener listener,
            final ComponentMainThreadExecutor mainThreadExecutor) {
        this.scheduledDetectionFuture =
                mainThreadExecutor.schedule(
                        () -> {
                            try {
                                // 每隔一段时间 通过findSlowTasks找到慢任务 并通知监听器
                                listener.notifySlowTasks(findSlowTasks(executionGraph));
                            } catch (Throwable throwable) {
                                fatalErrorHandler.onFatalError(throwable);
                            }
                            // 开启下次任务
                            scheduleTask(executionGraph, listener, mainThreadExecutor);
                        },
                        checkIntervalMillis,
                        TimeUnit.MILLISECONDS);
    }

    /**
     * Given that the parallelism is N and the ratio is R, define T as the median of the first N*R
     * finished tasks' execution time. The baseline will be T*M, where M is the multiplier. Note
     * that the execution time will be weighted with its input bytes when calculating the median. A
     * task will be identified as slow if its weighted execution time is longer than the baseline.
     * 查询慢任务
     */
    @VisibleForTesting
    Map<ExecutionVertexID, Collection<ExecutionAttemptID>> findSlowTasks(
            final ExecutionGraph executionGraph) {
        final long currentTimeMillis = System.currentTimeMillis();

        final Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks = new HashMap<>();

        // 找到需要检测的一组 task
        final List<ExecutionJobVertex> jobVerticesToCheck = getJobVerticesToCheck(executionGraph);

        for (ExecutionJobVertex ejv : jobVerticesToCheck) {
            // 产生一个基线对象   基线相当于一个基准  这个是具备一般意义   低于该值就可以认为是慢的
            final ExecutionTimeWithInputBytes baseline = getBaseline(ejv, currentTimeMillis);

            for (ExecutionVertex ev : ejv.getTaskVertices()) {
                if (ev.getExecutionState().isTerminal()) {
                    continue;
                }

                // 低于基线的都认为是慢的
                final List<ExecutionAttemptID> slowExecutions =
                        findExecutionsExceedingBaseline(
                                ev.getCurrentExecutions(), baseline, currentTimeMillis);

                if (!slowExecutions.isEmpty()) {
                    slowTasks.put(ev.getID(), slowExecutions);
                }
            }
        }

        return slowTasks;
    }

    /**
     * 找到还未结束的任务 并检测时间
     * @param executionGraph
     * @return
     */
    private List<ExecutionJobVertex> getJobVerticesToCheck(final ExecutionGraph executionGraph) {
        return IterableUtils.toStream(executionGraph.getVerticesTopologically())
                .filter(ExecutionJobVertex::isInitialized)
                // task级别 只要还有一个subtask未结束 就不会进入 finished状态
                .filter(ejv -> ejv.getAggregateState() != ExecutionState.FINISHED)
                // 当完成的比率超过该值 才会进行下一步检测
                .filter(ejv -> getFinishedRatio(ejv) >= baselineRatio)
                .collect(Collectors.toList());
    }

    /**
     * 计算完成的比率
     * @param executionJobVertex
     * @return
     */
    private double getFinishedRatio(final ExecutionJobVertex executionJobVertex) {
        checkState(executionJobVertex.getTaskVertices().length > 0);
        long finishedCount =
                Arrays.stream(executionJobVertex.getTaskVertices())
                        .filter(ev -> ev.getExecutionState() == ExecutionState.FINISHED)
                        .count();
        return (double) finishedCount / executionJobVertex.getTaskVertices().length;
    }

    /**
     * 计算基线
     * @param executionJobVertex
     * @param currentTimeMillis
     * @return
     */
    private ExecutionTimeWithInputBytes getBaseline(
            final ExecutionJobVertex executionJobVertex, final long currentTimeMillis) {

        // 取中间值
        final ExecutionTimeWithInputBytes weightedExecutionTimeMedian =
                calculateFinishedTaskExecutionTimeMedian(executionJobVertex, currentTimeMillis);

        // 乘以系数得到一个基线值
        long multipliedBaseline =
                (long) (weightedExecutionTimeMedian.getExecutionTime() * baselineMultiplier);

        return new ExecutionTimeWithInputBytes(
                multipliedBaseline, weightedExecutionTimeMedian.getInputBytes());
    }

    /**
     * 计算基线
     * @param executionJobVertex
     * @param currentTime
     * @return
     */
    private ExecutionTimeWithInputBytes calculateFinishedTaskExecutionTimeMedian(
            final ExecutionJobVertex executionJobVertex, final long currentTime) {

        // 表示至少超过这么多子任务完成了
        final int baselineExecutionCount =
                (int) Math.round(executionJobVertex.getParallelism() * baselineRatio);

        if (baselineExecutionCount == 0) {
            // 返回空对象
            return new ExecutionTimeWithInputBytes(0L, NUM_BYTES_UNKNOWN);
        }

        // 找到完成的子任务
        final List<Execution> finishedExecutions =
                Arrays.stream(executionJobVertex.getTaskVertices())
                        .flatMap(ev -> ev.getCurrentExecutions().stream())
                        .filter(e -> e.getState() == ExecutionState.FINISHED)
                        .collect(Collectors.toList());

        checkState(finishedExecutions.size() >= baselineExecutionCount);

        final List<ExecutionTimeWithInputBytes> firstFinishedExecutions =
                finishedExecutions.stream()
                        // 产生 ExecutionTimeWithInputBytes 对象
                        .map(e -> getExecutionTimeAndInputBytes(e, currentTime))
                        .sorted()
                        .limit(baselineExecutionCount)
                        .collect(Collectors.toList());

        // 取中间的
        return firstFinishedExecutions.get(baselineExecutionCount / 2);
    }

    /**
     * 找到低于基线的执行对象
     * @param executions
     * @param baseline
     * @param currentTimeMillis
     * @return
     */
    private List<ExecutionAttemptID> findExecutionsExceedingBaseline(
            Collection<Execution> executions,
            ExecutionTimeWithInputBytes baseline,
            long currentTimeMillis) {
        return executions.stream()
                .filter(
                        // We will filter out tasks that are in the CREATED state, as we do not
                        // allow speculative execution for them because they have not been
                        // scheduled.
                        // However, for tasks that are already in the SCHEDULED state, we allow
                        // speculative execution to provide the capability of parallel execution
                        // running.
                        e ->
                                // 找到运行中的
                                !e.getState().isTerminal()
                                        && e.getState() != ExecutionState.CANCELING
                                        && e.getState() != ExecutionState.CREATED)
                .filter(
                        e -> {
                            ExecutionTimeWithInputBytes timeWithBytes =
                                    getExecutionTimeAndInputBytes(e, currentTimeMillis);
                            // 执行时间要超过这个最小值  才能认为是慢的
                            return timeWithBytes.getExecutionTime() >= baselineLowerBoundMillis
                                    && timeWithBytes.compareTo(baseline) >= 0;
                        })
                .map(Execution::getAttemptId)
                .collect(Collectors.toList());
    }

    /**
     * 获取执行时间
     * @param execution
     * @param currentTime
     * @return
     */
    private long getExecutionTime(final Execution execution, final long currentTime) {
        // 获取进入部署阶段的时间
        final long deployingTimestamp = execution.getStateTimestamp(ExecutionState.DEPLOYING);
        if (deployingTimestamp == 0) {
            return 0;
        }

        // 用另一个时间配合计算
        if (execution.getState() == ExecutionState.FINISHED) {
            return execution.getStateTimestamp(ExecutionState.FINISHED) - deployingTimestamp;
        } else {
            return currentTime - deployingTimestamp;
        }
    }

    private long getExecutionInputBytes(final Execution execution) {
        return execution.getVertex().getInputBytes();
    }

    private ExecutionTimeWithInputBytes getExecutionTimeAndInputBytes(
            Execution execution, final long currentTime) {
        long executionTime = getExecutionTime(execution, currentTime);
        long executionInputBytes = getExecutionInputBytes(execution);

        return new ExecutionTimeWithInputBytes(executionTime, executionInputBytes);
    }

    @Override
    public void stop() {
        if (scheduledDetectionFuture != null) {
            scheduledDetectionFuture.cancel(false);
        }
    }

    @VisibleForTesting
    ScheduledFuture<?> getScheduledDetectionFuture() {
        return scheduledDetectionFuture;
    }

    /** This class defines the execution time and input bytes for an execution.
     * 记录多少时间处理了多少数据
     * */
    @VisibleForTesting
    static class ExecutionTimeWithInputBytes implements Comparable<ExecutionTimeWithInputBytes> {

        private final long executionTime;
        private final long inputBytes;

        public ExecutionTimeWithInputBytes(long executionTime, long inputBytes) {
            this.executionTime = executionTime;
            this.inputBytes = inputBytes;
        }

        public long getExecutionTime() {
            return executionTime;
        }

        public long getInputBytes() {
            return inputBytes;
        }

        @Override
        public int compareTo(ExecutionTimeWithInputBytes other) {
            // In order to ensure the stability of comparison, it requires both elements' input
            // bytes should be both valid or both UNKNOWN, unless the execution time is 0.
            // (When baselineRatio is 0, a baseline of 0 execution time will be generated.)
            if (inputBytes == NUM_BYTES_UNKNOWN || other.getInputBytes() == NUM_BYTES_UNKNOWN) {
                if (inputBytes == NUM_BYTES_UNKNOWN && other.getInputBytes() == NUM_BYTES_UNKNOWN
                        || executionTime == 0
                        || other.executionTime == 0) {
                    return (int) (executionTime - other.getExecutionTime());
                } else {
                    throw new IllegalArgumentException(
                            "Both compared elements should be NUM_BYTES_UNKNOWN.");
                }
            }

            return Double.compare(
                    (double) executionTime / Math.max(inputBytes, Double.MIN_VALUE),
                    (double) other.getExecutionTime()
                            / Math.max(other.getInputBytes(), Double.MIN_VALUE));
        }
    }
}
