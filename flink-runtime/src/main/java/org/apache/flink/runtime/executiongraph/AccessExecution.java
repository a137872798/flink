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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Optional;

/** Common interface for the runtime {@link Execution} and {@link ArchivedExecution}.
 * 提供访问接口
 * */
public interface AccessExecution {
    /**
     * Returns the {@link ExecutionAttemptID} for this Execution.
     *
     * @return ExecutionAttemptID for this execution
     * 其中还包含了重试次数   因为Execution是允许重试的
     */
    ExecutionAttemptID getAttemptId();

    /**
     * Returns the attempt number for this execution.
     *
     * @return attempt number for this execution.
     * 获取当前重试次数
     */
    int getAttemptNumber();

    /**
     * Returns the timestamps for every {@link ExecutionState}.
     *
     * @return timestamps for each state
     * 记录切换到不同状态的时间戳
     */
    long[] getStateTimestamps();

    /**
     * Returns the end timestamps for every {@link ExecutionState}.
     *
     * @return timestamps for each state
     * 就每个状态切换前的时间戳
     */
    long[] getStateEndTimestamps();

    /**
     * Returns the current {@link ExecutionState} for this execution.
     *
     * @return execution state for this execution
     * 获取当前状态
     */
    ExecutionState getState();

    /**
     * Returns the {@link TaskManagerLocation} for this execution.
     *
     * @return taskmanager location for this execution.
     * 获取TaskManager的位置
     */
    TaskManagerLocation getAssignedResourceLocation();

    /**
     * Returns the exception that caused the job to fail. This is the first root exception that was
     * not recoverable and triggered job failure.
     *
     * @return an {@code Optional} of {@link ErrorInfo} containing the {@code Throwable} and the
     *     time it was registered if an error occurred. If no error occurred an empty {@code
     *     Optional} will be returned.
     *     获取失败信息
     */
    Optional<ErrorInfo> getFailureInfo();

    /**
     * Returns the timestamp for the given {@link ExecutionState}.
     *
     * @param state state for which the timestamp should be returned
     * @return timestamp for the given state
     * 获取进入该状态的时间戳
     */
    long getStateTimestamp(ExecutionState state);

    /**
     * Returns the end timestamp for the given {@link ExecutionState}.
     *
     * @param state state for which the timestamp should be returned
     * @return timestamp for the given state
     * 结束该状态的时间戳
     */
    long getStateEndTimestamp(ExecutionState state);

    /**
     * Returns the user-defined accumulators as strings.
     *
     * @return user-defined accumulators as strings.
     * 表示一个累加的结果
     */
    StringifiedAccumulatorResult[] getUserAccumulatorsStringified();

    /**
     * Returns the subtask index of this execution.
     *
     * @return subtask index of this execution.
     * 该execution 对应的subtaskIndex
     */
    int getParallelSubtaskIndex();

    /**
     * 先忽略统计数据
     * @return
     */
    IOMetrics getIOMetrics();
}
