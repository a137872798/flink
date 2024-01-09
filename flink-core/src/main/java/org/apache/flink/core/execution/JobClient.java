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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** A client that is scoped to a specific job.
 * 该客户端仅限于 特定的job
 * */
@PublicEvolving
public interface JobClient {

    /** Returns the {@link JobID} that uniquely identifies the job this client is scoped to.
     * 返回该client 关联的job
     * */
    JobID getJobID();

    /** Requests the {@link JobStatus} of the associated job.
     * 返回此时job的状态
     * */
    CompletableFuture<JobStatus> getJobStatus();

    /** Cancels the associated job.
     * 取消关联的job
     * */
    CompletableFuture<Void> cancel();

    /**
     * Stops the associated job on Flink cluster.
     *
     * <p>Stopping works only for streaming programs. Be aware, that the job might continue to run
     * for a while after sending the stop command, because after sources stopped to emit data all
     * operators need to finish processing.
     *
     * @param advanceToEndOfEventTime flag indicating if the source should inject a {@code
     *     MAX_WATERMARK} in the pipeline
     * @param savepointDirectory directory the savepoint should be written to
     * @return a {@link CompletableFuture} containing the path where the savepoint is located
     * @deprecated pass the format explicitly
     */
    @Deprecated
    default CompletableFuture<String> stopWithSavepoint(
            boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
        return stopWithSavepoint(
                advanceToEndOfEventTime, savepointDirectory, SavepointFormatType.DEFAULT);
    }

    /**
     * Stops the associated job on Flink cluster.
     *
     * <p>Stopping works only for streaming programs. Be aware, that the job might continue to run
     * for a while after sending the stop command, because after sources stopped to emit data all
     * operators need to finish processing.
     *
     * @param advanceToEndOfEventTime flag indicating if the source should inject a {@code
     *     MAX_WATERMARK} in the pipeline
     * @param savepointDirectory directory the savepoint should be written to
     * @param formatType binary format of the savepoint
     * @return a {@link CompletableFuture} containing the path where the savepoint is located
     *
     * stop操作 仅限于流式程序
     */
    CompletableFuture<String> stopWithSavepoint(
            boolean advanceToEndOfEventTime,   // 表示源是否应当注入管道
            @Nullable String savepointDirectory,  // 保存点存储的目录
            SavepointFormatType formatType  // 保存点使用的格式
    );

    /**
     * Triggers a savepoint for the associated job. The savepoint will be written to the given
     * savepoint directory, or {@link
     * org.apache.flink.configuration.CheckpointingOptions#SAVEPOINT_DIRECTORY} if it is null.
     *
     * @param savepointDirectory directory the savepoint should be written to
     * @return a {@link CompletableFuture} containing the path where the savepoint is located
     * @deprecated pass the format explicitly
     *
     * 触发完成一次 保存点的写入
     */
    @Deprecated
    default CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
        return triggerSavepoint(savepointDirectory, SavepointFormatType.DEFAULT);
    }

    /**
     * Triggers a savepoint for the associated job. The savepoint will be written to the given
     * savepoint directory, or {@link
     * org.apache.flink.configuration.CheckpointingOptions#SAVEPOINT_DIRECTORY} if it is null.
     *
     * @param savepointDirectory directory the savepoint should be written to
     * @param formatType binary format of the savepoint
     * @return a {@link CompletableFuture} containing the path where the savepoint is located
     */
    CompletableFuture<String> triggerSavepoint(
            @Nullable String savepointDirectory, SavepointFormatType formatType);

    /**
     * Requests the accumulators of the associated job. Accumulators can be requested while it is
     * running or after it has finished. The class loader is used to deserialize the incoming
     * accumulator results.
     *
     * 获取累加器    client为什么会有这样的api?
     */
    CompletableFuture<Map<String, Object>> getAccumulators();

    /** Returns the {@link JobExecutionResult result of the job execution} of the submitted job.
     * JobExecutionResult 中主要存储的就是累加器的结果
     * */
    CompletableFuture<JobExecutionResult> getJobExecutionResult();

    /** The client reports the heartbeat to the dispatcher for aliveness.
     * 客户端需要定时发送心跳 向dispatcher证明自己存活
     * */
    default void reportHeartbeat(long expiredTimestamp) {}
}
