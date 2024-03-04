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

package org.apache.flink.runtime.scheduler.stopwithsavepoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * {@code StopWithSavepointTerminationManager} fulfills the contract given by {@link
 * StopWithSavepointTerminationHandler} to run the stop-with-savepoint steps in a specific order.
 * 表示因为要生成保存点而停止
 */
public class StopWithSavepointTerminationManager {

    /**
     * 该对象包含一些处理逻辑
     */
    private final StopWithSavepointTerminationHandler stopWithSavepointTerminationHandler;

    public StopWithSavepointTerminationManager(
            StopWithSavepointTerminationHandler stopWithSavepointTerminationHandler) {
        this.stopWithSavepointTerminationHandler =
                Preconditions.checkNotNull(stopWithSavepointTerminationHandler);
    }

    /**
     * Enforces the correct completion order of the passed {@code CompletableFuture} instances in
     * accordance to the contract of {@link StopWithSavepointTerminationHandler}.
     *
     * @param completedSavepointFuture The {@code CompletableFuture} of the savepoint creation step.
     * @param terminatedExecutionStatesFuture The {@code CompletableFuture} of the termination step.
     * @param mainThreadExecutor The executor the {@code StopWithSavepointTerminationHandler}
     *     operations run on.
     * @return A {@code CompletableFuture} containing the path to the created savepoint.
     * 产生保存点
     */
    public CompletableFuture<String> stopWithSavepoint(
            CompletableFuture<CompletedCheckpoint> completedSavepointFuture,
            CompletableFuture<Collection<ExecutionState>> terminatedExecutionStatesFuture,  // 等待各子任务结束
            ComponentMainThreadExecutor mainThreadExecutor) {
        FutureUtils.assertNoException(
                // 监听 triggerSavepoint的结果
                completedSavepointFuture
                        // the completedSavepointFuture could also be completed by
                        // CheckpointCanceller which doesn't run in the mainThreadExecutor
                        .handleAsync(
                                (completedSavepoint, throwable) -> {
                                    stopWithSavepointTerminationHandler.handleSavepointCreation(
                                            completedSavepoint, throwable);
                                    return null;
                                },
                                mainThreadExecutor)
                        .thenRun(
                                () ->
                                        FutureUtils.assertNoException(
                                                // the execution termination has to run in a
                                                // separate Runnable to disconnect it from any
                                                // previous task failure handling
                                                terminatedExecutionStatesFuture.thenAcceptAsync(
                                                        stopWithSavepointTerminationHandler
                                                                ::handleExecutionsTermination,
                                                        mainThreadExecutor))));

        return stopWithSavepointTerminationHandler.getSavepointPath();
    }

    /**
     * 检查保存点的前置条件
     * @param checkpointCoordinator
     * @param targetDirectory
     * @param jobId
     * @param logger
     */
    public static void checkSavepointActionPreconditions(
            CheckpointCoordinator checkpointCoordinator,
            @Nullable String targetDirectory,
            JobID jobId,
            Logger logger) {
        if (checkpointCoordinator == null) {
            throw new IllegalStateException(String.format("Job %s is not a streaming job.", jobId));
        }

        // 要求有保存点位置
        if (targetDirectory == null
                && !checkpointCoordinator.getCheckpointStorage().hasDefaultSavepointLocation()) {
            logger.info(
                    "Trying to cancel job {} with savepoint, but no savepoint directory configured.",
                    jobId);

            throw new IllegalStateException(
                    "No savepoint directory configured. "
                            + "You can either specify a directory via configure a cluster-wide "
                            + "default via key '"
                            + CheckpointingOptions.SAVEPOINT_DIRECTORY.key()
                            + "' or specify a directory in the command line, like "
                            + "-s :targetDirectory for cancelling, "
                            + "-p :targetDirectory for stopping "
                            + "or :targetDirectory for purely taking savepoint.");
        }
    }
}
