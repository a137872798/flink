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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * All the logic related to taking checkpoints of the {@link OperatorCoordinator}s.
 *
 * <p>NOTE: This class has a simplified error handling logic. If one of the several coordinator
 * checkpoints fail, no cleanup is triggered for the other concurrent ones. That is okay, since they
 * all produce just byte[] as the result. We have to change that once we allow then to create
 * external resources that actually need to be cleaned up.
 * 提供一些静态方法 起辅助作用
 */
final class OperatorCoordinatorCheckpoints {

    /**
     *
     * @param coordinatorContext  该对象会监听检查点的生命周期
     * @param checkpointId
     * @return
     * @throws Exception
     */
    public static CompletableFuture<CoordinatorSnapshot> triggerCoordinatorCheckpoint(
            final OperatorCoordinatorCheckpointContext coordinatorContext, final long checkpointId)
            throws Exception {

        final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        // 触发context相关的函数
        coordinatorContext.checkpointCoordinator(checkpointId, checkpointFuture);

        // 当完成后  将结果信息包装成快照对象
        return checkpointFuture.thenApply(
                (state) ->
                        new CoordinatorSnapshot(
                                coordinatorContext,
                                new ByteStreamStateHandle(
                                        coordinatorContext.operatorId().toString(), state)));
    }

    /**
     * 让所有上下文触发
     * @param coordinators
     * @param checkpointId
     * @return
     * @throws Exception
     */
    public static CompletableFuture<AllCoordinatorSnapshots> triggerAllCoordinatorCheckpoints(
            final Collection<OperatorCoordinatorCheckpointContext> coordinators,
            final long checkpointId)
            throws Exception {

        final Collection<CompletableFuture<CoordinatorSnapshot>> individualSnapshots =
                new ArrayList<>(coordinators.size());

        for (final OperatorCoordinatorCheckpointContext coordinator : coordinators) {
            final CompletableFuture<CoordinatorSnapshot> checkpointFuture =
                    triggerCoordinatorCheckpoint(coordinator, checkpointId);
            individualSnapshots.add(checkpointFuture);
        }

        return FutureUtils.combineAll(individualSnapshots).thenApply(AllCoordinatorSnapshots::new);
    }

    /**
     * 触发所有协调者的ack
     * @param coordinators
     * @param checkpoint
     * @param acknowledgeExecutor
     * @return
     * @throws Exception
     */
    public static CompletableFuture<Void> triggerAndAcknowledgeAllCoordinatorCheckpoints(
            final Collection<OperatorCoordinatorCheckpointContext> coordinators,
            final PendingCheckpoint checkpoint,
            final Executor acknowledgeExecutor)
            throws Exception {

        // 触发所有检查点
        final CompletableFuture<AllCoordinatorSnapshots> snapshots =
                triggerAllCoordinatorCheckpoints(coordinators, checkpoint.getCheckpointID());

        return snapshots.thenAcceptAsync(
                (allSnapshots) -> {
                    try {
                        // 触发协调者的ack
                        acknowledgeAllCoordinators(checkpoint, allSnapshots.snapshots);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                },
                acknowledgeExecutor);
    }

    public static CompletableFuture<Void>
            triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
                    final Collection<OperatorCoordinatorCheckpointContext> coordinators,
                    final PendingCheckpoint checkpoint,
                    final Executor acknowledgeExecutor)
                    throws CompletionException {

        try {
            return triggerAndAcknowledgeAllCoordinatorCheckpoints(
                    coordinators, checkpoint, acknowledgeExecutor);
        } catch (Exception e) {
            throw new CompletionException(e);
        }
    }

    // ------------------------------------------------------------------------

    /**
     *
     * @param checkpoint  本次进行中的检查点
     * @param snapshots  本次产生的所有 CoordinatorSnapshot 数据   每个context对应一个
     * @throws CheckpointException
     */
    private static void acknowledgeAllCoordinators(
            PendingCheckpoint checkpoint, Collection<CoordinatorSnapshot> snapshots)
            throws CheckpointException {
        for (final CoordinatorSnapshot snapshot : snapshots) {

            // 使用协调者信息来丰富 checkpoint内部的数据
            final PendingCheckpoint.TaskAcknowledgeResult result =
                    checkpoint.acknowledgeCoordinatorState(snapshot.coordinator, snapshot.state);

            // 表示重复添加 或者异常状态
            if (result != PendingCheckpoint.TaskAcknowledgeResult.SUCCESS) {
                final String errorMessage =
                        "Coordinator state not acknowledged successfully: " + result;
                final Throwable error =
                        checkpoint.isDisposed() ? checkpoint.getFailureCause() : null;

                CheckpointFailureReason reason = CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE;
                if (error != null) {
                    final Optional<IOException> ioExceptionOptional =
                            ExceptionUtils.findThrowable(error, IOException.class);
                    if (ioExceptionOptional.isPresent()) {
                        reason = CheckpointFailureReason.IO_EXCEPTION;
                    }

                    throw new CheckpointException(errorMessage, reason, error);
                } else {
                    throw new CheckpointException(errorMessage, reason);
                }
            }
        }
    }

    // ------------------------------------------------------------------------

    /**
     * 维护所有协调者的快照数据
     */
    static final class AllCoordinatorSnapshots {

        private final Collection<CoordinatorSnapshot> snapshots;

        AllCoordinatorSnapshots(Collection<CoordinatorSnapshot> snapshots) {
            this.snapshots = snapshots;
        }

        public Iterable<CoordinatorSnapshot> snapshots() {
            return snapshots;
        }
    }

    /**
     * 协调者快照
     */
    static final class CoordinatorSnapshot {

        /**
         * 某个算子信息  包含并行度  并行度跟子任务数量挂钩
         */
        final OperatorInfo coordinator;
        /**
         * 一些状态信息 通过该对象维护
         */
        final ByteStreamStateHandle state;

        CoordinatorSnapshot(OperatorInfo coordinator, ByteStreamStateHandle state) {
            this.coordinator = coordinator;
            this.state = state;
        }
    }
}
