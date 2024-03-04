/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

/**
 * A class to execute a {@link SnapshotStrategy}. It can execute a strategy either synchronously or
 * asynchronously. It takes care of common logging and resource cleaning.
 *
 * @param <T> type of the snapshot result.
 *           该对象用于为状态后端产生快照
 */
public final class SnapshotStrategyRunner<T extends StateObject, SR extends SnapshotResources> {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotStrategyRunner.class);

    private static final String LOG_SYNC_COMPLETED_TEMPLATE =
            "{} ({}, synchronous part) in thread {} took {} ms.";
    private static final String LOG_ASYNC_COMPLETED_TEMPLATE =
            "{} ({}, asynchronous part) in thread {} took {} ms.";

    /**
     * Descriptive name of the snapshot strategy that will appear in the log outputs and {@link
     * #toString()}.
     */
    @Nonnull private final String description;

    /**
     * 产生快照的逻辑包装在里面
     */
    @Nonnull private final SnapshotStrategy<T, SR> snapshotStrategy;
    @Nonnull private final CloseableRegistry cancelStreamRegistry;

    @Nonnull private final SnapshotExecutionType executionType;

    public SnapshotStrategyRunner(
            @Nonnull String description,
            @Nonnull SnapshotStrategy<T, SR> snapshotStrategy,
            @Nonnull CloseableRegistry cancelStreamRegistry,
            @Nonnull SnapshotExecutionType executionType) {
        this.description = description;
        this.snapshotStrategy = snapshotStrategy;
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.executionType = executionType;
    }

    /**
     * 调用该方法产生future
     * @param checkpointId
     * @param timestamp
     * @param streamFactory
     * @param checkpointOptions
     * @return
     * @throws Exception
     */
    @Nonnull
    public final RunnableFuture<SnapshotResult<T>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        long startTime = System.currentTimeMillis();

        // 生成快照数据  此时还在内存中
        SR snapshotResources = snapshotStrategy.syncPrepareResources(checkpointId);
        logCompletedInternal(LOG_SYNC_COMPLETED_TEMPLATE, streamFactory, startTime);

        // 执行该函数会触发快照的写入逻辑
        SnapshotStrategy.SnapshotResultSupplier<T> asyncSnapshot =
                snapshotStrategy.asyncSnapshot(
                        snapshotResources,
                        checkpointId,
                        timestamp,
                        streamFactory,
                        checkpointOptions);

        // 包装成异步任务
        FutureTask<SnapshotResult<T>> asyncSnapshotTask =
                new AsyncSnapshotCallable<SnapshotResult<T>>() {
                    @Override
                    protected SnapshotResult<T> callInternal() throws Exception {
                        return asyncSnapshot.get(snapshotCloseableRegistry);
                    }

                    /**
                     * 在完成快照后 删除内存数据
                     */
                    @Override
                    protected void cleanupProvidedResources() {
                        if (snapshotResources != null) {
                            snapshotResources.release();
                        }
                    }

                    @Override
                    protected void logAsyncSnapshotComplete(long startTime) {
                        logCompletedInternal(
                                LOG_ASYNC_COMPLETED_TEMPLATE, streamFactory, startTime);
                    }
                }.toAsyncSnapshotFutureTask(cancelStreamRegistry);

        // 同步模式下直接执行
        if (executionType == SnapshotExecutionType.SYNCHRONOUS) {
            asyncSnapshotTask.run();
        }

        return asyncSnapshotTask;
    }

    private void logCompletedInternal(
            @Nonnull String template, @Nonnull Object checkpointOutDescription, long startTime) {

        long duration = (System.currentTimeMillis() - startTime);

        LOG.debug(
                template, description, checkpointOutDescription, Thread.currentThread(), duration);
    }

    @Override
    public String toString() {
        return "SnapshotStrategy {" + description + "}";
    }
}
