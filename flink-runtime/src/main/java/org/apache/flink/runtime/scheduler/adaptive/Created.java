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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Initial state of the {@link AdaptiveScheduler}.
 * 表示刚被创建
 * */
class Created implements State {

    /**
     * 上下文决定了本对象可以转变的状态
     */
    private final Context context;

    private final Logger logger;

    Created(Context context, Logger logger) {
        this.context = context;
        this.logger = logger;
    }

    /**
     * 进入结束状态
     */
    @Override
    public void cancel() {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.CANCELED, null));
    }

    @Override
    public void suspend(Throwable cause) {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.SUSPENDED, cause));
    }

    /**
     * 因为是刚创建
     * @return
     */
    @Override
    public JobStatus getJobStatus() {
        return JobStatus.INITIALIZING;
    }

    /**
     * 基于当前状态获得归档图
     * @return
     */
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
        return logger;
    }

    /** Starts the scheduling by going into the {@link WaitingForResources} state.
     * 创建后就要开始调度了  然后就会等待资源
     * */
    void startScheduling() {
        context.goToWaitingForResources(null);
    }

    /** Context of the {@link Created} state.
     *
     * */
    interface Context extends StateTransitions.ToFinished, StateTransitions.ToWaitingForResources {

        /**
         * Creates an {@link ArchivedExecutionGraph} for the given jobStatus and failure cause.
         *
         * @param jobStatus jobStatus to create the {@link ArchivedExecutionGraph} with
         * @param cause cause represents the failure cause for the {@link ArchivedExecutionGraph};
         *     {@code null} if there is no failure cause
         * @return the created {@link ArchivedExecutionGraph}
         * 获取归档的执行图
         */
        ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause);
    }

    static class Factory implements StateFactory<Created> {

        private final Context context;
        private final Logger log;

        public Factory(Context context, Logger log) {
            this.context = context;
            this.log = log;
        }

        public Class<Created> getStateClass() {
            return Created.class;
        }

        public Created getState() {
            return new Created(this.context, this.log);
        }
    }
}
