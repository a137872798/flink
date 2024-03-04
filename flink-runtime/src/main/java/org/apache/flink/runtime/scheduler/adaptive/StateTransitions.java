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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An interface covering all possible {@link State} transitions. The main purpose is to align the
 * transition methods between different contexts.
 * 表示状态的转换
 */
public interface StateTransitions {

    /** Interface covering transition to the {@link Canceling} state.
     * 转换成取消状态
     * */
    interface ToCancelling extends StateTransitions {

        /**
         * Transitions into the {@link Canceling} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Canceling} state   对标一个job
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Canceling} state  可以执行一些函数
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Canceling} state   该对象可以管理多个协调者
         * @param failureCollection collection of failures that are propagated    记录一组错误信息
         */
        void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                List<ExceptionHistoryEntry> failureCollection);
    }

    /** Interface covering transition to the {@link CreatingExecutionGraph} state.
     * 可以转换成 创建执行图的状态
     * */
    interface ToCreatingExecutionGraph extends StateTransitions {

        /** Transitions into the {@link CreatingExecutionGraph} state.
         * 传入上个执行图
         * */
        void goToCreatingExecutionGraph(@Nullable ExecutionGraph previousExecutionGraph);
    }

    /** Interface covering transition to the {@link Executing} state.
     * 转换成执行状态
     * */
    interface ToExecuting extends StateTransitions {

        /**
         * Transitions into the {@link Executing} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Executing} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Executing} state  这个是跟检查点协调者互动的
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Executing} state   这个是算子协调者
         * @param failureCollection collection of failures that are propagated
         */
        void goToExecuting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                List<ExceptionHistoryEntry> failureCollection);
    }

    /** Interface covering transition to the {@link Finished} state. */
    interface ToFinished extends StateTransitions {

        /**
         * Transitions into the {@link Finished} state.
         *
         * @param archivedExecutionGraph archivedExecutionGraph which is passed to the {@link
         *     Finished} state   使用一个已经归档的执行图
         */
        void goToFinished(ArchivedExecutionGraph archivedExecutionGraph);
    }

    /** Interface covering transition to the {@link Failing} state. */
    interface ToFailing extends StateTransitions {

        /**
         * Transitions into the {@link Failing} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Failing} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Failing} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Failing} state
         * @param failureCause failureCause describing why the job execution failed
         * @param failureCollection collection of failures that are propagated
         */
        void goToFailing(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause,
                List<ExceptionHistoryEntry> failureCollection);
    }

    /** Interface covering transition to the {@link Restarting} state. */
    interface ToRestarting extends StateTransitions {

        /**
         * Transitions into the {@link Restarting} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Restarting} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Restarting}
         *     state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pas to the {@link
         *     Restarting} state
         * @param backoffTime backoffTime to wait before transitioning to the {@link Restarting}
         *     state      表示在重启前需要等待的时间
         * @param failureCollection collection of failures that are propagated
         */
        void goToRestarting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime,
                List<ExceptionHistoryEntry> failureCollection);
    }

    /** Interface covering transition to the {@link StopWithSavepoint} state.
     * 终止保存点生成
     * */
    interface ToStopWithSavepoint extends StateTransitions {

        /**
         * Transitions into the {@link StopWithSavepoint} state.
         *
         * @param executionGraph executionGraph to pass to the {@link StopWithSavepoint} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link
         *     StopWithSavepoint} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     StopWithSavepoint} state
         * @param checkpointScheduling   可以开始和终止检查点调度器
         * @param savepointFuture Future for the savepoint to complete.
         * @param failureCollection collection of failures that are propagated
         * @return Location of the savepoint.
         */
        CompletableFuture<String> goToStopWithSavepoint(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                CheckpointScheduling checkpointScheduling,
                CompletableFuture<String> savepointFuture,
                List<ExceptionHistoryEntry> failureCollection);
    }

    /** Interface covering transition to the {@link WaitingForResources} state. */
    interface ToWaitingForResources extends StateTransitions {

        /** Transitions into the {@link WaitingForResources} state.
         * 等待资源
         * */
        void goToWaitingForResources(@Nullable ExecutionGraph previousExecutionGraph);
    }
}
