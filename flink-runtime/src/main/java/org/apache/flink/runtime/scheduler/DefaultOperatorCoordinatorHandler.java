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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Default handler for the {@link OperatorCoordinator OperatorCoordinators}.
 * 该对象可以管理一组协调者
 * */
public class DefaultOperatorCoordinatorHandler implements OperatorCoordinatorHandler {

    /**
     * 对标一个job
     */
    private final ExecutionGraph executionGraph;

    /**
     * 维护协调者的容器
     */
    private final Map<OperatorID, OperatorCoordinatorHolder> coordinatorMap;

    private final GlobalFailureHandler globalFailureHandler;

    public DefaultOperatorCoordinatorHandler(
            ExecutionGraph executionGraph, GlobalFailureHandler globalFailureHandler) {
        this.executionGraph = executionGraph;

        this.coordinatorMap = createCoordinatorMap(executionGraph);
        this.globalFailureHandler = globalFailureHandler;
    }

    private static Map<OperatorID, OperatorCoordinatorHolder> createCoordinatorMap(
            ExecutionGraph executionGraph) {
        return executionGraph.getAllVertices().values().stream()
                .filter(ExecutionJobVertex::isInitialized)  // 找到已经初始化的顶点   顶点对标task
                .flatMap(v -> v.getOperatorCoordinators().stream())  // 每个协调者应该是对应每个子任务的
                .collect(
                        Collectors.toMap(
                                OperatorCoordinatorHolder::operatorId, Function.identity()));
    }

    @Override
    public void initializeOperatorCoordinators(ComponentMainThreadExecutor mainThreadExecutor) {
        for (OperatorCoordinatorHolder coordinatorHolder : coordinatorMap.values()) {
            // 执行延迟初始化
            coordinatorHolder.lazyInitialize(globalFailureHandler, mainThreadExecutor);
        }
    }

    @Override
    public void startAllOperatorCoordinators() {
        startOperatorCoordinators(coordinatorMap.values());
    }

    @Override
    public void disposeAllOperatorCoordinators() {
        coordinatorMap.values().forEach(IOUtils::closeQuietly);
    }

    /**
     * 将某个事件推送给某个协调者
     * @param taskExecutionId Execution attempt id of the originating task.
     * @param operatorId OperatorId of the target OperatorCoordinator.
     * @param evt
     * @throws FlinkException
     */
    @Override
    public void deliverOperatorEventToCoordinator(
            final ExecutionAttemptID taskExecutionId,
            final OperatorID operatorId,
            final OperatorEvent evt)
            throws FlinkException {

        // Failure semantics (as per the javadocs of the method):
        // If the task manager sends an event for a non-running task or an non-existing operator
        // coordinator, then respond with an exception to the call. If task and coordinator exist,
        // then we assume that the call from the TaskManager was valid, and any bubbling exception
        // needs to cause a job failure.

        // 找到执行对象 需要确保状态正确
        final Execution exec = executionGraph.getRegisteredExecutions().get(taskExecutionId);
        if (exec == null
                || exec.getState() != ExecutionState.RUNNING
                        && exec.getState() != ExecutionState.INITIALIZING) {
            // This situation is common when cancellation happens, or when the task failed while the
            // event was just being dispatched asynchronously on the TM side.
            // It should be fine in those expected situations to just ignore this event, but, to be
            // on the safe, we notify the TM that the event could not be delivered.
            throw new TaskNotRunningException(
                    "Task is not known or in state running on the JobManager.");
        }

        final OperatorCoordinatorHolder coordinator = coordinatorMap.get(operatorId);
        if (coordinator == null) {
            throw new FlinkException("No coordinator registered for operator " + operatorId);
        }

        try {
            // 操作者推送事件
            coordinator.handleEventFromOperator(
                    exec.getParallelSubtaskIndex(), exec.getAttemptNumber(), evt);
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            globalFailureHandler.handleGlobalFailure(t);
        }
    }

    /**
     * 转发请求
     * @param operator Id of target operator.
     * @param request request for the operator.
     * @return
     * @throws FlinkException
     */
    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {

        final OperatorCoordinatorHolder coordinatorHolder = coordinatorMap.get(operator);
        if (coordinatorHolder == null) {
            throw new FlinkException(
                    "Coordinator of operator "
                            + operator
                            + " does not exist or the job vertex this operator belongs to is not initialized.");
        }

        final OperatorCoordinator coordinator = coordinatorHolder.coordinator();
        // 只有该类型才能处理req
        if (coordinator instanceof CoordinationRequestHandler) {
            return ((CoordinationRequestHandler) coordinator).handleCoordinationRequest(request);
        } else {
            throw new FlinkException(
                    "Coordinator of operator " + operator + " cannot handle client event");
        }
    }

    /**
     * 添加一个新的协调者
     * @param coordinators the operator coordinator to be registered.
     * @param mainThreadExecutor Executor for submitting work to the main thread.
     */
    @Override
    public void registerAndStartNewCoordinators(
            Collection<OperatorCoordinatorHolder> coordinators,
            ComponentMainThreadExecutor mainThreadExecutor) {

        for (OperatorCoordinatorHolder coordinator : coordinators) {
            coordinatorMap.put(coordinator.operatorId(), coordinator);
            // 添加的同时延迟初始化
            coordinator.lazyInitialize(globalFailureHandler, mainThreadExecutor);
        }
        startOperatorCoordinators(coordinators);
    }

    /**
     * 启动协调者
     * @param coordinators
     */
    private void startOperatorCoordinators(Collection<OperatorCoordinatorHolder> coordinators) {
        try {
            for (OperatorCoordinatorHolder coordinator : coordinators) {
                coordinator.start();
            }
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            coordinators.forEach(IOUtils::closeQuietly);
            throw new FlinkRuntimeException("Failed to start the operator coordinators", t);
        }
    }

    @VisibleForTesting
    Map<OperatorID, OperatorCoordinatorHolder> getCoordinatorMap() {
        return coordinatorMap;
    }
}
