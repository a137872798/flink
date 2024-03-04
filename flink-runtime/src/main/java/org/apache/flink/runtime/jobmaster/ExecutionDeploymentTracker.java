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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.Map;

/** A tracker for deployed executions.
 * 该对象可以追踪execution的部署情况
 * */
public interface ExecutionDeploymentTracker {

    /**
     * Starts tracking the given execution that is being deployed on the given host.
     *
     * @param executionAttemptId execution to start tracking
     * @param host hosting task executor   代表被部署的目标 TaskExecutor
     *             表示开始追踪某个execution的部署情况
     */
    void startTrackingPendingDeploymentOf(ExecutionAttemptID executionAttemptId, ResourceID host);

    /**
     * Marks the deployment of the given execution as complete.
     *
     * @param executionAttemptId execution whose deployment to mark as complete
     *                           表示某个execution已经部署完了
     */
    void completeDeploymentOf(ExecutionAttemptID executionAttemptId);

    /**
     * Stops tracking the given execution.
     *
     * @param executionAttemptId execution to stop tracking
     *                           停止追踪
     */
    void stopTrackingDeploymentOf(ExecutionAttemptID executionAttemptId);

    /**
     * Returns all tracked executions for the given host.
     *
     * @param host hosting task executor
     * @return tracked executions
     * 获取在某个 taskExecutor上部署的所有execution
     */
    Map<ExecutionAttemptID, ExecutionDeploymentState> getExecutionsOn(ResourceID host);
}
