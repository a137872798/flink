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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Default {@link ExecutionDeploymentTracker} implementation.
 * 该对象会跟踪execution的部署情况
 * */
public class DefaultExecutionDeploymentTracker implements ExecutionDeploymentTracker {

    /**
     * 所有待部署的execution
     */
    private final Set<ExecutionAttemptID> pendingDeployments = new HashSet<>();
    /**
     * 记录每个 execution 部署在哪个 taskExecutor上   看来taskExecutor是用来执行execution的
     */
    private final Map<ResourceID, Set<ExecutionAttemptID>> executionsByHost = new HashMap<>();
    /**
     * 这是反向索引
     */
    private final Map<ExecutionAttemptID, ResourceID> hostByExecution = new HashMap<>();

    /**
     * 开始追踪
     * @param executionAttemptId execution to start tracking
     * @param host hosting task executor   代表被部署的目标 TaskExecutor
     */
    @Override
    public void startTrackingPendingDeploymentOf(
            ExecutionAttemptID executionAttemptId, ResourceID host) {
        // 在这3个容器中记录关联关系
        pendingDeployments.add(executionAttemptId);
        hostByExecution.put(executionAttemptId, host);
        executionsByHost.computeIfAbsent(host, ignored -> new HashSet<>()).add(executionAttemptId);
    }

    @Override
    public void completeDeploymentOf(ExecutionAttemptID executionAttemptId) {
        // 当某个execution部署完毕时 将其从pending容器移除
        pendingDeployments.remove(executionAttemptId);
    }

    /**
     * 停止追踪 就是从相关容器中移除该execution
     * @param executionAttemptId execution to stop tracking
     */
    @Override
    public void stopTrackingDeploymentOf(ExecutionAttemptID executionAttemptId) {
        pendingDeployments.remove(executionAttemptId);
        ResourceID host = hostByExecution.remove(executionAttemptId);
        if (host != null) {
            executionsByHost.computeIfPresent(
                    host,
                    (resourceID, executionAttemptIds) -> {
                        executionAttemptIds.remove(executionAttemptId);

                        return executionAttemptIds.isEmpty() ? null : executionAttemptIds;
                    });
        }
    }

    /**
     * 查看某个 taskExecutor 相关的所有execution的部署情况
     * @param host hosting task executor
     * @return
     */
    @Override
    public Map<ExecutionAttemptID, ExecutionDeploymentState> getExecutionsOn(ResourceID host) {
        return executionsByHost.getOrDefault(host, Collections.emptySet()).stream()
                .collect(
                        Collectors.toMap(
                                x -> x,
                                x ->
                                        pendingDeployments.contains(x)
                                                ? ExecutionDeploymentState.PENDING
                                                : ExecutionDeploymentState.DEPLOYED));
    }
}
