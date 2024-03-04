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
import org.apache.flink.runtime.taskexecutor.ExecutionDeploymentReport;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default {@link ExecutionDeploymentReconciler} implementation. Detects missing/unknown
 * deployments, and defers to a provided {@link ExecutionDeploymentReconciliationHandler} to resolve
 * them.
 * 该对象是用来协调处理部署问题的
 */
public class DefaultExecutionDeploymentReconciler implements ExecutionDeploymentReconciler {

    /**
     * 借助handler  可以处理当部署到未知的地方  或者 部署的TaskExecutor不存在
     */
    private final ExecutionDeploymentReconciliationHandler handler;

    public DefaultExecutionDeploymentReconciler(ExecutionDeploymentReconciliationHandler handler) {
        this.handler = handler;
    }

    /**
     * 基于当前的报告数据来检测部署状态是否满足期望条件
     * @param taskExecutorHost hosting task executor   用于标记任务执行器的
     * @param executionDeploymentReport task executor report for deployed executions   报告中就是包含一组 ExecutionAttemptID
     * @param expectedDeployedExecutions  这是期望的部署状态
     */
    @Override
    public void reconcileExecutionDeployments(
            ResourceID taskExecutorHost,
            ExecutionDeploymentReport executionDeploymentReport,
            Map<ExecutionAttemptID, ExecutionDeploymentState> expectedDeployedExecutions) {
        final Set<ExecutionAttemptID> unknownExecutions =
                new HashSet<>(executionDeploymentReport.getExecutions());
        final Set<ExecutionAttemptID> missingExecutions = new HashSet<>();

        for (Map.Entry<ExecutionAttemptID, ExecutionDeploymentState> execution :
                expectedDeployedExecutions.entrySet()) {
            boolean deployed = unknownExecutions.remove(execution.getKey());
            // 表示找不到该execution
            if (!deployed && execution.getValue() != ExecutionDeploymentState.PENDING) {
                missingExecutions.add(execution.getKey());
            }
        }
        // 这是报告多出来的execution
        if (!unknownExecutions.isEmpty()) {
            handler.onUnknownDeploymentsOf(unknownExecutions, taskExecutorHost);
        }
        // 这是缺失的execution
        if (!missingExecutions.isEmpty()) {
            handler.onMissingDeploymentsOf(missingExecutions, taskExecutorHost);
        }
    }
}
