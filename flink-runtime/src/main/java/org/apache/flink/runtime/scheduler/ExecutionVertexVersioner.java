/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Records modifications of {@link org.apache.flink.runtime.executiongraph.ExecutionVertex
 * ExecutionVertices}, and allows for checking whether a vertex was modified.
 *
 * <p>Examples for modifications include:
 *
 * <ul>
 *   <li>cancellation of the underlying execution
 *   <li>deployment of the execution vertex
 * </ul>
 *
 * @see DefaultScheduler
 */
public class ExecutionVertexVersioner {

    /**
     * 记录每个子任务的版本
     */
    private final Map<ExecutionVertexID, Long> executionVertexToVersion = new HashMap<>();

    /**
     * 增加版本号
     * @param executionVertexId
     * @return
     */
    public ExecutionVertexVersion recordModification(final ExecutionVertexID executionVertexId) {
        final Long newVersion = executionVertexToVersion.merge(executionVertexId, 1L, Long::sum);
        return new ExecutionVertexVersion(executionVertexId, newVersion);
    }

    /**
     * 更新一组子任务的版本
     * @param vertices
     * @return
     */
    public Map<ExecutionVertexID, ExecutionVertexVersion> recordVertexModifications(
            final Collection<ExecutionVertexID> vertices) {
        return vertices.stream()
                .map(this::recordModification)
                .collect(
                        Collectors.toMap(
                                ExecutionVertexVersion::getExecutionVertexId, Function.identity()));
    }

    /**
     * 比较版本号是否有变化
     * @param executionVertexVersion
     * @return
     */
    public boolean isModified(final ExecutionVertexVersion executionVertexVersion) {
        final Long currentVersion =
                getCurrentVersion(executionVertexVersion.getExecutionVertexId());
        return currentVersion != executionVertexVersion.getVersion();
    }

    private Long getCurrentVersion(ExecutionVertexID executionVertexId) {
        final Long currentVersion = executionVertexToVersion.get(executionVertexId);
        Preconditions.checkState(
                currentVersion != null,
                "Execution vertex %s does not have a recorded version",
                executionVertexId);
        return currentVersion;
    }

    /**
     * 返回未变化的 ExecutionVertexVersion
     * @param executionVertexVersions
     * @return
     */
    public Set<ExecutionVertexID> getUnmodifiedExecutionVertices(
            final Set<ExecutionVertexVersion> executionVertexVersions) {
        return executionVertexVersions.stream()
                .filter(executionVertexVersion -> !isModified(executionVertexVersion))
                .map(ExecutionVertexVersion::getExecutionVertexId)
                .collect(Collectors.toSet());
    }

    /**
     * 通过一组id查询相关的版本号
     * @param executionVertexIds
     * @return
     */
    public Map<ExecutionVertexID, ExecutionVertexVersion> getExecutionVertexVersions(
            Collection<ExecutionVertexID> executionVertexIds) {
        return executionVertexIds.stream()
                .map(id -> new ExecutionVertexVersion(id, getCurrentVersion(id)))
                .collect(
                        Collectors.toMap(
                                ExecutionVertexVersion::getExecutionVertexId, Function.identity()));
    }

    ExecutionVertexVersion getExecutionVertexVersion(ExecutionVertexID executionVertexId) {
        final long currentVersion = getCurrentVersion(executionVertexId);
        return new ExecutionVertexVersion(executionVertexId, currentVersion);
    }
}
