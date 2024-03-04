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

package org.apache.flink.runtime.jobgraph.forwardgroup;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A forward group is a set of job vertices connected via forward edges. Parallelisms of all job
 * vertices in the same {@link ForwardGroup} must be the same.
 *
 * 正向组  表示一组正向边连接的作业顶点
 * 在同一个forwardGroup的所有作业并行度必须相同
 */
public class ForwardGroup {

    /**
     * 表示这组顶点的 并行度
     */
    private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

    /**
     * 他们允许接收的最大并行度
     */
    private int maxParallelism = JobVertex.MAX_PARALLELISM_DEFAULT;

    /**
     * 存储job顶点
     */
    private final Set<JobVertexID> jobVertexIds = new HashSet<>();

    /**
     * 通过一组job顶点id进行初始化
     * @param jobVertices
     */
    public ForwardGroup(final Set<JobVertex> jobVertices) {
        checkNotNull(jobVertices);

        // 获取这些job的并行度
        Set<Integer> configuredParallelisms =
                jobVertices.stream()
                        .filter(
                                jobVertex -> {
                                    jobVertexIds.add(jobVertex.getID());
                                    return jobVertex.getParallelism() > 0;
                                })
                        .map(JobVertex::getParallelism)
                        .collect(Collectors.toSet());

        // 表示他们的并行度应当一致  否则set的size就会超过1
        checkState(configuredParallelisms.size() <= 1);
        if (configuredParallelisms.size() == 1) {
            this.parallelism = configuredParallelisms.iterator().next();
        }

        Set<Integer> configuredMaxParallelisms =
                jobVertices.stream()
                        .map(JobVertex::getMaxParallelism)
                        .filter(val -> val > 0)
                        .collect(Collectors.toSet());

        // 使用最小值 作为公共的最大并行度
        if (!configuredMaxParallelisms.isEmpty()) {
            this.maxParallelism = Collections.min(configuredMaxParallelisms);

            // 确保此时并行度 <= 最大并行度
            checkState(
                    parallelism == ExecutionConfig.PARALLELISM_DEFAULT
                            || maxParallelism >= parallelism,
                    "There is a job vertex in the forward group whose maximum parallelism is smaller than the group's parallelism");
        }
    }

    /**
     * 只有当前并行度未设置的情况下 才能调用该方法
     * @param parallelism
     */
    public void setParallelism(int parallelism) {
        checkState(this.parallelism == ExecutionConfig.PARALLELISM_DEFAULT);
        this.parallelism = parallelism;
    }

    public boolean isParallelismDecided() {
        return parallelism > 0;
    }

    public int getParallelism() {
        checkState(isParallelismDecided());
        return parallelism;
    }

    public boolean isMaxParallelismDecided() {
        return maxParallelism > 0;
    }

    public int getMaxParallelism() {
        checkState(isMaxParallelismDecided());
        return maxParallelism;
    }

    public int size() {
        return jobVertexIds.size();
    }

    public Set<JobVertexID> getJobVertexIds() {
        return jobVertexIds;
    }
}
