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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.JobVertexResourceRequirements;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/** Maintains the configured parallelisms for vertices, which should be defined by a scheduler.
 * 该对象用于维护顶点的并行度
 * */
public class DefaultVertexParallelismStore implements MutableVertexParallelismStore {

    private static final Function<Integer, Optional<String>> RESCALE_MAX_REJECT =
            maxParallelism -> Optional.of("Cannot change the max parallelism.");

    /**
     * Create a new {@link VertexParallelismStore} that reflects given {@link
     * JobResourceRequirements}.
     *
     * @param oldVertexParallelismStore old vertex parallelism store that serves as a base for the
     *     new one    对应本对象 DefaultVertexParallelismStore
     * @param jobResourceRequirements to apply over the old vertex parallelism store     该对象描述整个job的资源需求
     * @return new vertex parallelism store iff it was updated
     */
    public static Optional<VertexParallelismStore> applyJobResourceRequirements(
            VertexParallelismStore oldVertexParallelismStore,
            JobResourceRequirements jobResourceRequirements) {

        // 产生一个新对象
        final DefaultVertexParallelismStore newVertexParallelismStore =
                new DefaultVertexParallelismStore();
        boolean changed = false;
        for (final JobVertexID jobVertexId : jobResourceRequirements.getJobVertices()) {

            // 按照 jobResourceRequirements 获取某顶点的并行度
            final VertexParallelismInformation oldVertexParallelismInfo =
                    oldVertexParallelismStore.getParallelismInfo(jobVertexId);

            // 获取他需要的资源
            final JobVertexResourceRequirements.Parallelism parallelismSettings =
                    jobResourceRequirements.getParallelism(jobVertexId);
            final int minParallelism = parallelismSettings.getLowerBound();
            final int parallelism = parallelismSettings.getUpperBound();
            newVertexParallelismStore.setParallelismInfo(
                    jobVertexId,
                    new DefaultVertexParallelismInfo(
                            minParallelism,
                            parallelism,
                            oldVertexParallelismInfo.getMaxParallelism(),  // 只保留了最大并行度  当前并行度和最小并行度都更新了
                            RESCALE_MAX_REJECT));
            changed |=
                    oldVertexParallelismInfo.getMinParallelism() != minParallelism
                            || oldVertexParallelismInfo.getParallelism() != parallelism;
        }
        return changed ? Optional.of(newVertexParallelismStore) : Optional.empty();
    }

    /**
     * 通过该容器维护信息
     */
    private final Map<JobVertexID, VertexParallelismInformation> vertexToParallelismInfo =
            new HashMap<>();

    /**
     * 将顶点并行度 写入容器
     * @param vertexId vertex to set parallelism for
     * @param info parallelism information for the given vertex
     */
    @Override
    public void setParallelismInfo(JobVertexID vertexId, VertexParallelismInformation info) {
        vertexToParallelismInfo.put(vertexId, info);
    }

    /**
     * 查询
     * @param vertexId vertex for which the parallelism information should be returned
     * @return
     */
    @Override
    public VertexParallelismInformation getParallelismInfo(JobVertexID vertexId) {
        return Optional.ofNullable(vertexToParallelismInfo.get(vertexId))
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        String.format(
                                                "No parallelism information set for vertex %s",
                                                vertexId)));
    }
}
