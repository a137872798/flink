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

import org.apache.flink.runtime.executiongraph.VertexGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/** Common utils for computing forward groups.
 * 提供一些有关正向组的公共方法
 * */
public class ForwardGroupComputeUtil {

    public static Map<JobVertexID, ForwardGroup> computeForwardGroupsAndCheckParallelism(
            final Iterable<JobVertex> topologicallySortedVertices) {
        final Map<JobVertexID, ForwardGroup> forwardGroupsByJobVertexId =
                computeForwardGroups(
                        topologicallySortedVertices, ForwardGroupComputeUtil::getForwardProducers);
        // the vertex's parallelism in parallelism-decided forward group should have been set at
        // compilation phase
        topologicallySortedVertices.forEach(
                jobVertex -> {
                    ForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertex.getID());
                    if (forwardGroup != null && forwardGroup.isParallelismDecided()) {
                        checkState(jobVertex.getParallelism() == forwardGroup.getParallelism());
                    }
                });
        return forwardGroupsByJobVertexId;
    }

    /**
     * 简单来讲就是将有关联的顶点纳入同一个顶点组  并将顶点组包装成正向组
     * @param topologicallySortedVertices   本次参与计算的所有顶点
     * @param forwardProducersRetriever
     * @return
     */
    public static Map<JobVertexID, ForwardGroup> computeForwardGroups(
            final Iterable<JobVertex> topologicallySortedVertices,
            final Function<JobVertex, Set<JobVertex>> forwardProducersRetriever) {

        final Map<JobVertex, Set<JobVertex>> vertexToGroup = new IdentityHashMap<>();

        // iterate all the vertices which are topologically sorted
        // 先遍历所有需要考虑的顶点
        for (JobVertex vertex : topologicallySortedVertices) {

            // 为每个顶点都分配一个set
            Set<JobVertex> currentGroup = new HashSet<>();
            currentGroup.add(vertex);

            // 在外层将该顶点与相关的set关联起来
            vertexToGroup.put(vertex, currentGroup);

            // 该顶点借由函数处理后  会衍生出一些新的顶点
            for (JobVertex producerVertex : forwardProducersRetriever.apply(vertex)) {

                // 检测该顶点是否已经存在于map中
                final Set<JobVertex> producerGroup = vertexToGroup.get(producerVertex);

                // 这时异常情况  也就是每个顶点衍生出的其他顶点 应当先于本顶点被加入  感觉像是一种检测机制
                if (producerGroup == null) {
                    throw new IllegalStateException(
                            "Producer task "
                                    + producerVertex.getID()
                                    + " forward group is null"
                                    + " while calculating forward group for the consumer task "
                                    + vertex.getID()
                                    + ". This should be a forward group building bug.");
                }

                // 当该顶点与衍生出的顶点group 不同时
                if (currentGroup != producerGroup) {
                    // 使得小组中每个顶点指向大组
                    currentGroup =
                            VertexGroupComputeUtil.mergeVertexGroups(
                                    currentGroup, producerGroup, vertexToGroup);
                }
            }
        }

        final Map<JobVertexID, ForwardGroup> ret = new HashMap<>();

        // 上面顶点有关联 (通过function可以衍生) 的最终引向的都是同一个顶点组
        // 在通过 uniqueVertexGroups 处理后 仅得到无关的顶点组信息
        for (Set<JobVertex> vertexGroup :
                VertexGroupComputeUtil.uniqueVertexGroups(vertexToGroup)) {

            // 只有到不同的顶点组 数量超过1时  才会将数据插入ret
            if (vertexGroup.size() > 1) {
                // 为每个顶点组包装成正向组   相同的顶点组的并行度是一致的
                ForwardGroup forwardGroup = new ForwardGroup(vertexGroup);

                // 将每个顶点与顶点组的关联关系存储到map中
                for (JobVertexID jobVertexId : forwardGroup.getJobVertexIds()) {
                    ret.put(jobVertexId, forwardGroup);
                }
            }
        }
        return ret;
    }

    static Set<JobVertex> getForwardProducers(final JobVertex jobVertex) {
        return jobVertex.getInputs().stream()
                .filter(JobEdge::isForward)
                .map(JobEdge::getSource)
                .map(IntermediateDataSet::getProducer)
                .collect(Collectors.toSet());
    }
}
