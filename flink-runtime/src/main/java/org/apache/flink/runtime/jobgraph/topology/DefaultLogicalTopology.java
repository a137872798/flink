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

package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.executiongraph.failover.flip1.LogicalPipelinedRegionComputeUtil;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link LogicalTopology}. It is an adapter of {@link JobGraph}.
 * 拓扑图可以获取流水线  以及通过顶点id 查询流水线
 * */
public class DefaultLogicalTopology implements LogicalTopology {

    /**
     * 涉及到的所有顶点
     */
    private final List<DefaultLogicalVertex> verticesSorted;

    /**
     * id -> 顶点
     */
    private final Map<JobVertexID, DefaultLogicalVertex> idToVertexMap;

    /**
     * 每个中间结果集 也就是结果
     */
    private final Map<IntermediateDataSetID, DefaultLogicalResult> idToResultMap;

    private DefaultLogicalTopology(final List<JobVertex> jobVertices) {
        checkNotNull(jobVertices);

        this.verticesSorted = new ArrayList<>(jobVertices.size());
        this.idToVertexMap = new HashMap<>();
        this.idToResultMap = new HashMap<>();

        buildVerticesAndResults(jobVertices);
    }

    /**
     * 通过一个job图进行初始化
     * @param jobGraph
     * @return
     */
    public static DefaultLogicalTopology fromJobGraph(final JobGraph jobGraph) {
        checkNotNull(jobGraph);

        return fromTopologicallySortedJobVertices(
                jobGraph.getVerticesSortedTopologicallyFromSources());
    }

    public static DefaultLogicalTopology fromTopologicallySortedJobVertices(
            final List<JobVertex> jobVertices) {
        return new DefaultLogicalTopology(jobVertices);
    }

    /**
     * 读取顶点信息 构建map数据
     * @param topologicallySortedJobVertices
     */
    private void buildVerticesAndResults(final Iterable<JobVertex> topologicallySortedJobVertices) {
        final Function<JobVertexID, DefaultLogicalVertex> vertexRetriever = this::getVertex;
        final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever =
                this::getResult;

        // 遍历包含在拓扑图中的每个顶点
        for (JobVertex jobVertex : topologicallySortedJobVertices) {

            // 这样每个顶点都可以用结果集id 查询结果集
            final DefaultLogicalVertex logicalVertex =
                    new DefaultLogicalVertex(jobVertex, resultRetriever);

            this.verticesSorted.add(logicalVertex);
            // 将顶点id与顶点插入到map
            this.idToVertexMap.put(logicalVertex.getId(), logicalVertex);

            // 遍历producer数据  这样resultRetriever就变成了查询producer数据了
            for (IntermediateDataSet intermediateDataSet : jobVertex.getProducedDataSets()) {
                final DefaultLogicalResult logicalResult =
                        new DefaultLogicalResult(intermediateDataSet, vertexRetriever);
                idToResultMap.put(logicalResult.getId(), logicalResult);
            }
        }
    }

    @Override
    public Iterable<DefaultLogicalVertex> getVertices() {
        return verticesSorted;
    }

    public DefaultLogicalVertex getVertex(final JobVertexID vertexId) {
        return Optional.ofNullable(idToVertexMap.get(vertexId))
                .orElseThrow(
                        () -> new IllegalArgumentException("can not find vertex: " + vertexId));
    }

    /**
     * 通过结果集id 查询result
     * @param resultId
     * @return
     */
    private DefaultLogicalResult getResult(final IntermediateDataSetID resultId) {
        return Optional.ofNullable(idToResultMap.get(resultId))
                .orElseThrow(
                        () -> new IllegalArgumentException("can not find result: " + resultId));
    }

    /**
     * 获取相关的所有流水线
     * @return
     */
    @Override
    public Iterable<DefaultLogicalPipelinedRegion> getAllPipelinedRegions() {
        final Set<Set<LogicalVertex>> regionsRaw =
                LogicalPipelinedRegionComputeUtil.computePipelinedRegions(verticesSorted);

        final Set<DefaultLogicalPipelinedRegion> regions = new HashSet<>();
        for (Set<LogicalVertex> regionVertices : regionsRaw) {
            // 将相同区域的包装成流水线
            regions.add(new DefaultLogicalPipelinedRegion(regionVertices));
        }
        return regions;
    }
}
