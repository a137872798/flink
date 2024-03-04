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

import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link LogicalVertex}. It is an adapter of {@link JobVertex}.
 * 表示一个定点
 * */
public class DefaultLogicalVertex implements LogicalVertex {

    /**
     * 关联的顶点对象
     */
    private final JobVertex jobVertex;

    /**
     * 查找函数 实际上是从DefaultLogicalTopology 查询数据
     */
    private final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever;

    private final List<LogicalEdge> inputEdges;


    /**
     *
     * @param jobVertex  某个顶点对象
     * @param resultRetriever  通过中间结果集id 查询结果对象
     */
    DefaultLogicalVertex(
            final JobVertex jobVertex,
            final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever) {

        this.jobVertex = checkNotNull(jobVertex);
        this.resultRetriever = checkNotNull(resultRetriever);

        // 初始化时 根据Job顶点的input 产生edge
        this.inputEdges =
                jobVertex.getInputs().stream()
                        .map(DefaultLogicalEdge::new)
                        .collect(Collectors.toList());
    }

    @Override
    public JobVertexID getId() {
        return jobVertex.getID();
    }

    /**
     * 表示本顶点消费的数据
     * @return
     */
    @Override
    public Iterable<DefaultLogicalResult> getConsumedResults() {
        // input相当于是该顶点的上游  这样就是获取上游的中间数据集  应该是表示这些数据集已经被消耗了
        return jobVertex.getInputs().stream()
                .map(JobEdge::getSource)
                .map(IntermediateDataSet::getId)
                .map(resultRetriever)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<DefaultLogicalResult> getProducedResults() {
        // 表示由本顶点产生的待消费的中间结果集  (已产生 未消费)
        return jobVertex.getProducedDataSets().stream()
                .map(IntermediateDataSet::getId)
                .map(resultRetriever)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<? extends LogicalEdge> getInputs() {
        return inputEdges;
    }
}
