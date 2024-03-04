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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Records the end of data event of each task, and allows for checking whether all tasks of a {@link
 * JobGraph} have reached the end of data.
 * 监听task是否处理完数据
 */
public class VertexEndOfDataListener {
    private final ExecutionGraph executionGraph;

    /**
     * 每个JobVertexID 对应一个task   位图对应subtask
     */
    private final Map<JobVertexID, BitSet> tasksReachedEndOfData;

    public VertexEndOfDataListener(ExecutionGraph executionGraph) {
        this.executionGraph = executionGraph;
        tasksReachedEndOfData = new HashMap<>();
        for (ExecutionJobVertex vertex : executionGraph.getAllVertices().values()) {
            tasksReachedEndOfData.put(vertex.getJobVertexId(), new BitSet());
        }
    }

    /**
     *
     * @param executionAttemptID  细化到subtask
     */
    public void recordTaskEndOfData(ExecutionAttemptID executionAttemptID) {
        BitSet subtaskStatus = tasksReachedEndOfData.get(executionAttemptID.getJobVertexId());
        subtaskStatus.set(executionAttemptID.getSubtaskIndex());
    }

    /**
     * 判断是否所有任务都完成
     * @return
     */
    public boolean areAllTasksEndOfData() {
        Iterator<Map.Entry<JobVertexID, BitSet>> iterator =
                tasksReachedEndOfData.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<JobVertexID, BitSet> entry = iterator.next();
            JobVertexID vertex = entry.getKey();
            BitSet status = entry.getValue();
            // 设置的位与并行度相同
            if (status.cardinality() != executionGraph.getJobVertex(vertex).getParallelism()) {
                return false;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * 将这些子任务重置成false
     * @param executionVertices
     */
    public void restoreVertices(Set<ExecutionVertexID> executionVertices) {
        for (ExecutionVertexID executionVertex : executionVertices) {
            JobVertexID jobVertexId = executionVertex.getJobVertexId();
            tasksReachedEndOfData.putIfAbsent(jobVertexId, new BitSet());
            tasksReachedEndOfData.get(jobVertexId).set(executionVertex.getSubtaskIndex(), false);
        }
    }
}
