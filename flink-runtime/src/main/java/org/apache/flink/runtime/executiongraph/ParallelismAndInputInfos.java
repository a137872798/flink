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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The parallelism and {@link JobVertexInputInfo}s of a job vertex .
 * 描述并行度和输入信息
 * */
public class ParallelismAndInputInfos {

    private final int parallelism;

    /**
     * 通过结果集id 可以对应到多个输入信息  每个输入表示某个子任务 以及相关的分区，子分区数据范围
     */
    private final Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos;

    public ParallelismAndInputInfos(
            int parallelism, Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos) {
        checkArgument(parallelism > 0);
        jobVertexInputInfos
                .values()
                .forEach(
                        jobVertexInputInfo ->
                                checkArgument(
                                        // 要求input数量和并行度是一样的 (input数量与内部子任务数量是一样的 所以合理)
                                        jobVertexInputInfo.getExecutionVertexInputInfos().size()
                                                == parallelism));
        this.parallelism = parallelism;
        this.jobVertexInputInfos = checkNotNull(jobVertexInputInfos);
    }

    public int getParallelism() {
        return parallelism;
    }

    public Map<IntermediateDataSetID, JobVertexInputInfo> getJobVertexInputInfos() {
        return Collections.unmodifiableMap(jobVertexInputInfos);
    }
}
