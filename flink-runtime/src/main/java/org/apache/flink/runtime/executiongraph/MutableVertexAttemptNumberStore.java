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

import org.apache.flink.runtime.jobgraph.JobVertexID;

/** Mutability extension to the {@link VertexAttemptNumberStore}.
 * VertexAttemptNumberStore是查看 而该对象赋予写入能力
 * */
public interface MutableVertexAttemptNumberStore extends VertexAttemptNumberStore {
    /**
     * Sets the attempt count for the given subtask of the given vertex.
     *
     * @param jobVertexId vertex the subtask belongs to
     * @param subtaskIndex subtask to set the attempt number for
     * @param attemptNumber attempt number to set
     */
    void setAttemptCount(JobVertexID jobVertexId, int subtaskIndex, int attemptNumber);
}
