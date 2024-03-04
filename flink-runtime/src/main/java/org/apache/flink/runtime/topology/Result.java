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

package org.apache.flink.runtime.topology;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

/**
 * Represents a data set produced by a {@link Vertex} Each result is produced by one {@link Vertex}.
 * Each result can be consumed by multiple {@link Vertex}.
 * 表示产生的一个结果
 */
public interface Result<
        VID extends VertexID,
        RID extends ResultID,
        V extends Vertex<VID, RID, V, R>,
        R extends Result<VID, RID, V, R>> {

    /**
     * 标识结果的id
     * @return
     */
    RID getId();

    /**
     * 获取结果的类型 或者说分区类型
     * @return
     */
    ResultPartitionType getResultType();

    /**
     * 获取产生该结果的数据
     * @return
     */
    V getProducer();
}
