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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

/** Describe the different data sources in {@link TieredStorageConsumerClient}.
 * 有关消费目标的信息
 * */
public class TieredStorageConsumerSpec {

    private final TieredStoragePartitionId tieredStoragePartitionId;

    private final TieredStorageSubpartitionId tieredStorageSubpartitionId;

    public TieredStorageConsumerSpec(
            TieredStoragePartitionId tieredStoragePartitionId,
            TieredStorageSubpartitionId tieredStorageSubpartitionId) {
        this.tieredStoragePartitionId = tieredStoragePartitionId;
        this.tieredStorageSubpartitionId = tieredStorageSubpartitionId;
    }

    public TieredStoragePartitionId getPartitionId() {
        return tieredStoragePartitionId;
    }

    public TieredStorageSubpartitionId getSubpartitionId() {
        return tieredStorageSubpartitionId;
    }
}
