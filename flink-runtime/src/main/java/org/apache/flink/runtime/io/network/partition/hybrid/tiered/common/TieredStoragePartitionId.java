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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.common;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.Serializable;
import java.util.Objects;

/**
 * Identifier of a partition.
 *
 * <p>A partition is equivalent to a result partition in Flink.
 * 表示分区id
 */
public class TieredStoragePartitionId implements TieredStorageDataIdentifier, Serializable {

    private static final long serialVersionUID = 1L;

    private final ResultPartitionID partitionID;

    public TieredStoragePartitionId(ResultPartitionID partitionID) {
        this.partitionID = partitionID;
    }

    public ResultPartitionID getPartitionID() {
        return partitionID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TieredStoragePartitionId that = (TieredStoragePartitionId) o;
        return Objects.equals(partitionID, that.partitionID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionID);
    }

    @Override
    public String toString() {
        return "TieredStoragePartitionId{" + "ID=" + partitionID + '}';
    }
}
