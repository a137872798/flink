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

package org.apache.flink.runtime.io.network.partition;

import java.io.IOException;

/** Interface for creating result partitions. */
public interface ResultPartitionProvider {

    /** Returns the requested intermediate result partition input view.
     * 执行分区 + subIndex  就可以定位到一个子分区
     * @param partitionId
     * @param index
     * @param availabilityListener
     * */
    ResultSubpartitionView createSubpartitionView(
            ResultPartitionID partitionId,
            int index,
            BufferAvailabilityListener availabilityListener)
            throws IOException;
}
