/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor.slot;

import java.io.IOException;
import java.util.Collection;

/** Service for persisting {@link SlotAllocationSnapshot}.
 * 用于将slot的分配信息持久化
 * */
public interface SlotAllocationSnapshotPersistenceService {

    /**
     * Persist the given slot allocation snapshot.
     *
     * @param slotAllocationSnapshot slot allocation snapshot to persist
     * @throws IOException if the slot allocation snapshot cannot be persisted
     */
    void persistAllocationSnapshot(SlotAllocationSnapshot slotAllocationSnapshot)
            throws IOException;

    /**
     * Delete the slot allocation snapshot identified by the slot index.
     *
     * @param slotIndex identifying the slot allocation snapshot to delete
     *                  通过slot编号 可以找到快照并删除
     */
    void deleteAllocationSnapshot(int slotIndex);

    /**
     * Load all persisted slot allocation snapshots.
     *
     * @return loaded slot allocations snapshots
     * 加载所有持久化的slot分配快照
     */
    Collection<SlotAllocationSnapshot> loadAllocationSnapshots();
}
