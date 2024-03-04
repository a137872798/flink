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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

/** Strategy how to find a matching slot.
 * 通过策略寻找匹配的slot
 * */
public interface SlotMatchingStrategy {

    /**
     * Finds a matching slot for the requested {@link ResourceProfile} given the collection of free
     * slots and the total number of slots per TaskExecutor.
     *
     * @param requestedProfile to find a matching slot for     表示资源消耗  需要找到匹配的slot
     * @param freeSlots collection of free slots   当前可用的slot
     * @param numberRegisteredSlotsLookup lookup for the number of registered slots   通过id查询编号
     * @return Returns a matching slots or {@link Optional#empty()} if there is none
     */
    <T extends TaskManagerSlotInformation> Optional<T> findMatchingSlot(
            ResourceProfile requestedProfile,
            Collection<T> freeSlots,
            Function<InstanceID, Integer> numberRegisteredSlotsLookup);
}
