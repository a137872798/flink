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
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link SlotMatchingStrategy} which picks a matching slot from a TaskExecutor with the least
 * utilization.
 * 优先选择使用率最小的slot
 */
public enum LeastUtilizationSlotMatchingStrategy implements SlotMatchingStrategy {
    INSTANCE;

    @Override
    public <T extends TaskManagerSlotInformation> Optional<T> findMatchingSlot(
            ResourceProfile requestedProfile,
            Collection<T> freeSlots,
            Function<InstanceID, Integer> numberRegisteredSlotsLookup) {

        // 按照 TaskExecutor 实例来分组
        final Map<InstanceID, Integer> numSlotsPerTaskExecutor =
                freeSlots.stream()
                        .collect(
                                Collectors.groupingBy(
                                        TaskManagerSlotInformation::getInstanceId,
                                        Collectors.reducing(0, i -> 1, Integer::sum)));

        return freeSlots.stream()
                // 首先资源要匹配
                .filter(taskManagerSlot -> taskManagerSlot.isMatchingRequirement(requestedProfile))
                .min(
                        // 优先返回利用率低的实例相关的slot
                        Comparator.comparingDouble(
                                taskManagerSlot ->
                                        calculateUtilization(
                                                taskManagerSlot.getInstanceId(),
                                                numberRegisteredSlotsLookup,
                                                numSlotsPerTaskExecutor)));
    }

    /**
     * 计算实例上slot利用率
     * @param instanceId   当前实例id
     * @param numberRegisteredSlotsLookup
     * @param numSlotsPerTaskExecutor
     * @return
     */
    private static double calculateUtilization(
            InstanceID instanceId,
            Function<? super InstanceID, Integer> numberRegisteredSlotsLookup,
            Map<InstanceID, Integer> numSlotsPerTaskExecutor) {
        // 找到该实例上注册的slot数量
        final int numberRegisteredSlots = numberRegisteredSlotsLookup.apply(instanceId);

        Preconditions.checkArgument(
                numberRegisteredSlots > 0,
                "The TaskExecutor %s has no slots registered.",
                instanceId);

        final int numberFreeSlots = numSlotsPerTaskExecutor.getOrDefault(instanceId, 0);

        Preconditions.checkArgument(
                numberRegisteredSlots >= numberFreeSlots,
                "The TaskExecutor %s has fewer registered slots than free slots.",
                instanceId);

        // 剩的越多 free的越多 利用率越低
        return (double) (numberRegisteredSlots - numberFreeSlots) / numberRegisteredSlots;
    }
}
