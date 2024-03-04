/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** A bi-directional mapping between required and acquired resources. */
class BiDirectionalResourceToRequirementMapping {

    // 双向索引 维护需要的资源和提供的资源

    private final Map<ResourceProfile, ResourceCounter> requirementToFulfillingResources =
            new HashMap<>();

    private final Map<ResourceProfile, ResourceCounter> resourceToFulfilledRequirement =
            new HashMap<>();

    public void incrementCount(
            ResourceProfile requirement, ResourceProfile resource, int increment) {
        Preconditions.checkNotNull(requirement);
        Preconditions.checkNotNull(resource);
        Preconditions.checkArgument(increment > 0);
        internalIncrementCount(requirementToFulfillingResources, requirement, resource, increment);
        internalIncrementCount(resourceToFulfilledRequirement, resource, requirement, increment);
    }

    /**
     * 逆操作
     * @param requirement
     * @param resource
     * @param decrement
     */
    public void decrementCount(
            ResourceProfile requirement, ResourceProfile resource, int decrement) {
        Preconditions.checkNotNull(requirement);
        Preconditions.checkNotNull(resource);
        Preconditions.checkArgument(decrement > 0);
        internalDecrementCount(requirementToFulfillingResources, requirement, resource, decrement);
        internalDecrementCount(resourceToFulfilledRequirement, resource, requirement, decrement);
    }

    /**
     *
     * @param primaryMap
     * @param primaryKey   一个是需要的资源 一个是提供的资源
     * @param secondaryKey
     * @param increment
     */
    private static void internalIncrementCount(
            Map<ResourceProfile, ResourceCounter> primaryMap,
            ResourceProfile primaryKey,
            ResourceProfile secondaryKey,
            int increment) {
        primaryMap.compute(
                primaryKey,
                (resourceProfile, resourceCounter) -> {
                    // 可以看到 key/value 存的是不同的资源
                    if (resourceCounter == null) {
                        return ResourceCounter.withResource(secondaryKey, increment);
                    } else {
                        return resourceCounter.add(secondaryKey, increment);
                    }
                });
    }

    /**
     * 减少资源
     * @param primaryMap
     * @param primaryKey
     * @param secondaryKey
     * @param decrement
     */
    private static void internalDecrementCount(
            Map<ResourceProfile, ResourceCounter> primaryMap,
            ResourceProfile primaryKey,
            ResourceProfile secondaryKey,
            int decrement) {
        primaryMap.compute(
                primaryKey,
                (resourceProfile, resourceCounter) -> {
                    Preconditions.checkState(
                            resourceCounter != null,
                            "Attempting to decrement count of %s->%s, but primary key was unknown.",
                            resourceProfile,
                            secondaryKey);
                    final ResourceCounter newCounter =
                            resourceCounter.subtract(secondaryKey, decrement);
                    return newCounter.isEmpty() ? null : newCounter;
                });
    }

    public ResourceCounter getResourcesFulfilling(ResourceProfile requirement) {
        Preconditions.checkNotNull(requirement);
        return requirementToFulfillingResources.getOrDefault(requirement, ResourceCounter.empty());
    }

    public ResourceCounter getRequirementsFulfilledBy(ResourceProfile resource) {
        Preconditions.checkNotNull(resource);
        return resourceToFulfilledRequirement.getOrDefault(resource, ResourceCounter.empty());
    }

    public Set<ResourceProfile> getAllResourceProfiles() {
        return resourceToFulfilledRequirement.keySet();
    }

    public Set<ResourceProfile> getAllRequirementProfiles() {
        return requirementToFulfillingResources.keySet();
    }

    public int getNumFulfillingResources(ResourceProfile requirement) {
        Preconditions.checkNotNull(requirement);
        return requirementToFulfillingResources
                .getOrDefault(requirement, ResourceCounter.empty())
                .getTotalResourceCount();
    }

    public int getNumFulfilledRequirements(ResourceProfile resource) {
        Preconditions.checkNotNull(resource);
        return resourceToFulfilledRequirement
                .getOrDefault(resource, ResourceCounter.empty())
                .getTotalResourceCount();
    }

    @VisibleForTesting
    boolean isEmpty() {
        return requirementToFulfillingResources.isEmpty()
                && resourceToFulfilledRequirement.isEmpty();
    }
}
