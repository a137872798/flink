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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.DefaultRequirementMatcher;
import org.apache.flink.runtime.slots.RequirementMatcher;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Tracks resource for a single job.
 * 追踪单个job的资源
 * */
class JobScopedResourceTracker {

    private static final Logger LOG = LoggerFactory.getLogger(JobScopedResourceTracker.class);

    // only for logging purposes
    private final JobID jobId;

    /**
     * 该对象维护了分配的资源和需要的资源
     */
    private final BiDirectionalResourceToRequirementMapping resourceToRequirementMapping =
            new BiDirectionalResourceToRequirementMapping();

    /**
     * 匹配资源 返回对象就表示还需要分配
     */
    private final RequirementMatcher requirementMatcher = new DefaultRequirementMatcher();

    /**
     * 表示资源的需求量
     */
    private ResourceCounter resourceRequirements = ResourceCounter.empty();

    /**
     * 表示多余的资源
     */
    private ResourceCounter excessResources = ResourceCounter.empty();

    JobScopedResourceTracker(JobID jobId) {
        this.jobId = Preconditions.checkNotNull(jobId);
    }

    /**
     * 告知资源的需求量
     * @param newResourceRequirements
     */
    public void notifyResourceRequirements(
            Collection<ResourceRequirement> newResourceRequirements) {
        Preconditions.checkNotNull(newResourceRequirements);

        resourceRequirements = ResourceCounter.empty();
        for (ResourceRequirement newResourceRequirement : newResourceRequirements) {
            resourceRequirements =
                    resourceRequirements.add(
                            newResourceRequirement.getResourceProfile(),
                            newResourceRequirement.getNumberOfRequiredSlots());
        }
        findExcessSlots();
        tryAssigningExcessSlots();
    }

    /**
     * 记录收到的资源
     * @param resourceProfile
     */
    public void notifyAcquiredResource(ResourceProfile resourceProfile) {
        Preconditions.checkNotNull(resourceProfile);
        final Optional<ResourceProfile> matchingRequirement =
                findMatchingRequirement(resourceProfile);

        // 表示又分配了一个资源
        if (matchingRequirement.isPresent()) {
            resourceToRequirementMapping.incrementCount(
                    matchingRequirement.get(), resourceProfile, 1);
        } else {
            // 此时已经满了 就加入到过量容器中
            LOG.debug("Job {} acquired excess resource {}.", resourceProfile, jobId);
            excessResources = excessResources.add(resourceProfile, 1);
        }
    }

    private Optional<ResourceProfile> findMatchingRequirement(ResourceProfile resourceProfile) {
        return requirementMatcher.match(
                resourceProfile,
                resourceRequirements,
                resourceToRequirementMapping::getNumFulfillingResources);
    }

    /**
     * 通知本对象失去资源
     * @param resourceProfile
     */
    public void notifyLostResource(ResourceProfile resourceProfile) {
        Preconditions.checkNotNull(resourceProfile);
        // 如果超量 先从超出的减
        if (excessResources.getResourceCount(resourceProfile) > 0) {
            LOG.trace("Job {} lost excess resource {}.", jobId, resourceProfile);
            excessResources = excessResources.subtract(resourceProfile, 1);
            return;
        }

        // 获取针对该资源描述 提供的资源
        Set<ResourceProfile> fulfilledRequirements =
                resourceToRequirementMapping
                        .getRequirementsFulfilledBy(resourceProfile)
                        .getResources();

        if (!fulfilledRequirements.isEmpty()) {
            // deduct the resource from any requirement
            // from a correctness standpoint the choice is arbitrary
            // from a resource utilization standpoint it could make sense to search for the
            // requirement with the largest
            // distance to the resource profile (i.e., the smallest requirement), but it may not
            // matter since we are
            // likely to get back a similarly-sized resource later on
            ResourceProfile assignedRequirement = fulfilledRequirements.iterator().next();

            // 这里会同时清理双向索引  并且只清理一个
            resourceToRequirementMapping.decrementCount(assignedRequirement, resourceProfile, 1);

            // 某个资源的需求量少了 就会有多出的资源  就可以尝试调配给别的需求
            tryAssigningExcessSlots();
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Job %s lost a resource %s but no such resource was tracked.",
                            jobId, resourceProfile));
        }
    }

    /**
     * 返回缺失的资源
     * @return
     */
    public Collection<ResourceRequirement> getMissingResources() {
        final Collection<ResourceRequirement> missingResources = new ArrayList<>();
        for (Map.Entry<ResourceProfile, Integer> requirement :
                resourceRequirements.getResourcesWithCount()) {
            ResourceProfile requirementProfile = requirement.getKey();

            int numRequiredResources = requirement.getValue();

            // 查看此时的提供另
            int numAcquiredResources =
                    resourceToRequirementMapping.getNumFulfillingResources(requirementProfile);

            if (numAcquiredResources < numRequiredResources) {
                missingResources.add(
                        ResourceRequirement.create(
                                requirementProfile, numRequiredResources - numAcquiredResources));
            }
        }
        return missingResources;
    }

    /**
     * 查看已经分配的量
     * @return
     */
    public Collection<ResourceRequirement> getAcquiredResources() {
        final Set<ResourceProfile> knownResourceProfiles = new HashSet<>();
        knownResourceProfiles.addAll(resourceToRequirementMapping.getAllResourceProfiles());
        knownResourceProfiles.addAll(excessResources.getResources());

        final List<ResourceRequirement> acquiredResources = new ArrayList<>();
        for (ResourceProfile knownResourceProfile : knownResourceProfiles) {
            int numTotalAcquiredResources =
                    resourceToRequirementMapping.getNumFulfilledRequirements(knownResourceProfile)
                            + excessResources.getResourceCount(knownResourceProfile);
            ResourceRequirement resourceRequirement =
                    ResourceRequirement.create(knownResourceProfile, numTotalAcquiredResources);
            acquiredResources.add(resourceRequirement);
        }

        return acquiredResources;
    }

    public boolean isEmpty() {
        return resourceRequirements.isEmpty() && excessResources.isEmpty();
    }

    public boolean isRequirementEmpty() {
        return resourceRequirements.isEmpty();
    }

    /**
     * 查询多出的资源
     */
    private void findExcessSlots() {
        final Collection<ExcessResource> excessResources = new ArrayList<>();

        // 遍历此时已经分配的所有资源
        for (ResourceProfile requirementProfile :
                resourceToRequirementMapping.getAllRequirementProfiles()) {

            // 获取需求数量
            int numTotalRequiredResources =
                    resourceRequirements.getResourceCount(requirementProfile);
            // 获取分配数量
            int numTotalAcquiredResources =
                    resourceToRequirementMapping.getNumFulfillingResources(requirementProfile);

            // 分配多了才需要处理
            if (numTotalAcquiredResources > numTotalRequiredResources) {
                int numExcessResources = numTotalAcquiredResources - numTotalRequiredResources;

                // 确认分配出的资源有哪些
                for (Map.Entry<ResourceProfile, Integer> acquiredResource :
                        resourceToRequirementMapping
                                .getResourcesFulfilling(requirementProfile)
                                .getResourcesWithCount()) {
                    ResourceProfile acquiredResourceProfile = acquiredResource.getKey();
                    int numAcquiredResources = acquiredResource.getValue();

                    if (numAcquiredResources <= numExcessResources) {
                        // 包装成多出的对象
                        excessResources.add(
                                new ExcessResource(
                                        requirementProfile,
                                        acquiredResourceProfile,
                                        numAcquiredResources));

                        numExcessResources -= numAcquiredResources;
                    } else {
                        excessResources.add(
                                new ExcessResource(
                                        requirementProfile,
                                        acquiredResourceProfile,
                                        numExcessResources));
                        break;
                    }
                }
            }
        }

        // 表示发现了多出的资源
        if (!excessResources.isEmpty()) {
            LOG.debug("Detected excess resources for job {}: {}", jobId, excessResources);
            for (ExcessResource excessResource : excessResources) {
                // 进行资源的转移
                resourceToRequirementMapping.decrementCount(
                        excessResource.requirementProfile,
                        excessResource.resourceProfile,
                        excessResource.numExcessResources);
                this.excessResources =
                        this.excessResources.add(
                                excessResource.resourceProfile, excessResource.numExcessResources);
            }
        }
    }

    private void tryAssigningExcessSlots() {
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "There are {} excess resources for job {} before re-assignment.",
                    excessResources.getTotalResourceCount(),
                    jobId);
        }

        ResourceCounter assignedResources = ResourceCounter.empty();
        // 遍历多出的资源
        for (Map.Entry<ResourceProfile, Integer> excessResource :
                excessResources.getResourcesWithCount()) {
            for (int i = 0; i < excessResource.getValue(); i++) {
                final ResourceProfile resourceProfile = excessResource.getKey();

                // 发现有资源需要 相当于是为多出的资源做一次调配
                final Optional<ResourceProfile> matchingRequirement =
                        findMatchingRequirement(resourceProfile);
                if (matchingRequirement.isPresent()) {
                    resourceToRequirementMapping.incrementCount(
                            matchingRequirement.get(), resourceProfile, 1);
                    assignedResources = assignedResources.add(resourceProfile, 1);
                } else {
                    break;
                }
            }
        }

        for (Map.Entry<ResourceProfile, Integer> assignedResource :
                assignedResources.getResourcesWithCount()) {
            excessResources =
                    excessResources.subtract(
                            assignedResource.getKey(), assignedResource.getValue());
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "There are {} excess resources for job {} after re-assignment.",
                    excessResources.getTotalResourceCount(),
                    jobId);
        }
    }

    /**
     * 描述多出的资源
     */
    private static class ExcessResource {
        private final ResourceProfile requirementProfile;
        private final ResourceProfile resourceProfile;
        private final int numExcessResources;

        private ExcessResource(
                ResourceProfile requirementProfile,
                ResourceProfile resourceProfile,
                int numExcessResources) {
            this.requirementProfile = requirementProfile;
            this.resourceProfile = resourceProfile;
            this.numExcessResources = numExcessResources;
        }

        @Override
        public String toString() {
            return "ExcessResource{"
                    + "numExcessResources="
                    + numExcessResources
                    + ", requirementProfile="
                    + requirementProfile
                    + ", resourceProfile="
                    + resourceProfile
                    + '}';
        }
    }
}
