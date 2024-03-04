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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Default {@link ResourceTracker} implementation.
 * 追踪每个job的资源
 * */
public class DefaultResourceTracker implements ResourceTracker {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultResourceTracker.class);

    /**
     * 每个 JobScopedResourceTracker 追踪每个job的资源需求
     */
    private final Map<JobID, JobScopedResourceTracker> trackers = new HashMap<>();

    /**
     * 通知资源需求
     * @param jobId the job that the resource requirements belongs to
     * @param resourceRequirements new resource requirements
     */
    @Override
    public void notifyResourceRequirements(
            JobID jobId, Collection<ResourceRequirement> resourceRequirements) {
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(resourceRequirements);
        LOG.trace(
                "Received notification for job {} having new resource requirements {}.",
                jobId,
                resourceRequirements);
        getOrCreateTracker(jobId).notifyResourceRequirements(resourceRequirements);

        // 表示可能是清理操作
        if (resourceRequirements.isEmpty()) {
            checkWhetherTrackerCanBeRemoved(jobId, trackers.get(jobId));
        }
    }

    private void checkWhetherTrackerCanBeRemoved(JobID jobId, JobScopedResourceTracker tracker) {
        // 在上面的操作中 resourceRequirements 已经变空 只要没有excessResources 即可  也就是一开始内部还不能有资源 不然都会变成excessResources
        if (tracker.isEmpty()) {
            LOG.debug("Stopping tracking of resources for job {}.", jobId);
            trackers.remove(jobId);
        }
    }

    /**
     * 分配给某个job 资源
     * @param jobId the job that acquired the resource
     * @param resourceProfile profile of the resource
     */
    @Override
    public void notifyAcquiredResource(JobID jobId, ResourceProfile resourceProfile) {
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(resourceProfile);
        LOG.trace(
                "Received notification for job {} having acquired resource {}.",
                jobId,
                resourceProfile);
        getOrCreateTracker(jobId).notifyAcquiredResource(resourceProfile);
    }

    /**
     * 获取/创建 job的资源追踪对象
     * @param jobId
     * @return
     */
    private JobScopedResourceTracker getOrCreateTracker(JobID jobId) {
        return trackers.computeIfAbsent(
                jobId,
                ignored -> {
                    LOG.debug("Initiating tracking of resources for job {}.", jobId);
                    return new JobScopedResourceTracker(jobId);
                });
    }

    /**
     * 通知job失去了某个资源
     * @param jobId the job that lost the resource
     * @param resourceProfile profile of the resource
     */
    @Override
    public void notifyLostResource(JobID jobId, ResourceProfile resourceProfile) {
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(resourceProfile);
        JobScopedResourceTracker tracker = trackers.get(jobId);

        // during shutdown the tracker is cleared before task executors are unregistered,
        // to prevent the loss of resources triggering new allocations
        if (tracker != null) {
            LOG.trace(
                    "Received notification for job {} having lost resource {}.",
                    jobId,
                    resourceProfile);
            tracker.notifyLostResource(resourceProfile);

            checkWhetherTrackerCanBeRemoved(jobId, tracker);
        } else {
            LOG.trace(
                    "Received notification for job {} having lost resource {}, but no such job was tracked.",
                    jobId,
                    resourceProfile);
        }
    }

    @Override
    public void clear() {
        trackers.clear();
    }

    /**
     * 获取缺失(需要)的资源
     * @return
     */
    @Override
    public Map<JobID, Collection<ResourceRequirement>> getMissingResources() {
        Map<JobID, Collection<ResourceRequirement>> allMissingResources = new HashMap<>();
        for (Map.Entry<JobID, JobScopedResourceTracker> tracker : trackers.entrySet()) {
            Collection<ResourceRequirement> missingResources =
                    tracker.getValue().getMissingResources();
            if (!missingResources.isEmpty()) {
                allMissingResources.put(tracker.getKey(), missingResources);
            }
        }
        return allMissingResources;
    }

    /**
     * 返回已经分配的资源
     * @param jobId job ID
     * @return
     */
    @Override
    public Collection<ResourceRequirement> getAcquiredResources(JobID jobId) {
        Preconditions.checkNotNull(jobId);
        JobScopedResourceTracker tracker = trackers.get(jobId);
        return tracker == null ? Collections.emptyList() : tracker.getAcquiredResources();
    }

    @Override
    public boolean isRequirementEmpty(JobID jobId) {
        return Optional.ofNullable(trackers.get(jobId))
                .map(JobScopedResourceTracker::isRequirementEmpty)
                .orElse(true);
    }

    @VisibleForTesting
    boolean isEmpty() {
        return trackers.isEmpty();
    }
}
