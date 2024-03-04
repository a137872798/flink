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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link PreferredLocationsRetriever}. Locations based on state will be
 * returned if exist. Otherwise locations based on inputs will be returned.
 * 该对象可以为Execution选择更好的位置
 */
public class DefaultPreferredLocationsRetriever implements PreferredLocationsRetriever {

    static final int MAX_DISTINCT_LOCATIONS_TO_CONSIDER = 8;

    static final int MAX_DISTINCT_CONSUMERS_TO_CONSIDER = 8;

    /**
     * 可以获取某个子任务的TM位置
     */
    private final StateLocationRetriever stateLocationRetriever;

    private final InputsLocationsRetriever inputsLocationsRetriever;

    DefaultPreferredLocationsRetriever(
            final StateLocationRetriever stateLocationRetriever,
            final InputsLocationsRetriever inputsLocationsRetriever) {

        this.stateLocationRetriever = checkNotNull(stateLocationRetriever);
        this.inputsLocationsRetriever = checkNotNull(inputsLocationsRetriever);
    }

    /**
     * 查找更合适的位置
     * @param executionVertexId id of the execution vertex
     * @param producersToIgnore producer vertices to ignore when calculating input locations
     * @return
     */
    @Override
    public CompletableFuture<Collection<TaskManagerLocation>> getPreferredLocations(
            final ExecutionVertexID executionVertexId,
            final Set<ExecutionVertexID> producersToIgnore) {

        checkNotNull(executionVertexId);
        checkNotNull(producersToIgnore);

        // 这里只有一个位置啊
        final Collection<TaskManagerLocation> preferredLocationsBasedOnState =
                getPreferredLocationsBasedOnState(executionVertexId);
        if (!preferredLocationsBasedOnState.isEmpty()) {
            // 作为结果返回了
            return CompletableFuture.completedFuture(preferredLocationsBasedOnState);
        }

        // 如果此时还没有设置位置  从输入来考虑
        return getPreferredLocationsBasedOnInputs(executionVertexId, producersToIgnore);
    }

    /**
     * 找到execution的位置
     * @param executionVertexId
     * @return
     */
    private Collection<TaskManagerLocation> getPreferredLocationsBasedOnState(
            final ExecutionVertexID executionVertexId) {

        return stateLocationRetriever
                .getStateLocation(executionVertexId)
                .map(Collections::singleton)
                .orElse(Collections.emptySet());
    }

    /**
     *
     * @param executionVertexId
     * @param producersToIgnore
     * @return
     */
    private CompletableFuture<Collection<TaskManagerLocation>> getPreferredLocationsBasedOnInputs(
            final ExecutionVertexID executionVertexId,
            final Set<ExecutionVertexID> producersToIgnore) {

        CompletableFuture<Collection<TaskManagerLocation>> preferredLocations =
                CompletableFuture.completedFuture(Collections.emptyList());

        // 得到由该子任务消费的数据 也就是input数据
        final Collection<ConsumedPartitionGroup> consumedPartitionGroups =
                inputsLocationsRetriever.getConsumedPartitionGroups(executionVertexId);

        // 遍历每个被消费的数据组
        for (ConsumedPartitionGroup consumedPartitionGroup : consumedPartitionGroups) {
            // Ignore the location of a consumed partition group if it has too many distinct
            // consumers. This is to avoid tasks unevenly distributed on nodes when running batch
            // jobs or running jobs in session/standalone mode.
            // 表示该数据组有很多消费者 (下游Execution) 就忽略
            if (consumedPartitionGroup.getConsumerVertexGroup().size()
                    > MAX_DISTINCT_CONSUMERS_TO_CONSIDER) {
                continue;
            }

            final Collection<CompletableFuture<TaskManagerLocation>> locationsFutures =
                    getInputLocationFutures(
                            producersToIgnore,
                            inputsLocationsRetriever.getProducersOfConsumedPartitionGroup(
                                    consumedPartitionGroup));

            // 结合到结果里
            preferredLocations = combineLocations(preferredLocations, locationsFutures);
        }
        return preferredLocations;
    }

    /**
     * 找到一组备选位置
     * @param producersToIgnore
     * @param producers
     * @return
     */
    private Collection<CompletableFuture<TaskManagerLocation>> getInputLocationFutures(
            final Set<ExecutionVertexID> producersToIgnore,
            final Collection<ExecutionVertexID> producers) {

        final Collection<CompletableFuture<TaskManagerLocation>> locationsFutures =
                new ArrayList<>();

        // 产生消费数据的多个子任务
        for (ExecutionVertexID producer : producers) {
            final Optional<CompletableFuture<TaskManagerLocation>> optionalLocationFuture;
            // 非忽略节点才获取位置
            if (!producersToIgnore.contains(producer)) {
                optionalLocationFuture = inputsLocationsRetriever.getTaskManagerLocation(producer);
            } else {
                optionalLocationFuture = Optional.empty();
            }
            optionalLocationFuture.ifPresent(locationsFutures::add);

            // inputs which have too many distinct sources are not considered because
            // input locality does not make much difference in this case and it could
            // be a long time to wait for all the location futures to complete
            // 位置太多也不行
            if (locationsFutures.size() > MAX_DISTINCT_LOCATIONS_TO_CONSIDER) {
                return Collections.emptyList();
            }
        }

        return locationsFutures;
    }

    /**
     * 合并位置信息
     * @param locationsCombinedAlready  之前的数据
     * @param locationsToCombine   本次新增的数据
     * @return
     */
    private CompletableFuture<Collection<TaskManagerLocation>> combineLocations(
            final CompletableFuture<Collection<TaskManagerLocation>> locationsCombinedAlready,
            final Collection<CompletableFuture<TaskManagerLocation>> locationsToCombine) {

        final CompletableFuture<Set<TaskManagerLocation>> uniqueLocationsFuture =
                FutureUtils.combineAll(locationsToCombine).thenApply(HashSet::new);

        return locationsCombinedAlready.thenCombine(
                uniqueLocationsFuture,
                (locationsOnOneEdge, locationsOnAnotherEdge) -> {
                    // 返回少的
                    if ((!locationsOnOneEdge.isEmpty()
                                    && locationsOnAnotherEdge.size() > locationsOnOneEdge.size())
                            || locationsOnAnotherEdge.isEmpty()) {
                        return locationsOnOneEdge;
                    } else {
                        return locationsOnAnotherEdge;
                    }
                });
    }
}
