/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupPartitioner;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * This wrapper combines a HeapPriorityQueue with backend meta data.
 *
 * @param <T> type of the queue elements.
 *           将一些组件包装起来
 */
public class HeapPriorityQueueSnapshotRestoreWrapper<T extends HeapPriorityQueueElement>
        implements StateSnapshotRestore {

    /**
     * 优先队列 并关联一组基于keyGroup的hashMap
     */
    @Nonnull private final HeapPriorityQueueSet<T> priorityQueue;
    /**
     * 提取key
     */
    @Nonnull private final KeyExtractorFunction<T> keyExtractorFunction;

    /**
     * 表示使用优先队列存储状态时 的元数据信息
     */
    @Nonnull private final RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo;
    @Nonnull private final KeyGroupRange localKeyGroupRange;
    @Nonnegative private final int totalKeyGroups;

    public HeapPriorityQueueSnapshotRestoreWrapper(
            @Nonnull HeapPriorityQueueSet<T> priorityQueue,
            @Nonnull RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo,
            @Nonnull KeyExtractorFunction<T> keyExtractorFunction,
            @Nonnull KeyGroupRange localKeyGroupRange,
            int totalKeyGroups) {

        this.priorityQueue = priorityQueue;
        this.keyExtractorFunction = keyExtractorFunction;
        this.metaInfo = metaInfo;
        this.localKeyGroupRange = localKeyGroupRange;
        this.totalKeyGroups = totalKeyGroups;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public HeapPriorityQueueStateSnapshot<T> stateSnapshot() {
        final T[] queueDump =
                (T[]) priorityQueue.toArray(new HeapPriorityQueueElement[priorityQueue.size()]);
        return new HeapPriorityQueueStateSnapshot<T>(
                queueDump,
                keyExtractorFunction,
                metaInfo.deepCopy(),
                localKeyGroupRange,
                totalKeyGroups);
    }

    /**
     * 返回用于读取快照的对象
     * @param readVersionHint the required version of the state to read.
     * @return
     */
    @Nonnull
    @Override
    public StateSnapshotKeyGroupReader keyGroupReader(int readVersionHint) {
        final TypeSerializer<T> elementSerializer = metaInfo.getElementSerializer();
        return KeyGroupPartitioner.createKeyGroupPartitionReader(
                elementSerializer
                        ::deserialize, // we know that this does not deliver nulls, because we never
                // write nulls
                (element, keyGroupId) -> priorityQueue.add(element));
    }

    @Nonnull
    public HeapPriorityQueueSet<T> getPriorityQueue() {
        return priorityQueue;
    }

    @Nonnull
    public RegisteredPriorityQueueStateBackendMetaInfo<T> getMetaInfo() {
        return metaInfo;
    }

    /**
     * Returns a deep copy of the snapshot, where the serializer is changed to the given serializer.
     */
    public HeapPriorityQueueSnapshotRestoreWrapper<T> forUpdatedSerializer(
            @Nonnull TypeSerializer<T> updatedSerializer) {
        return forUpdatedSerializer(updatedSerializer, false);
    }

    /**
     * Returns a deep copy of the snapshot, where the serializer is re-registered by the serializer
     * snapshot or changed to the given serializer.
     *
     * @param updatedSerializer updated serializer.
     * @param allowFutureMetadataUpdates whether allow metadata to update in the future or not.
     * @return the queue with the specified unique name.
     * 更新元数据  产生新对象
     */
    public HeapPriorityQueueSnapshotRestoreWrapper<T> forUpdatedSerializer(
            @Nonnull TypeSerializer<T> updatedSerializer, boolean allowFutureMetadataUpdates) {

        RegisteredPriorityQueueStateBackendMetaInfo<T> updatedMetaInfo =
                new RegisteredPriorityQueueStateBackendMetaInfo<>(
                        metaInfo.getName(), updatedSerializer);

        updatedMetaInfo =
                allowFutureMetadataUpdates
                        ? updatedMetaInfo.withSerializerUpgradesAllowed()
                        : updatedMetaInfo;

        return new HeapPriorityQueueSnapshotRestoreWrapper<>(
                priorityQueue,
                updatedMetaInfo,
                keyExtractorFunction,
                localKeyGroupRange,
                totalKeyGroups);
    }
}
