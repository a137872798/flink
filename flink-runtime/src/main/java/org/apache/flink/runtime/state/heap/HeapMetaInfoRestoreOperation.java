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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A helper class shared between the {@link HeapRestoreOperation} and {@link
 * HeapSavepointRestoreOperation} for restoring {@link StateMetaInfoSnapshot
 * StateMetaInfoSnapshots}.
 *
 * @param <K> The key by which state is keyed.
 *           用于恢复元数据相关的
 */
class HeapMetaInfoRestoreOperation<K> {
    /**
     * 用于反序列化
     */
    private final StateSerializerProvider<K> keySerializerProvider;
    @Nonnull private final KeyGroupRange keyGroupRange;
    @Nonnegative private final int numberOfKeyGroups;

    // 使用工厂初始化内存结构
    private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
    private final StateTableFactory<K> stateTableFactory;
    private final InternalKeyContext<K> keyContext;

    HeapMetaInfoRestoreOperation(
            StateSerializerProvider<K> keySerializerProvider,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            @Nonnull KeyGroupRange keyGroupRange,
            int numberOfKeyGroups,
            StateTableFactory<K> stateTableFactory,
            InternalKeyContext<K> keyContext) {
        this.keySerializerProvider = keySerializerProvider;
        this.priorityQueueSetFactory = priorityQueueSetFactory;
        this.keyGroupRange = keyGroupRange;
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.stateTableFactory = stateTableFactory;
        this.keyContext = keyContext;
    }

    /**
     * 通过元数据快照  恢复快照id
     * @param restoredMetaInfo
     * @param registeredKVStates
     * @param registeredPQStates
     * @return
     */
    Map<Integer, StateMetaInfoSnapshot> createOrCheckStateForMetaInfo(
            List<StateMetaInfoSnapshot> restoredMetaInfo,
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates) {

        final Map<Integer, StateMetaInfoSnapshot> kvStatesById = new HashMap<>();

        // 遍历每个state元数据
        for (StateMetaInfoSnapshot metaInfoSnapshot : restoredMetaInfo) {
            final StateSnapshotRestore registeredState;

            // 先找到相应的类型
            switch (metaInfoSnapshot.getBackendStateType()) {
                case KEY_VALUE:
                    // 先查找是否已经加载了该state
                    registeredState = registeredKVStates.get(metaInfoSnapshot.getName());
                    if (registeredState == null) {
                        // 产生元数据
                        RegisteredKeyValueStateBackendMetaInfo<?, ?>
                                registeredKeyedBackendStateMetaInfo =
                                        new RegisteredKeyValueStateBackendMetaInfo<>(
                                                metaInfoSnapshot);

                        // 构建 stateTable (此时的stateTable只是一个空壳) 元数据中包含了序列化对象 辅助数据恢复
                        registeredKVStates.put(
                                metaInfoSnapshot.getName(),
                                stateTableFactory.newStateTable(
                                        keyContext,
                                        registeredKeyedBackendStateMetaInfo,
                                        keySerializerProvider.currentSchemaSerializer()));
                    }
                    break;
                case PRIORITY_QUEUE:
                    registeredState = registeredPQStates.get(metaInfoSnapshot.getName());
                    if (registeredState == null) {
                        registeredPQStates.put(
                                metaInfoSnapshot.getName(),
                                createInternal(
                                        new RegisteredPriorityQueueStateBackendMetaInfo<>(
                                                metaInfoSnapshot)));
                    }
                    break;
                default:
                    throw new IllegalStateException(
                            "Unexpected state type: "
                                    + metaInfoSnapshot.getBackendStateType()
                                    + ".");
            }

            // always put metaInfo into kvStatesById, because kvStatesById is KeyGroupsStateHandle
            // related
            // id 跟着size走
            kvStatesById.put(kvStatesById.size(), metaInfoSnapshot);
        }

        return kvStatesById;
    }

    /**
     * 产生一个基于优先队列存储state的对象
     * @param metaInfo
     * @param <T>
     * @return
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            HeapPriorityQueueSnapshotRestoreWrapper<T> createInternal(
                    RegisteredPriorityQueueStateBackendMetaInfo metaInfo) {

        final String stateName = metaInfo.getName();
        final HeapPriorityQueueSet<T> priorityQueue =
                priorityQueueSetFactory.create(stateName, metaInfo.getElementSerializer());

        return new HeapPriorityQueueSnapshotRestoreWrapper<>(
                priorityQueue,
                metaInfo,
                KeyExtractorFunction.forKeyedObjects(),
                keyGroupRange,
                numberOfKeyGroups);
    }
}
