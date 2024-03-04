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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.IterableStateSnapshot;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Base class for state tables. Accesses to state are typically scoped by the currently active key,
 * as provided through the {@link InternalKeyContext}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 *
 *           stateTable 可以作为迭代器 遍历内部的状态
 *           也可以作为快照的reader对象  用于读取快照并恢复数据
 *           一个StateTable对应一个逻辑概念上的状态
 */
public abstract class StateTable<K, N, S>
        implements StateSnapshotRestore, Iterable<StateEntry<K, N, S>> {

    /**
     * The key context view on the backend. This provides information, such as the currently active
     * key.
     * 可以查看当前读取的key 以及修改key
     */
    protected final InternalKeyContext<K> keyContext;

    /** Combined meta information such as name and serializers for this state.
     * 状态的元数据信息  也可以产生对应的快照 其中包含了序列化对象
     * */
    protected RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo;

    /** The serializer of the key.
     * 上面的序列化针对整个state 这个是仅针对key state可以抽出key
     * */
    protected final TypeSerializer<K> keySerializer;

    /** The current key group range.
     * 通过keyGroup 算法 划分范围
     * */
    protected final KeyGroupRange keyGroupRange;

    /**
     * Map for holding the actual state objects. The outer array represents the key-groups. All
     * array positions will be initialized with an empty state map.
     *
     * 先通过keyGroup算法 稀释掉state 然后分散的state又各自进入到自己的 stateMap中
     */
    protected final StateMap<K, N, S>[] keyGroupedStateMaps;

    /**
     * @param keyContext the key context provides the key scope for all put/get/delete operations.
     * @param metaInfo the meta information, including the type serializer for state copy-on-write.
     * @param keySerializer the serializer of the key.
     */
    public StateTable(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer) {
        this.keyContext = Preconditions.checkNotNull(keyContext);
        this.metaInfo = Preconditions.checkNotNull(metaInfo);
        this.keySerializer = Preconditions.checkNotNull(keySerializer);

        this.keyGroupRange = keyContext.getKeyGroupRange();

        // 根据key数量创建map
        @SuppressWarnings("unchecked")
        StateMap<K, N, S>[] state =
                (StateMap<K, N, S>[])
                        new StateMap[keyContext.getKeyGroupRange().getNumberOfKeyGroups()];
        this.keyGroupedStateMaps = state;
        for (int i = 0; i < this.keyGroupedStateMaps.length; i++) {
            this.keyGroupedStateMaps[i] = createStateMap();
        }
    }

    /**
     * 不同table 对应不同map
     * @return
     */
    protected abstract StateMap<K, N, S> createStateMap();

    /**
     * 对应StateMap的快照
     * @return
     */
    @Override
    @Nonnull
    public abstract IterableStateSnapshot<K, N, S> stateSnapshot();

    // Main interface methods of StateTable -------------------------------------------------------

    /**
     * Returns whether this {@link StateTable} is empty.
     *
     * @return {@code true} if this {@link StateTable} has no elements, {@code false} otherwise.
     * @see #size()
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the total number of entries in this {@link StateTable}. This is the sum of both
     * sub-tables.
     *
     * @return the number of entries in this {@link StateTable}.
     */
    public int size() {
        int count = 0;
        for (StateMap<K, N, S> stateMap : keyGroupedStateMaps) {
            count += stateMap.size();
        }
        return count;
    }

    /**
     * Returns the state of the mapping for the composite of active key and given namespace.
     *
     * @param namespace the namespace. Not null.
     * @return the states of the mapping with the specified key/namespace composite key, or {@code
     *     null} if no mapping for the specified key is found.
     *     通过命名空间 + context的一些信息 读取state
     */
    public S get(N namespace) {
        return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Returns whether this table contains a mapping for the composite of active key and given
     * namespace.
     *
     * @param namespace the namespace in the composite key to search for. Not null.
     * @return {@code true} if this map contains the specified key/namespace composite key, {@code
     *     false} otherwise.
     *     查看state是否存在
     */
    public boolean containsKey(N namespace) {
        return containsKey(
                keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Maps the composite of active key and given namespace to the specified state.
     *
     * @param namespace the namespace. Not null.
     * @param state the state. Can be null.
     */
    public void put(N namespace, S state) {
        put(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, state);
    }

    /**
     * Removes the mapping for the composite of active key and given namespace. This method should
     * be preferred over {@link #removeAndGetOld(N)} when the caller is not interested in the old
     * state.
     *
     * @param namespace the namespace of the mapping to remove. Not null.
     */
    public void remove(N namespace) {
        remove(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Removes the mapping for the composite of active key and given namespace, returning the state
     * that was found under the entry.
     *
     * @param namespace the namespace of the mapping to remove. Not null.
     * @return the state of the removed mapping or {@code null} if no mapping for the specified key
     *     was found.
     */
    public S removeAndGetOld(N namespace) {
        return removeAndGetOld(
                keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
    }

    /**
     * Applies the given {@link StateTransformationFunction} to the state (1st input argument),
     * using the given value as second input argument. The result of {@link
     * StateTransformationFunction#apply(Object, Object)} is then stored as the new state. This
     * function is basically an optimization for get-update-put pattern.
     *
     * @param namespace the namespace. Not null.
     * @param value the value to use in transforming the state. Can be null.
     * @param transformation the transformation function.
     * @throws Exception if some exception happens in the transformation function.
     */
    public <T> void transform(
            N namespace, T value, StateTransformationFunction<S, T> transformation)
            throws Exception {
        K key = keyContext.getCurrentKey();
        // 确保参数不为空
        checkKeyNamespacePreconditions(key, namespace);

        int keyGroup = keyContext.getCurrentKeyGroupIndex();
        // 找到map并调用方法
        getMapForKeyGroup(keyGroup).transform(key, namespace, value, transformation);
    }

    // For queryable state ------------------------------------------------------------------------

    /**
     * Returns the state for the composite of active key and given namespace. This is typically used
     * by queryable state.
     *
     * @param key the key. Not null.
     * @param namespace the namespace. Not null.
     * @return the state of the mapping with the specified key/namespace composite key, or {@code
     *     null} if no mapping for the specified key is found.
     */
    public S get(K key, N namespace) {
        // 找到map下标
        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(key, keyContext.getNumberOfKeyGroups());
        return get(key, keyGroup, namespace);
    }

    public Stream<K> getKeys(N namespace) {
        return Arrays.stream(keyGroupedStateMaps)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                // 展开所有entry 找到namespace匹配的对象
                .filter(entry -> entry.getNamespace().equals(namespace))
                .map(StateEntry::getKey);
    }

    public Stream<Tuple2<K, N>> getKeysAndNamespaces() {
        return Arrays.stream(keyGroupedStateMaps)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                .map(entry -> Tuple2.of(entry.getKey(), entry.getNamespace()));
    }

    public StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return new StateEntryIterator(recommendedMaxNumberOfReturnedRecords);
    }

    // ------------------------------------------------------------------------

    // 定位到map后 就可以简单的调用api了

    private S get(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        return getMapForKeyGroup(keyGroupIndex).get(key, namespace);
    }

    private boolean containsKey(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        return getMapForKeyGroup(keyGroupIndex).containsKey(key, namespace);
    }

    private void checkKeyNamespacePreconditions(K key, N namespace) {
        Preconditions.checkNotNull(
                key, "No key set. This method should not be called outside of a keyed context.");
        Preconditions.checkNotNull(namespace, "Provided namespace is null.");
    }

    private void remove(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        getMapForKeyGroup(keyGroupIndex).remove(key, namespace);
    }

    private S removeAndGetOld(K key, int keyGroupIndex, N namespace) {
        checkKeyNamespacePreconditions(key, namespace);
        return getMapForKeyGroup(keyGroupIndex).removeAndGetOld(key, namespace);
    }

    // ------------------------------------------------------------------------
    //  access to maps
    // ------------------------------------------------------------------------

    /** Returns the internal data structure. */
    @VisibleForTesting
    public StateMap<K, N, S>[] getState() {
        return keyGroupedStateMaps;
    }

    public int getKeyGroupOffset() {
        return keyGroupRange.getStartKeyGroup();
    }

    @VisibleForTesting
    public StateMap<K, N, S> getMapForKeyGroup(int keyGroupIndex) {
        final int pos = indexToOffset(keyGroupIndex);
        if (pos >= 0 && pos < keyGroupedStateMaps.length) {
            return keyGroupedStateMaps[pos];
        } else {
            throw KeyGroupRangeOffsets.newIllegalKeyGroupException(keyGroupIndex, keyGroupRange);
        }
    }

    /** Translates a key-group id to the internal array offset. */
    private int indexToOffset(int index) {
        return index - getKeyGroupOffset();
    }

    // Meta data setter / getter and toString -----------------------------------------------------

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public TypeSerializer<S> getStateSerializer() {
        return metaInfo.getStateSerializer();
    }

    public TypeSerializer<N> getNamespaceSerializer() {
        return metaInfo.getNamespaceSerializer();
    }

    public RegisteredKeyValueStateBackendMetaInfo<N, S> getMetaInfo() {
        return metaInfo;
    }

    public void setMetaInfo(RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo) {
        this.metaInfo = metaInfo;
    }

    // Snapshot / Restore -------------------------------------------------------------------------

    public void put(K key, int keyGroup, N namespace, S state) {
        checkKeyNamespacePreconditions(key, namespace);
        getMapForKeyGroup(keyGroup).put(key, namespace, state);
    }

    @Override
    public Iterator<StateEntry<K, N, S>> iterator() {
        return Arrays.stream(keyGroupedStateMaps)
                .filter(Objects::nonNull)
                .flatMap(
                        stateMap ->
                                StreamSupport.stream(
                                        Spliterators.spliteratorUnknownSize(stateMap.iterator(), 0),
                                        false))
                .iterator();
    }

    // For testing --------------------------------------------------------------------------------

    @VisibleForTesting
    public int sizeOfNamespace(Object namespace) {
        int count = 0;
        for (StateMap<K, N, S> stateMap : keyGroupedStateMaps) {
            count += stateMap.sizeOfNamespace(namespace);
        }

        return count;
    }

    /**
     * 指定要读取的版本后 返回reader对象
     * @param readVersion
     * @return
     */
    @Nonnull
    @Override
    public StateSnapshotKeyGroupReader keyGroupReader(int readVersion) {
        return StateTableByKeyGroupReaders.readerForVersion(this, readVersion);
    }

    // StateEntryIterator
    // ---------------------------------------------------------------------------------------------

    /**
     * 只能遍历部分记录
     */
    class StateEntryIterator implements StateIncrementalVisitor<K, N, S> {

        /**
         * 每个map都可以读这么多
         */
        final int recommendedMaxNumberOfReturnedRecords;

        int keyGroupIndex;

        // 实际上就是利用visitor
        StateIncrementalVisitor<K, N, S> stateIncrementalVisitor;

        StateEntryIterator(int recommendedMaxNumberOfReturnedRecords) {
            this.recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
            this.keyGroupIndex = 0;
            next();
        }

        private void next() {
            while (keyGroupIndex < keyGroupedStateMaps.length) {
                StateMap<K, N, S> stateMap = keyGroupedStateMaps[keyGroupIndex++];
                StateIncrementalVisitor<K, N, S> visitor =
                        stateMap.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
                if (visitor.hasNext()) {
                    stateIncrementalVisitor = visitor;
                    return;
                }
            }
        }

        @Override
        public boolean hasNext() {
            // visitor为空 或者无了
            while (stateIncrementalVisitor == null || !stateIncrementalVisitor.hasNext()) {
                // 读取完了
                if (keyGroupIndex == keyGroupedStateMaps.length) {
                    return false;
                }

                // 更新visitor
                StateIncrementalVisitor<K, N, S> visitor =
                        keyGroupedStateMaps[keyGroupIndex++].getStateIncrementalVisitor(
                                recommendedMaxNumberOfReturnedRecords);
                if (visitor.hasNext()) {
                    stateIncrementalVisitor = visitor;
                    break;
                }
            }
            return true;
        }

        @Override
        public Collection<StateEntry<K, N, S>> nextEntries() {
            if (!hasNext()) {
                return null;
            }

            return stateIncrementalVisitor.nextEntries();
        }

        @Override
        public void remove(StateEntry<K, N, S> stateEntry) {
            keyGroupedStateMaps[keyGroupIndex - 1].remove(
                    stateEntry.getKey(), stateEntry.getNamespace());
        }

        @Override
        public void update(StateEntry<K, N, S> stateEntry, S newValue) {
            keyGroupedStateMaps[keyGroupIndex - 1].put(
                    stateEntry.getKey(), stateEntry.getNamespace(), newValue);
        }
    }
}
