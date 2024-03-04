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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IterableStateSnapshot;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.ListDelimitedSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshot;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.runtime.state.KeyValueStateIterator} over Heap backend snapshot
 * resources.
 * 用于遍历kv state内部的entry
 */
@Internal
@NotThreadSafe
public final class HeapKeyValueStateIterator implements KeyValueStateIterator {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * 每个state name -> 与 id的映射关系
     */
    private final Map<StateUID, Integer> stateNamesToId;
    /**
     * 每个状态的快照
     */
    private final Map<StateUID, StateSnapshot> stateStableSnapshots;
    private final int keyGroupPrefixBytes;

    private boolean isValid;
    private boolean newKeyGroup;
    private boolean newKVState;
    private byte[] currentKey;
    private byte[] currentValue;

    /** Iterator over the key groups of the corresponding key group range. */
    private final Iterator<Integer> keyGroupIterator;
    /** The current value of the keyGroupIterator. */
    private int currentKeyGroup;

    /** Iterator over all states present in the snapshots. */
    private Iterator<StateUID> statesIterator;
    /** The current value of the statesIterator. */
    private StateUID currentState;
    /**
     * An iterator over the values of the current state. It can be one of three:
     *
     * <ul>
     *   <li>{@link QueueIterator} for iterating over entries in a priority queue
     *   <li>{@link StateTableIterator} for iterating over entries in a StateTable
     *   <li>{@link MapStateIterator} for iterating over entries in a user map, this one falls back
     *       to the upper one automatically if exhausted
     * </ul>
     */
    private SingleStateIterator currentStateIterator;
    /** Helpers for serializing state into the unified format.
     * 存放state值的输出流
     * */
    private final DataOutputSerializer valueOut = new DataOutputSerializer(64);

    /**
     * 可以序列化/反序列化 list类型
     */
    private final ListDelimitedSerializer listDelimitedSerializer = new ListDelimitedSerializer();

    /**
     * 存放key namespace的输出流
     */
    private final SerializedCompositeKeyBuilder<Object> compositeKeyBuilder;

    public HeapKeyValueStateIterator(
            @Nonnull final KeyGroupRange keyGroupRange,
            @Nonnull final TypeSerializer<?> keySerializer,
            @Nonnegative final int totalKeyGroups,
            @Nonnull final Map<StateUID, Integer> stateNamesToId,
            @Nonnull final Map<StateUID, StateSnapshot> stateSnapshots)
            throws IOException {
        checkNotNull(keyGroupRange);
        checkNotNull(keySerializer);
        this.stateNamesToId = checkNotNull(stateNamesToId);
        this.stateStableSnapshots = checkNotNull(stateSnapshots);

        this.statesIterator = stateSnapshots.keySet().iterator();
        this.keyGroupIterator = keyGroupRange.iterator();

        // 表示前缀需要多少个字节表示
        this.keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(totalKeyGroups);
        this.compositeKeyBuilder =
                new SerializedCompositeKeyBuilder<>(
                        castToType(keySerializer), keyGroupPrefixBytes, 32);

        // 这里存储了状态 如果还没有状态数据  那么此时本对象不可用
        if (!keyGroupIterator.hasNext() || !statesIterator.hasNext()) {
            // stop early, no key groups or states
            isValid = false;
        } else {
            // 获得此时在使用的key
            currentKeyGroup = keyGroupIterator.next();
            next();
            this.newKeyGroup = true;
        }
    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    @Override
    public boolean isNewKeyValueState() {
        return this.newKVState;
    }

    @Override
    public boolean isNewKeyGroup() {
        return this.newKeyGroup;
    }

    @Override
    public int keyGroup() {
        return currentKeyGroup;
    }

    /**
     * 获取当前state的id
     * @return
     */
    @Override
    public int kvStateId() {
        return stateNamesToId.get(currentState);
    }

    /**
     * 获取下个state
     */
    @Override
    public void next() throws IOException {
        this.newKVState = false;
        this.newKeyGroup = false;

        boolean nextElementSet = false;
        do {
            // 表示当前state的所有数据都处理完了  需要切换到下个state
            if (currentState == null) {
                boolean hasNextState = moveToNextState();
                if (!hasNextState) {
                    isValid = false;
                    return;
                }
            }

            // 获得下个stateEntry
            boolean hasStateEntry = currentStateIterator != null && currentStateIterator.hasNext();
            if (!hasStateEntry) {
                this.currentState = null;
            }

            // 要将数据写入output
            if (hasStateEntry) {
                nextElementSet = currentStateIterator.writeOutNext();
            }
        } while (!nextElementSet);
        isValid = true;
    }

    /**
     * 读取下个state
     * @return
     * @throws IOException
     */
    private boolean moveToNextState() throws IOException {
        if (statesIterator.hasNext()) {
            // 使用map的迭代器 得到state
            this.currentState = statesIterator.next();
            this.newKVState = true;
            // 每次迭代所有stateEntry时  会使用一个固定的key， 当某个key的元素遍历完后  在这里切换key
            // 之后重新遍历state 因为使用的key不同 得到的stateEntry组也会不同
        } else if (keyGroupIterator.hasNext()) {
            this.currentKeyGroup = keyGroupIterator.next();
            // 这样又会从第一个state开始  不过本次选择的槽就不一样了
            resetStates();
            this.newKeyGroup = true;
            this.newKVState = true;
        } else {
            return false;
        }

        // 上面代表所有key/state都遍历完了

        // 获取当前状态对应的快照
        StateSnapshot stateSnapshot = this.stateStableSnapshots.get(currentState);
        // 在确定state后 还需要一个迭代器来遍历 stateEntry
        setCurrentStateIterator(stateSnapshot);

        // set to a valid entry
        return true;
    }

    private void resetStates() {
        this.statesIterator = stateStableSnapshots.keySet().iterator();
        this.currentState = statesIterator.next();
    }

    /**
     * 设置当前状态快照
     * @param stateSnapshot
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private void setCurrentStateIterator(StateSnapshot stateSnapshot) throws IOException {
        // 快照本身也有多种类型
        if (stateSnapshot instanceof IterableStateSnapshot) {
            // 还原元数据快照
            RegisteredKeyValueStateBackendMetaInfo<Object, Object> metaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            stateSnapshot.getMetaInfoSnapshot());

            // 支持按照key 得到不同的迭代器
            Iterator<? extends StateEntry<?, ?, ?>> snapshotIterator =
                    ((IterableStateSnapshot<?, ?, ?>) stateSnapshot).getIterator(currentKeyGroup);
            // 包装产生迭代器
            this.currentStateIterator = new StateTableIterator(snapshotIterator, metaInfo);
        } else if (stateSnapshot instanceof HeapPriorityQueueStateSnapshot) {
            Iterator<Object> snapshotIterator =
                    ((HeapPriorityQueueStateSnapshot<Object>) stateSnapshot)
                            .getIteratorForKeyGroup(currentKeyGroup);
            RegisteredPriorityQueueStateBackendMetaInfo<Object> metaInfo =
                    new RegisteredPriorityQueueStateBackendMetaInfo<>(
                            stateSnapshot.getMetaInfoSnapshot());

            // 访问优先队列的迭代器
            this.currentStateIterator = new QueueIterator<>(snapshotIterator, metaInfo);
        } else {
            throw new IllegalStateException("Unknown snapshot type: " + stateSnapshot);
        }
    }

    /** A common interface for writing out a single entry in a state.
     * 该迭代器每次可以将一个entry写入输出流
     * */
    private interface SingleStateIterator {

        boolean hasNext();

        /**
         * Sets the {@link #currentKey} and {@link #currentValue} to the value of the next entry in
         * the state.
         *
         * @return false if an entry was empty. It can be the case if we try to serialize an empty
         *     Map or List. In that case we should skip to a next entry.
         */
        boolean writeOutNext() throws IOException;
    }

    private final class StateTableIterator implements SingleStateIterator {

        /**
         * 维护一个state下所有的entry
         */
        private final Iterator<? extends StateEntry<?, ?, ?>> entriesIterator;
        private final RegisteredKeyValueStateBackendMetaInfo<?, ?> stateSnapshot;

        private StateTableIterator(
                Iterator<? extends StateEntry<?, ?, ?>> entriesIterator,
                RegisteredKeyValueStateBackendMetaInfo<?, ?> stateSnapshot) {
            this.entriesIterator = entriesIterator;
            this.stateSnapshot = stateSnapshot;
        }

        /**
         * 判断有无下个entry
         * @return
         */
        @Override
        public boolean hasNext() {
            return entriesIterator.hasNext();
        }

        @Override
        public boolean writeOutNext() throws IOException {
            StateEntry<?, ?, ?> currentEntry = entriesIterator.next();

            // 清除之前写的state value
            valueOut.clear();

            // 将三元组写入输出流
            compositeKeyBuilder.setKeyAndKeyGroup(currentEntry.getKey(), keyGroup());
            compositeKeyBuilder.setNamespace(
                    currentEntry.getNamespace(),
                    castToType(stateSnapshot.getNamespaceSerializer()));


            TypeSerializer<?> stateSerializer = stateSnapshot.getStateSerializer();
            switch (stateSnapshot.getStateType()) {
                // 最后写入state本身
                case AGGREGATING:
                case REDUCING:
                case FOLDING:
                case VALUE:
                    return writeOutValue(currentEntry, stateSerializer);
                case LIST:
                    return writeOutList(currentEntry, stateSerializer);
                case MAP:
                    return writeOutMap(currentEntry, stateSerializer);
                default:
                    throw new IllegalStateException("");
            }
        }

        /**
         * 写入值类型的value
         * @param currentEntry
         * @param stateSerializer
         * @return
         * @throws IOException
         */
        private boolean writeOutValue(
                StateEntry<?, ?, ?> currentEntry, TypeSerializer<?> stateSerializer)
                throws IOException {
            // 此时的数据是key
            currentKey = compositeKeyBuilder.build();
            castToType(stateSerializer).serialize(currentEntry.getState(), valueOut);
            currentValue = valueOut.getCopyOfBuffer();
            return true;
        }

        @SuppressWarnings("unchecked")
        private boolean writeOutList(
                StateEntry<?, ?, ?> currentEntry, TypeSerializer<?> stateSerializer)
                throws IOException {
            List<Object> state = (List<Object>) currentEntry.getState();
            if (state.isEmpty()) {
                return false;
            }
            ListSerializer<Object> listSerializer = (ListSerializer<Object>) stateSerializer;
            currentKey = compositeKeyBuilder.build();
            currentValue =
                    listDelimitedSerializer.serializeList(
                            state, listSerializer.getElementSerializer());
            return true;
        }

        /**
         * 写入map类型的  state value
         * @param currentEntry
         * @param stateSerializer
         * @return
         * @throws IOException
         */
        @SuppressWarnings("unchecked")
        private boolean writeOutMap(
                StateEntry<?, ?, ?> currentEntry, TypeSerializer<?> stateSerializer)
                throws IOException {
            Map<Object, Object> state = (Map<Object, Object>) currentEntry.getState();
            if (state.isEmpty()) {
                return false;
            }
            MapSerializer<Object, Object> mapSerializer =
                    (MapSerializer<Object, Object>) stateSerializer;
            currentStateIterator =
                    new MapStateIterator(
                            state,
                            mapSerializer.getKeySerializer(),
                            mapSerializer.getValueSerializer(),
                            this);
            return currentStateIterator.writeOutNext();
        }
    }

    /**
     * 该携带器 专门处理map类型
     */
    private final class MapStateIterator implements SingleStateIterator {

        private final Iterator<Map.Entry<Object, Object>> mapEntries;
        private final TypeSerializer<Object> userKeySerializer;
        private final TypeSerializer<Object> userValueSerializer;
        private final StateTableIterator parentIterator;

        private MapStateIterator(
                Map<Object, Object> mapEntries,
                TypeSerializer<Object> userKeySerializer,
                TypeSerializer<Object> userValueSerializer,
                StateTableIterator parentIterator) {
            assert !mapEntries.isEmpty();
            this.mapEntries = mapEntries.entrySet().iterator();
            this.userKeySerializer = userKeySerializer;
            this.userValueSerializer = userValueSerializer;
            this.parentIterator = parentIterator;
        }

        @Override
        public boolean hasNext() {
            // we should never end up here with an exhausted map iterator
            // if an iterator is exhausted in the writeOutNext we switch back to
            // the originating StateTableIterator
            assert mapEntries.hasNext();
            return true;
        }

        @Override
        public boolean writeOutNext() throws IOException {
            // 遍历内部元素
            Map.Entry<Object, Object> entry = mapEntries.next();
            valueOut.clear();
            // 这个key是外层 key + namespace的基础上 又增加了内层map的key
            currentKey =
                    compositeKeyBuilder.buildCompositeKeyUserKey(entry.getKey(), userKeySerializer);
            Object userValue = entry.getValue();
            valueOut.writeBoolean(userValue == null);
            userValueSerializer.serialize(userValue, valueOut);
            currentValue = valueOut.getCopyOfBuffer();

            if (!mapEntries.hasNext()) {
                // 写完后 还原外面的currentStateIterator
                currentStateIterator = parentIterator;
            }
            return true;
        }
    }

    /**
     * 当使用优先队列存储state时  使用该迭代器
     * @param <T>
     */
    private final class QueueIterator<T> implements SingleStateIterator {
        private final Iterator<T> elementsForKeyGroup;
        private final RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo;
        private final DataOutputSerializer keyOut = new DataOutputSerializer(128);
        private final int afterKeyMark;

        public QueueIterator(
                Iterator<T> elementsForKeyGroup,
                RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo)
                throws IOException {
            this.elementsForKeyGroup = elementsForKeyGroup;
            this.metaInfo = metaInfo;
            CompositeKeySerializationUtils.writeKeyGroup(keyGroup(), keyGroupPrefixBytes, keyOut);
            afterKeyMark = keyOut.length();
        }

        @Override
        public boolean hasNext() {
            return elementsForKeyGroup.hasNext();
        }

        @Override
        public boolean writeOutNext() throws IOException {
            // 使用优先队列时  应该不需要用到value
            currentValue = EMPTY_BYTE_ARRAY;
            keyOut.setPosition(afterKeyMark);
            // 得到下个entry
            T next = elementsForKeyGroup.next();
            // 这里就是更新key的值
            metaInfo.getElementSerializer().serialize(next, keyOut);
            currentKey = keyOut.getCopyOfBuffer();
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private static <T> TypeSerializer<T> castToType(@Nonnull TypeSerializer<?> serializer) {
        return (TypeSerializer<T>) serializer;
    }

    @Override
    public byte[] key() {
        return currentKey;
    }

    @Override
    public byte[] value() {
        return currentValue;
    }

    @Override
    public void close() {}
}
