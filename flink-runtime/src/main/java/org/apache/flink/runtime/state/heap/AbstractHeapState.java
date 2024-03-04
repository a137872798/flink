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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

/**
 * Base class for partitioned {@link State} implementations that are backed by a regular heap hash
 * map. The concrete implementations define how the state is checkpointed.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of the values in the state.
 *
 *            InternalKvState开放api 可以通过namespace和key 查询value 还可以用visitor查看entry
 *            AbstractHeapState 表示利用内存来存储state
 *
 *            作为state的基类  然后根据不同的state类型 又分化出ValueState/MapState等类型
 */
public abstract class AbstractHeapState<K, N, SV> implements InternalKvState<K, N, SV> {

    /** Map containing the actual key/value pairs.
     * StateTable利用 keyGroup对数据先进行一次散列 然后分配到同一个key的数据 又使用 StateMap进行存储
     * StateMap 存储数据的容器(也是手写了一个hash桶)  每个元素还携带版本号
     * 该数据结构存储的state 都是以kv形式展现的  并且多了一个namespace
     * */
    protected final StateTable<K, N, SV> stateTable;

    /** The current namespace, which the access methods will refer to.
     * 当前使用的命名空间
     * */
    protected N currentNamespace;

    // 各自的序列化对象

    protected final TypeSerializer<K> keySerializer;

    protected TypeSerializer<SV> valueSerializer;

    protected TypeSerializer<N> namespaceSerializer;

    /**
     * 应该是state的默认值
     */
    private SV defaultValue;

    /**
     * Creates a new key/value state for the given hash map of key/value pairs.
     *
     * @param stateTable The state table for which this state is associated to.
     * @param keySerializer The serializer for the keys.
     * @param valueSerializer The serializer for the state.
     * @param namespaceSerializer The serializer for the namespace.
     * @param defaultValue The default value for the state.
     *                     使用相关组件进行初始化
     */
    AbstractHeapState(
            StateTable<K, N, SV> stateTable,
            TypeSerializer<K> keySerializer,
            TypeSerializer<SV> valueSerializer,
            TypeSerializer<N> namespaceSerializer,
            SV defaultValue) {

        this.stateTable = Preconditions.checkNotNull(stateTable, "State table must not be null.");
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.defaultValue = defaultValue;
        this.currentNamespace = null;
    }

    // ------------------------------------------------------------------------

    /**
     * 配合当前上下文的信息删除ns的数据
     */
    @Override
    public final void clear() {
        stateTable.remove(currentNamespace);
    }

    @Override
    public final void setCurrentNamespace(N namespace) {
        this.currentNamespace =
                Preconditions.checkNotNull(namespace, "Namespace must not be null.");
    }

    /**
     * 利用序列化对象 解析数据 并得到value
     * @param serializedKeyAndNamespace Serialized key and namespace
     * @param safeKeySerializer A key serializer which is safe to be used even in multi-threaded
     *     context
     * @param safeNamespaceSerializer A namespace serializer which is safe to be used even in
     *     multi-threaded context
     * @param safeValueSerializer A value serializer which is safe to be used even in multi-threaded
     *     context
     * @return
     * @throws Exception
     */
    @Override
    public byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<SV> safeValueSerializer)
            throws Exception {

        Preconditions.checkNotNull(serializedKeyAndNamespace);
        Preconditions.checkNotNull(safeKeySerializer);
        Preconditions.checkNotNull(safeNamespaceSerializer);
        Preconditions.checkNotNull(safeValueSerializer);

        // 读出了key和ns
        Tuple2<K, N> keyAndNamespace =
                KvStateSerializer.deserializeKeyAndNamespace(
                        serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer);

        // 使用他们查询value
        SV result = stateTable.get(keyAndNamespace.f0, keyAndNamespace.f1);

        if (result == null) {
            return null;
        }
        // 对解析进行序列化处理
        return KvStateSerializer.serializeValue(result, safeValueSerializer);
    }

    /** This should only be used for testing. */
    @VisibleForTesting
    public StateTable<K, N, SV> getStateTable() {
        return stateTable;
    }

    /**
     * 返回默认值
     * @return
     */
    protected SV getDefaultValue() {
        if (defaultValue != null) {
            return valueSerializer.copy(defaultValue);
        } else {
            return null;
        }
    }

    protected AbstractHeapState<K, N, SV> setNamespaceSerializer(
            TypeSerializer<N> namespaceSerializer) {
        this.namespaceSerializer = namespaceSerializer;
        return this;
    }

    protected AbstractHeapState<K, N, SV> setValueSerializer(TypeSerializer<SV> valueSerializer) {
        this.valueSerializer = valueSerializer;
        return this;
    }

    protected AbstractHeapState<K, N, SV> setDefaultValue(SV defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    @Override
    public StateIncrementalVisitor<K, N, SV> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return stateTable.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }
}
