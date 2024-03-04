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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.util.Disposable;

import java.util.stream.Stream;

/**
 * A keyed state backend provides methods for managing keyed state.
 *
 * @param <K> The key by which state is keyed.
 *
 *           KeyedStateFactory 表示可以产生状态  从状态后端来看 产生应该就是读取吧?
 *           PriorityQueueSetFactory 产生优先队列
 *
 */
public interface KeyedStateBackend<K>
        extends KeyedStateFactory, PriorityQueueSetFactory, Disposable {

    /**
     * Sets the current key that is used for partitioned state.
     *
     * @param newKey The new current key.
     *               设置当前key
     */
    void setCurrentKey(K newKey);

    /** @return Current key.
     * 获取当前key
     * */
    K getCurrentKey();

    /** @return Serializer of the key.
     * 获取key的序列化对象
     * */
    TypeSerializer<K> getKeySerializer();

    /**
     * Applies the provided {@link KeyedStateFunction} to the state with the provided {@link
     * StateDescriptor} of all the currently active keys.
     *
     * @param namespace the namespace of the state.
     * @param namespaceSerializer the serializer for the namespace.
     * @param stateDescriptor the descriptor of the state to which the function is going to be
     *     applied.
     * @param function the function to be applied to the keyed state.
     * @param <N> The type of the namespace.
     * @param <S> The type of the state.
     *
     *           将函数作用在某state在该命名空间的所有key上
     */
    <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function)
            throws Exception;

    /**
     * @return A stream of all keys for the given state and namespace. Modifications to the state
     *     during iterating over it keys are not supported.
     * @param state State variable for which existing keys will be returned.
     * @param namespace Namespace for which existing keys will be returned.
     *
     *                  获取某个状态在某个命名空间下所有key
     */
    <N> Stream<K> getKeys(String state, N namespace);

    /**
     * @return A stream of all keys for the given state and namespace. Modifications to the state
     *     during iterating over it keys are not supported. Implementations go not make any ordering
     *     guarantees about the returned tupes. Two records with the same key or namespace may not
     *     be returned near each other in the stream.
     * @param state State variable for which existing keys will be returned.
     *              获取某个状态相关的所有命名空间 和 keys
     */
    <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state);

    /**
     * Creates or retrieves a keyed state backed by this state backend.
     *
     * @param namespaceSerializer The serializer used for the namespace type of the state
     * @param stateDescriptor The identifier for the state. This contains name and can create a
     *     default state value.
     * @param <N> The type of the namespace.
     * @param <S> The type of the state.
     * @return A new key/value state backed by this backend.
     * @throws Exception Exceptions may occur during initialization of the state and should be
     *     forwarded.
     *
     *     创建/获取状态
     */
    <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception;

    /**
     * Creates or retrieves a partitioned state backed by this state backend.
     *
     * <p>TODO: NOTE: This method does a lot of work caching / retrieving states just to update the
     * namespace. This method should be removed for the sake of namespaces being lazily fetched from
     * the keyed state backend, or being set on the state directly.
     *
     * @param stateDescriptor The identifier for the state. This contains name and can create a
     *     default state value.
     * @param <N> The type of the namespace.
     * @param <S> The type of the state.
     * @return A new key/value state backed by this backend.
     * @throws Exception Exceptions may occur during initialization of the state and should be
     *     forwarded.
     */
    <N, S extends State> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception;

    @Override
    void dispose();

    /**
     * State backend will call {@link KeySelectionListener#keySelected} when key context is switched
     * if supported.
     *
     * KeySelectionListener 会监听key的切换
     */
    void registerKeySelectionListener(KeySelectionListener<K> listener);

    /**
     * Stop calling listener registered in {@link #registerKeySelectionListener}.
     *
     * @return returns true iff listener was registered before.
     */
    boolean deregisterKeySelectionListener(KeySelectionListener<K> listener);

    @Deprecated
    default boolean isStateImmutableInStateBackend(CheckpointType checkpointOptions) {
        return false;
    }

    /**
     * Whether it's safe to reuse key-values from the state-backend, e.g for the purpose of
     * optimization.
     *
     * <p>NOTE: this method should not be used to check for {@link InternalPriorityQueue}, as the
     * priority queue could be stored on different locations, e.g RocksDB state-backend could store
     * that on JVM heap if configuring HEAP as the time-service factory.
     *
     * @return returns ture if safe to reuse the key-values from the state-backend.
     *
     * 默认情况下  重用状态被认为是不安全的
     */
    default boolean isSafeToReuseKVState() {
        return false;
    }

    /** Listener is given a callback when {@link #setCurrentKey} is called (key context changes). */
    @FunctionalInterface
    interface KeySelectionListener<K> {
        /** Callback when key context is switched. */
        void keySelected(K newKey);
    }
}
