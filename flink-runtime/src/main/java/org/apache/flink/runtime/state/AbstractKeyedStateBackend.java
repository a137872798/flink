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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.InternalCheckpointListener;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateFactory;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base implementation of KeyedStateBackend. The state can be checkpointed to streams using {@link
 * #snapshot(long, long, CheckpointStreamFactory, CheckpointOptions)}.
 *
 * @param <K> Type of the key by which state is keyed.
 *
 *           状态后端骨架类   对应到一个task
 */
public abstract class AbstractKeyedStateBackend<K>
        implements CheckpointableKeyedStateBackend<K>,  // 表示该状态后端支持检查点
                InternalCheckpointListener,  // 监听检查点作业行为
                TestableKeyedStateBackend<K>,  // 可以解包装
                InternalKeyContext<K> {

    /** The key serializer.
     * 用于key的序列化
     * */
    protected final TypeSerializer<K> keySerializer;

    /** Listeners to changes of ({@link #keyContext}).
     * 维护监听器  负责监听当前key的变化
     * */
    private final ArrayList<KeySelectionListener<K>> keySelectionListeners;

    /** So that we can give out state when the user uses the same key.
     * 维护 name 与 kvState的映射关系
     * */
    private final HashMap<String, InternalKvState<K, ?, ?>> keyValueStatesByName;

    /** For caching the last accessed partitioned state.
     * 最近访问的状态名
     * */
    private String lastName;

    /**
     * 最近访问的状态
     */
    @SuppressWarnings("rawtypes")
    private InternalKvState lastState;

    /** The number of key-groups aka max parallelism. */
    protected final int numberOfKeyGroups;

    /** Range of key-groups for which this backend is responsible.
     * 维护的状态的 key范围
     * */
    protected final KeyGroupRange keyGroupRange;

    /** KvStateRegistry helper for this task.
     * 维护该task产生的所有state 并在后面维护一个大的 注册对象 管理所有task的
     * */
    protected final TaskKvStateRegistry kvStateRegistry;

    /**
     * Registry for all opened streams, so they can be closed if the task using this backend is
     * closed.
     *
     * 统一管理所有可关闭对象
     */
    protected CloseableRegistry cancelStreamRegistry;

    protected final ClassLoader userCodeClassLoader;

    /**
     * 包含各种配置
     */
    private final ExecutionConfig executionConfig;

    /**
     * 提供当前时间戳
     */
    protected final TtlTimeProvider ttlTimeProvider;

    /**
     * 统计相关的先忽略
     */
    protected final LatencyTrackingStateConfig latencyTrackingStateConfig;

    /** Decorates the input and output streams to write key-groups compressed. */
    protected final StreamCompressionDecorator keyGroupCompressionDecorator;

    /** The key context for this backend. */
    protected final InternalKeyContext<K> keyContext;

    public AbstractKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            InternalKeyContext<K> keyContext) {
        this(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                determineStreamCompression(executionConfig),
                keyContext);
    }

    public AbstractKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            InternalKeyContext<K> keyContext) {
        this(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                Preconditions.checkNotNull(keyContext),
                keyContext.getNumberOfKeyGroups(),
                keyContext.getKeyGroupRange(),
                new HashMap<>(),
                new ArrayList<>(1),
                null,
                null);
    }

    // Copy constructor
    protected AbstractKeyedStateBackend(AbstractKeyedStateBackend<K> abstractKeyedStateBackend) {
        this(
                abstractKeyedStateBackend.kvStateRegistry,
                abstractKeyedStateBackend.keySerializer,
                abstractKeyedStateBackend.userCodeClassLoader,
                abstractKeyedStateBackend.executionConfig,
                abstractKeyedStateBackend.ttlTimeProvider,
                abstractKeyedStateBackend.latencyTrackingStateConfig,
                abstractKeyedStateBackend.cancelStreamRegistry,
                abstractKeyedStateBackend.keyGroupCompressionDecorator,
                abstractKeyedStateBackend.keyContext,
                abstractKeyedStateBackend.numberOfKeyGroups,
                abstractKeyedStateBackend.keyGroupRange,
                abstractKeyedStateBackend.keyValueStatesByName,
                abstractKeyedStateBackend.keySelectionListeners,
                abstractKeyedStateBackend.lastState,
                abstractKeyedStateBackend.lastName);
    }

    @SuppressWarnings("rawtypes")
    private AbstractKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            InternalKeyContext<K> keyContext,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            HashMap<String, InternalKvState<K, ?, ?>> keyValueStatesByName,  // 在初始化时 就传入一定量的state
            ArrayList<KeySelectionListener<K>> keySelectionListeners,   // 这些选择器会感知key的变化 并产生一定操作
            InternalKvState lastState,
            String lastName) {
        this.keyContext = Preconditions.checkNotNull(keyContext);
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
        Preconditions.checkArgument(
                numberOfKeyGroups >= 1, "NumberOfKeyGroups must be a positive number");
        Preconditions.checkArgument(
                numberOfKeyGroups >= keyGroupRange.getNumberOfKeyGroups(),
                "The total number of key groups must be at least the number in the key group range assigned to this backend. "
                        + "The total number of key groups: %s, the number in key groups in range: %s",
                numberOfKeyGroups,
                keyGroupRange.getNumberOfKeyGroups());

        this.kvStateRegistry = kvStateRegistry;
        this.keySerializer = keySerializer;
        this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.keyValueStatesByName = keyValueStatesByName;
        this.executionConfig = executionConfig;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.ttlTimeProvider = Preconditions.checkNotNull(ttlTimeProvider);
        this.latencyTrackingStateConfig = Preconditions.checkNotNull(latencyTrackingStateConfig);
        this.keySelectionListeners = keySelectionListeners;
        this.lastState = lastState;
        this.lastName = lastName;
    }

    /**
     * 根据是否使用压缩算法  产生不同的包装对象
     * @param executionConfig
     * @return
     */
    private static StreamCompressionDecorator determineStreamCompression(
            ExecutionConfig executionConfig) {
        if (executionConfig != null && executionConfig.isUseSnapshotCompression()) {
            return SnappyStreamCompressionDecorator.INSTANCE;
        } else {
            return UncompressedStreamCompressionDecorator.INSTANCE;
        }
    }

    /**
     * 表示某个检查点被纳入
     * @param checkpointId The ID of the checkpoint that has been subsumed.
     * @throws Exception
     */
    @Override
    public void notifyCheckpointSubsumed(long checkpointId) throws Exception {}

    /**
     * Closes the state backend, releasing all internal resources, but does not delete any
     * persistent checkpoint data.
     * 丢弃本任务关联的所有state
     */
    @Override
    public void dispose() {

        IOUtils.closeQuietly(cancelStreamRegistry);

        if (kvStateRegistry != null) {
            kvStateRegistry.unregisterAll();
        }

        lastName = null;
        lastState = null;

        // 将name通往state的索引信息清除
        keyValueStatesByName.clear();
    }

    /** @see KeyedStateBackend
     * 更新当前访问的key   key用于keyGroup算法  能够定位到一个槽
     * */
    @Override
    public void setCurrentKey(K newKey) {
        notifyKeySelected(newKey);
        this.keyContext.setCurrentKey(newKey);

        // 计算当前key对应的keyGroup下标   (通过计算key的hashCode值)
        this.keyContext.setCurrentKeyGroupIndex(
                KeyGroupRangeAssignment.assignToKeyGroup(newKey, numberOfKeyGroups));
    }

    /**
     * 通知监听器 当前key发生了变化
     * @param newKey
     */
    private void notifyKeySelected(K newKey) {
        // we prefer a for-loop over other iteration schemes for performance reasons here.
        for (int i = 0; i < keySelectionListeners.size(); ++i) {
            keySelectionListeners.get(i).keySelected(newKey);
        }
    }

    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        keySelectionListeners.add(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return keySelectionListeners.remove(listener);
    }

    /** @see KeyedStateBackend */
    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    /** @see KeyedStateBackend */
    @Override
    public K getCurrentKey() {
        return this.keyContext.getCurrentKey();
    }

    /** @see KeyedStateBackend */
    public int getCurrentKeyGroupIndex() {
        return this.keyContext.getCurrentKeyGroupIndex();
    }

    /** @see KeyedStateBackend */
    public int getNumberOfKeyGroups() {
        return numberOfKeyGroups;
    }

    /** @see KeyedStateBackend */
    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }


    /**
     * 将函数作用到某个ns下所有状态
     */
    /** @see KeyedStateBackend */
    @Override
    public <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function)
            throws Exception {

        applyToAllKeys(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                function,
                this::getPartitionedState);
    }

    public <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function,  // 该函数会作用到state的某个key下的所有entries
            final PartitionStateFactory partitionStateFactory)
            throws Exception {

        // 找到某个state某个ns下所有key
        try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

            // 因为这是一个大的状态存储  所以 stateName和ns能够帮助定位到某个state
            final S state =
                    partitionStateFactory.get(namespace, namespaceSerializer, stateDescriptor);

            keyStream.forEach(
                    (K key) -> {
                        setCurrentKey(key);
                        try {
                            function.process(key, state);
                        } catch (Throwable e) {
                            // we wrap the checked exception in an unchecked
                            // one and catch it (and re-throw it) later.
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    /** @see KeyedStateBackend
     * 获取state  如果不存在则创建
     * */
    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends State, V> S getOrCreateKeyedState(
            final TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor)
            throws Exception {
        checkNotNull(namespaceSerializer, "Namespace serializer");
        checkNotNull(
                keySerializer,
                "State key serializer has not been configured in the config. "
                        + "This operation cannot use partitioned state.");

        InternalKvState<K, ?, ?> kvState = keyValueStatesByName.get(stateDescriptor.getName());

        // 准备创建state   这里只是一个模版 主要创建还是依赖 createOrUpdateInternalState
        if (kvState == null) {
            // 表示序列化还没有被初始化
            if (!stateDescriptor.isSerializerInitialized()) {
                // 基于config产生序列化对象
                stateDescriptor.initializeSerializerUnlessSet(executionConfig);
            }
            // 先忽略测量相关的 应该是加上一层包装
            kvState =
                    LatencyTrackingStateFactory.createStateAndWrapWithLatencyTrackingIfEnabled(
                            // 该方法内部会调用 createOrUpdateInternalState 由子类实现 完成state的创建
                            TtlStateFactory.createStateAndWrapWithTtlIfEnabled(
                                    namespaceSerializer, stateDescriptor, this, ttlTimeProvider),
                            stateDescriptor,
                            latencyTrackingStateConfig);
            keyValueStatesByName.put(stateDescriptor.getName(), kvState);
            // 因为添加了一个新的state 要对外暴露 使得可以被查询
            publishQueryableStateIfEnabled(stateDescriptor, kvState);
        }
        return (S) kvState;
    }

    /**
     * 将某个状态暴露出来
     * @param stateDescriptor
     * @param kvState
     */
    public void publishQueryableStateIfEnabled(
            StateDescriptor<?, ?> stateDescriptor, InternalKvState<?, ?, ?> kvState) {
        if (stateDescriptor.isQueryable()) {
            if (kvStateRegistry == null) {
                throw new IllegalStateException("State backend has not been initialized for job.");
            }
            String name = stateDescriptor.getQueryableStateName();
            kvStateRegistry.registerKvState(keyGroupRange, name, kvState, userCodeClassLoader);
        }
    }

    /**
     * TODO: NOTE: This method does a lot of work caching / retrieving states just to update the
     * namespace. This method should be removed for the sake of namespaces being lazily fetched from
     * the keyed state backend, or being set on the state directly.
     *
     * @see KeyedStateBackend
     * 更新当前选择的状态
     */
    @SuppressWarnings("unchecked")
    @Override
    public <N, S extends State> S getPartitionedState(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, ?> stateDescriptor)
            throws Exception {

        checkNotNull(namespace, "Namespace");

        // 还是使用同一个状态的数据 直接切换namespace即可
        if (lastName != null && lastName.equals(stateDescriptor.getName())) {
            lastState.setCurrentNamespace(namespace);
            return (S) lastState;
        }

        // 通过名字定位到state  然后切换ns
        InternalKvState<K, ?, ?> previous = keyValueStatesByName.get(stateDescriptor.getName());
        if (previous != null) {
            lastState = previous;
            lastState.setCurrentNamespace(namespace);
            lastName = stateDescriptor.getName();
            return (S) previous;
        }

        // 表示此时state还不存在
        final S state = getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        final InternalKvState<K, N, ?> kvState = (InternalKvState<K, N, ?>) state;

        lastName = stateDescriptor.getName();
        lastState = kvState;
        kvState.setCurrentNamespace(namespace);

        return state;
    }

    @Override
    public void close() throws IOException {
        cancelStreamRegistry.close();
    }

    public LatencyTrackingStateConfig getLatencyTrackingStateConfig() {
        return latencyTrackingStateConfig;
    }

    @VisibleForTesting
    public StreamCompressionDecorator getKeyGroupCompressionDecorator() {
        return keyGroupCompressionDecorator;
    }

    @VisibleForTesting
    public int numKeyValueStatesByName() {
        return keyValueStatesByName.size();
    }

    // TODO remove this once heap-based timers are working with RocksDB incremental snapshots!
    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        return false;
    }

    public InternalKeyContext<K> getKeyContext() {
        return keyContext;
    }

    /**
     * 获取des描述的状态在指定ns下的state
     */
    public interface PartitionStateFactory {
        <N, S extends State> S get(
                final N namespace,
                final TypeSerializer<N> namespaceSerializer,
                final StateDescriptor<S, ?> stateDescriptor)
                throws Exception;
    }

    @Override
    public void setCurrentKeyGroupIndex(int currentKeyGroupIndex) {
        keyContext.setCurrentKeyGroupIndex(currentKeyGroupIndex);
    }
}
