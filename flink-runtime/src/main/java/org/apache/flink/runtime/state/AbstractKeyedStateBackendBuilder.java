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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Collection;

/** An abstract base implementation of the {@link StateBackendBuilder} interface.
 * 用于构建包含key的state的 状态存储后端
 * */
public abstract class AbstractKeyedStateBackendBuilder<K>
        implements StateBackendBuilder<AbstractKeyedStateBackend, BackendBuildingException> {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // 以下是需要的各种组件

    /**
     * 该对象用于注册state
     */
    protected final TaskKvStateRegistry kvStateRegistry;
    /**
     * 该对象提供序列化对象
     */
    protected final StateSerializerProvider<K> keySerializerProvider;
    protected final ClassLoader userCodeClassLoader;
    protected final int numberOfKeyGroups;
    /**
     * keyGroup算法需要的范围
     */
    protected final KeyGroupRange keyGroupRange;
    /**
     * 包含各种配置
     */
    protected final ExecutionConfig executionConfig;
    /**
     * 提供当前时间
     */
    protected final TtlTimeProvider ttlTimeProvider;
    /**
     * TODO
     */
    protected final LatencyTrackingStateConfig latencyTrackingStateConfig;

    /**
     * 为数据流包装压缩能力
     */
    protected final StreamCompressionDecorator keyGroupCompressionDecorator;

    /**
     * 简单理解是state的持有者
     */
    protected final Collection<KeyedStateHandle> restoreStateHandles;

    /**
     * 统一管理一些需要关闭的对象
     */
    protected final CloseableRegistry cancelStreamRegistry;

    public AbstractKeyedStateBackendBuilder(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            CloseableRegistry cancelStreamRegistry) {
        this.kvStateRegistry = kvStateRegistry;
        this.keySerializerProvider =
                StateSerializerProvider.fromNewRegisteredSerializer(keySerializer);
        this.userCodeClassLoader = userCodeClassLoader;
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.keyGroupRange = keyGroupRange;
        this.executionConfig = executionConfig;
        this.ttlTimeProvider = ttlTimeProvider;
        this.latencyTrackingStateConfig = latencyTrackingStateConfig;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.restoreStateHandles = stateHandles;
        this.cancelStreamRegistry = cancelStreamRegistry;
    }
}
