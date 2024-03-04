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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.CompositeSerializer;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;

/** This state factory wraps state objects, produced by backends, with TTL logic.
 * 该工厂可以给state附加ttl信息
 * */
public class TtlStateFactory<K, N, SV, TTLSV, S extends State, IS extends S> {
    public static <K, N, SV, TTLSV, S extends State, IS extends S>
            IS createStateAndWrapWithTtlIfEnabled(
                    TypeSerializer<N> namespaceSerializer,  // 用于序列化ns
                    StateDescriptor<S, SV> stateDesc,   // 该状态的描述
                    KeyedStateBackend<K> stateBackend,  // 存储状态的后端
                    TtlTimeProvider timeProvider)  // 提供state的过期时间
                    throws Exception {
        Preconditions.checkNotNull(namespaceSerializer);
        Preconditions.checkNotNull(stateDesc);
        Preconditions.checkNotNull(stateBackend);
        Preconditions.checkNotNull(timeProvider);
        return stateDesc.getTtlConfig().isEnabled()
                // 初始化工厂 并创建状态
                ? new TtlStateFactory<K, N, SV, TTLSV, S, IS>(
                                namespaceSerializer, stateDesc, stateBackend, timeProvider)
                        .createState()
                : stateBackend.createOrUpdateInternalState(namespaceSerializer, stateDesc);
    }

    private final Map<StateDescriptor.Type, SupplierWithException<IS, Exception>> stateFactories;

    @Nonnull private final TypeSerializer<N> namespaceSerializer;
    @Nonnull private final StateDescriptor<S, SV> stateDesc;
    @Nonnull private final KeyedStateBackend<K> stateBackend;
    @Nonnull private final StateTtlConfig ttlConfig;
    @Nonnull private final TtlTimeProvider timeProvider;
    private final long ttl;
    @Nullable private final TtlIncrementalCleanup<K, N, TTLSV> incrementalCleanup;

    private TtlStateFactory(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull KeyedStateBackend<K> stateBackend,
            @Nonnull TtlTimeProvider timeProvider) {
        this.namespaceSerializer = namespaceSerializer;
        this.stateDesc = stateDesc;
        this.stateBackend = stateBackend;
        this.ttlConfig = stateDesc.getTtlConfig();
        this.timeProvider = timeProvider;
        // 获取状态存活时间
        this.ttl = ttlConfig.getTtl().toMilliseconds();
        // 填充生成状态的工厂
        this.stateFactories = createStateFactories();
        // 产生清理 state过期数据的对象
        this.incrementalCleanup = getTtlIncrementalCleanup();
    }

    /**
     * 填充工厂
     * @return
     */
    private Map<StateDescriptor.Type, SupplierWithException<IS, Exception>> createStateFactories() {
        return Stream.of(
                // 根据state的不同类型 转发给不同的函数

                        Tuple2.of(
                                StateDescriptor.Type.VALUE,
                                (SupplierWithException<IS, Exception>) this::createValueState),
                        Tuple2.of(
                                StateDescriptor.Type.LIST,
                                (SupplierWithException<IS, Exception>) this::createListState),
                        Tuple2.of(
                                StateDescriptor.Type.MAP,
                                (SupplierWithException<IS, Exception>) this::createMapState),
                        Tuple2.of(
                                StateDescriptor.Type.REDUCING,
                                (SupplierWithException<IS, Exception>) this::createReducingState),
                        Tuple2.of(
                                StateDescriptor.Type.AGGREGATING,
                                (SupplierWithException<IS, Exception>)
                                        this::createAggregatingState))
                .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
    }

    /**
     * 包装状态
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private IS createState() throws Exception {
        SupplierWithException<IS, Exception> stateFactory = stateFactories.get(stateDesc.getType());
        // 可忽略 在初始化时已经填充了工厂了
        if (stateFactory == null) {
            String message =
                    String.format(
                            "State type: %s is not supported by %s",
                            stateDesc.getType(), TtlStateFactory.class);
            throw new FlinkRuntimeException(message);
        }

        // 通过工厂产生状态
        IS state = stateFactory.get();

        // 在返回状态时  也将状态交由cleanup管理
        if (incrementalCleanup != null) {
            incrementalCleanup.setTtlState((AbstractTtlState<K, N, ?, TTLSV, ?>) state);
        }
        return state;
    }

    @SuppressWarnings("unchecked")
    private IS createValueState() throws Exception {
        ValueStateDescriptor<TtlValue<SV>> ttlDescriptor =
                stateDesc.getSerializer() instanceof TtlSerializer  // 如果当前state相关的序列化对象已经包含了ttl字段了 就不需要额外加工
                        ? (ValueStateDescriptor<TtlValue<SV>>) stateDesc
                        : new ValueStateDescriptor<>(
                                stateDesc.getName(),
                                // 多使用一个 Long序列化对象 用于存储访问时间
                                new TtlSerializer<>(
                                        LongSerializer.INSTANCE, stateDesc.getSerializer()));
        return (IS) new TtlValueState<>(createTtlStateContext(ttlDescriptor));
    }

    /**
     * 创建list类型的state
     * @param <T>
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private <T> IS createListState() throws Exception {
        ListStateDescriptor<T> listStateDesc = (ListStateDescriptor<T>) stateDesc;
        ListStateDescriptor<TtlValue<T>> ttlDescriptor =
                listStateDesc.getElementSerializer() instanceof TtlSerializer
                        ? (ListStateDescriptor<TtlValue<T>>) stateDesc
                        : new ListStateDescriptor<>(
                                stateDesc.getName(),
                                new TtlSerializer<>(
                                        LongSerializer.INSTANCE,
                                        listStateDesc.getElementSerializer()));
        return (IS) new TtlListState<>(createTtlStateContext(ttlDescriptor));
    }

    /**
     * 获取map类型状态
     * @param <UK>
     * @param <UV>
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private <UK, UV> IS createMapState() throws Exception {
        MapStateDescriptor<UK, UV> mapStateDesc = (MapStateDescriptor<UK, UV>) stateDesc;
        MapStateDescriptor<UK, TtlValue<UV>> ttlDescriptor =
                mapStateDesc.getValueSerializer() instanceof TtlSerializer
                        ? (MapStateDescriptor<UK, TtlValue<UV>>) stateDesc
                        : new MapStateDescriptor<>(
                                stateDesc.getName(),
                                mapStateDesc.getKeySerializer(),
                                // key 不变 只修改value
                                new TtlSerializer<>(
                                        LongSerializer.INSTANCE,
                                        mapStateDesc.getValueSerializer()));
        return (IS) new TtlMapState<>(createTtlStateContext(ttlDescriptor));
    }

    /**
     * 处理reduce状态
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private IS createReducingState() throws Exception {
        ReducingStateDescriptor<SV> reducingStateDesc = (ReducingStateDescriptor<SV>) stateDesc;
        ReducingStateDescriptor<TtlValue<SV>> ttlDescriptor =
                new ReducingStateDescriptor<>(
                        stateDesc.getName(),
                        new TtlReduceFunction<>(
                                reducingStateDesc.getReduceFunction(), ttlConfig, timeProvider),
                        stateDesc.getSerializer() instanceof TtlSerializer
                                ? (TtlSerializer) stateDesc.getSerializer()
                                : new TtlSerializer<>(
                                        LongSerializer.INSTANCE, stateDesc.getSerializer()));
        return (IS) new TtlReducingState<>(createTtlStateContext(ttlDescriptor));
    }

    /**
     * 获取聚合状态
     * @param <IN>
     * @param <OUT>
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private <IN, OUT> IS createAggregatingState() throws Exception {
        AggregatingStateDescriptor<IN, SV, OUT> aggregatingStateDescriptor =
                (AggregatingStateDescriptor<IN, SV, OUT>) stateDesc;
        TtlAggregateFunction<IN, SV, OUT> ttlAggregateFunction =
                new TtlAggregateFunction<>(
                        aggregatingStateDescriptor.getAggregateFunction(), ttlConfig, timeProvider);
        AggregatingStateDescriptor<IN, TtlValue<SV>, OUT> ttlDescriptor =
                new AggregatingStateDescriptor<>(
                        stateDesc.getName(),
                        ttlAggregateFunction,
                        stateDesc.getSerializer() instanceof TtlSerializer
                                ? (TtlSerializer) stateDesc.getSerializer()
                                : new TtlSerializer<>(
                                        LongSerializer.INSTANCE, stateDesc.getSerializer()));
        return (IS)
                new TtlAggregatingState<>(
                        createTtlStateContext(ttlDescriptor), ttlAggregateFunction);
    }

    @SuppressWarnings("unchecked")
    private <OIS extends State, TTLS extends State, V, TTLV>
            TtlStateContext<OIS, V> createTtlStateContext(StateDescriptor<TTLS, TTLV> ttlDescriptor)
                    throws Exception {

        // 确保ttl有效
        ttlDescriptor.enableTimeToLive(
                stateDesc.getTtlConfig()); // also used by RocksDB backend for TTL compaction filter
        // config
        OIS originalState =
                // 该函数就是利用状态后端产生状态
                (OIS)
                        stateBackend.createOrUpdateInternalState(
                                namespaceSerializer, ttlDescriptor, getSnapshotTransformFactory());

        // 将赋予ttl能力的state 序列化对象 配置对象 等组件和在一起 变成context
        return new TtlStateContext<>(
                originalState,
                ttlConfig,
                timeProvider,
                (TypeSerializer<V>) stateDesc.getSerializer(),
                registerTtlIncrementalCleanupCallback((InternalKvState<?, ?, ?>) originalState));
    }

    /**
     * 产生清理对象
     * @return
     */
    private TtlIncrementalCleanup<K, N, TTLSV> getTtlIncrementalCleanup() {
        // 获取基于 IncrementalCleanup 进行状态清理的策略
        StateTtlConfig.IncrementalCleanupStrategy config =
                ttlConfig.getCleanupStrategies().getIncrementalCleanupStrategy();
        return config != null ? new TtlIncrementalCleanup<>(config.getCleanupSize()) : null;
    }

    /**
     * @param originalState
     * @return
     */
    private Runnable registerTtlIncrementalCleanupCallback(InternalKvState<?, ?, ?> originalState) {

        // 有关增量清理策略的相关配置值
        StateTtlConfig.IncrementalCleanupStrategy config =
                ttlConfig.getCleanupStrategies().getIncrementalCleanupStrategy();

        boolean cleanupConfigured = config != null && incrementalCleanup != null;

        // 判断state 是否允许遍历
        boolean isCleanupActive =
                cleanupConfigured
                        && isStateIteratorSupported(
                                originalState, incrementalCleanup.getCleanupSize());

        Runnable callback = isCleanupActive ? incrementalCleanup::stateAccessed : () -> {};

        // 表示每当key切换时  就触发 stateAccessed
        if (isCleanupActive && config.runCleanupForEveryRecord()) {
            stateBackend.registerKeySelectionListener(stub -> callback.run());
        }
        return callback;
    }

    /**
     * state是否支持迭代
     * @param originalState
     * @param size
     * @return
     */
    private boolean isStateIteratorSupported(InternalKvState<?, ?, ?> originalState, int size) {
        boolean stateIteratorSupported = false;
        try {
            // 支持访问者模式即可
            stateIteratorSupported = originalState.getStateIncrementalVisitor(size) != null;
        } catch (Throwable t) {
            // ignore
        }
        return stateIteratorSupported;
    }

    /**
     * 得到可以对state entries进行ttl检测的工厂对象
     * @return
     */
    private StateSnapshotTransformFactory<?> getSnapshotTransformFactory() {
        if (!ttlConfig.getCleanupStrategies().inFullSnapshot()) {
            return StateSnapshotTransformFactory.noTransform();
        } else {
            return new TtlStateSnapshotTransformer.Factory<>(timeProvider, ttl);
        }
    }

    /**
     * Serializer for user state value with TTL. Visibility is public for usage with external tools.
     * 表示通过该序列化对象产生的value 还会携带最后访问时间
     */
    public static class TtlSerializer<T> extends CompositeSerializer<TtlValue<T>> {
        private static final long serialVersionUID = 131020282727167064L;

        /**
         * 使用2个序列化对象 分别读取时间和数值
         * @param timestampSerializer
         * @param userValueSerializer
         */
        @SuppressWarnings("WeakerAccess")
        public TtlSerializer(
                TypeSerializer<Long> timestampSerializer, TypeSerializer<T> userValueSerializer) {
            super(true, timestampSerializer, userValueSerializer);
            // 要求用户value不是ttl类型的
            checkArgument(!(userValueSerializer instanceof TtlSerializer));
        }

        @SuppressWarnings("WeakerAccess")
        public TtlSerializer(
                PrecomputedParameters precomputed, TypeSerializer<?>... fieldSerializers) {
            super(precomputed, fieldSerializers);
        }

        /**
         * 构造对象时 包装出ttlValue
         * @param values
         * @return
         */
        @SuppressWarnings("unchecked")
        @Override
        public TtlValue<T> createInstance(@Nonnull Object... values) {
            Preconditions.checkArgument(values.length == 2);
            return new TtlValue<>((T) values[1], (long) values[0]);
        }

        @Override
        protected void setField(@Nonnull TtlValue<T> v, int index, Object fieldValue) {
            throw new UnsupportedOperationException("TtlValue is immutable");
        }

        @Override
        protected Object getField(@Nonnull TtlValue<T> v, int index) {
            return index == 0 ? v.getLastAccessTimestamp() : v.getUserValue();
        }

        /**
         * 产生ttlValue的序列化对象  是一个组合对象内部2个小的序列化对象分别用于时间和value
         * @param precomputed
         * @param originalSerializers
         * @return
         */
        @SuppressWarnings("unchecked")
        @Override
        protected CompositeSerializer<TtlValue<T>> createSerializerInstance(
                PrecomputedParameters precomputed, TypeSerializer<?>... originalSerializers) {
            Preconditions.checkNotNull(originalSerializers);
            Preconditions.checkArgument(originalSerializers.length == 2);
            return new TtlSerializer<>(precomputed, originalSerializers);
        }

        @SuppressWarnings("unchecked")
        TypeSerializer<Long> getTimestampSerializer() {
            return (TypeSerializer<Long>) (TypeSerializer<?>) fieldSerializers[0];
        }

        @SuppressWarnings("unchecked")
        TypeSerializer<T> getValueSerializer() {
            return (TypeSerializer<T>) fieldSerializers[1];
        }

        @Override
        public TypeSerializerSnapshot<TtlValue<T>> snapshotConfiguration() {
            return new TtlSerializerSnapshot<>(this);
        }

        /**
         * 判断该对象是否是ttl序列化对象
         * @param typeSerializer
         * @return
         */
        public static boolean isTtlStateSerializer(TypeSerializer<?> typeSerializer) {
            boolean ttlSerializer = typeSerializer instanceof TtlStateFactory.TtlSerializer;
            boolean ttlListSerializer =
                    typeSerializer instanceof ListSerializer
                            && ((ListSerializer) typeSerializer).getElementSerializer()
                                    instanceof TtlStateFactory.TtlSerializer;
            boolean ttlMapSerializer =
                    typeSerializer instanceof MapSerializer
                            && ((MapSerializer) typeSerializer).getValueSerializer()
                                    instanceof TtlStateFactory.TtlSerializer;
            return ttlSerializer || ttlListSerializer || ttlMapSerializer;
        }
    }

    /** A {@link TypeSerializerSnapshot} for TtlSerializer.
     * 快照对象描述如何恢复数值和序列化对象
     * */
    public static final class TtlSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<TtlValue<T>, TtlSerializer<T>> {

        private static final int VERSION = 2;

        @SuppressWarnings({"WeakerAccess", "unused"})
        public TtlSerializerSnapshot() {
            super(TtlSerializer.class);
        }

        TtlSerializerSnapshot(TtlSerializer<T> serializerInstance) {
            super(serializerInstance);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        /**
         * 返回内部2个序列化对象
         * @param outerSerializer the outer serializer.
         * @return
         */
        @Override
        protected TypeSerializer<?>[] getNestedSerializers(TtlSerializer<T> outerSerializer) {
            return new TypeSerializer[] {
                outerSerializer.getTimestampSerializer(), outerSerializer.getValueSerializer()
            };
        }

        @Override
        @SuppressWarnings("unchecked")
        protected TtlSerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            TypeSerializer<Long> timestampSerializer = (TypeSerializer<Long>) nestedSerializers[0];
            TypeSerializer<T> valueSerializer = (TypeSerializer<T>) nestedSerializers[1];

            return new TtlSerializer<>(timestampSerializer, valueSerializer);
        }
    }
}
