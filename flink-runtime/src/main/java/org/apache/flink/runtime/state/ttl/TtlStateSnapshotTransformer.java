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

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;

/** State snapshot filter of expired values with TTL.
 * 在ttl模块使用 对集合类型的状态进行过滤和转换
 * */
abstract class TtlStateSnapshotTransformer<T> implements CollectionStateSnapshotTransformer<T> {

    /**
     * 提供当前时间
     */
    private final TtlTimeProvider ttlTimeProvider;
    final long ttl;
    private final DataInputDeserializer div;

    TtlStateSnapshotTransformer(@Nonnull TtlTimeProvider ttlTimeProvider, long ttl) {
        this.ttlTimeProvider = ttlTimeProvider;
        this.ttl = ttl;
        this.div = new DataInputDeserializer();
    }

    /**
     * 在处理value时  判断是否要过滤(是否过期)
     * 如果超时了 就会返回null
     * @param value
     * @param <V>
     * @return
     */
    <V> TtlValue<V> filterTtlValue(TtlValue<V> value) {
        return expired(value) ? null : value;
    }

    /**
     * 比较最后访问时间和当前时间
     * @param ttlValue
     * @return
     */
    private boolean expired(TtlValue<?> ttlValue) {
        return expired(ttlValue.getLastAccessTimestamp());
    }

    boolean expired(long ts) {
        return TtlUtils.expired(ts, ttl, ttlTimeProvider);
    }

    /**
     * 反序列化时间信息
     * @param value
     * @return
     * @throws IOException
     */
    long deserializeTs(byte[] value) throws IOException {
        div.setBuffer(value, 0, Long.BYTES);
        // 读取buf 也就是 value 并产生一个long
        return LongSerializer.INSTANCE.deserialize(div);
    }

    /**
     * 表示当处理到第一个非空entries时 就可以停止扫描了
     * 针对ttl的场景 如果是按照时间顺序排序 确实一旦发现有效的就可以停止扫描了
     * @return
     */
    @Override
    public TransformStrategy getFilterStrategy() {
        return TransformStrategy.STOP_ON_FIRST_INCLUDED;
    }

    // 2种转换器 分别对应从字节流反序列化 和 从ttl对象序列化

    static class TtlDeserializedValueStateSnapshotTransformer<T>
            extends TtlStateSnapshotTransformer<TtlValue<T>> {
        TtlDeserializedValueStateSnapshotTransformer(TtlTimeProvider ttlTimeProvider, long ttl) {
            super(ttlTimeProvider, ttl);
        }

        @Override
        @Nullable
        public TtlValue<T> filterOrTransform(@Nullable TtlValue<T> value) {
            return filterTtlValue(value);
        }
    }

    static class TtlSerializedValueStateSnapshotTransformer
            extends TtlStateSnapshotTransformer<byte[]> {
        TtlSerializedValueStateSnapshotTransformer(TtlTimeProvider ttlTimeProvider, long ttl) {
            super(ttlTimeProvider, ttl);
        }

        @Override
        @Nullable
        public byte[] filterOrTransform(@Nullable byte[] value) {
            if (value == null) {
                return null;
            }
            long ts;
            try {
                // 多了一步反序列化
                ts = deserializeTs(value);
            } catch (IOException e) {
                throw new FlinkRuntimeException("Unexpected timestamp deserialization failure", e);
            }
            return expired(ts) ? null : value;
        }
    }

    /**
     * 快照  表示可以从快照恢复对象
     * @param <T>
     */
    static class Factory<T> implements StateSnapshotTransformFactory<TtlValue<T>> {
        private final TtlTimeProvider ttlTimeProvider;
        private final long ttl;

        Factory(@Nonnull TtlTimeProvider ttlTimeProvider, long ttl) {
            this.ttlTimeProvider = ttlTimeProvider;
            this.ttl = ttl;
        }

        // 根据需要创建不同的对象

        @Override
        public Optional<StateSnapshotTransformer<TtlValue<T>>> createForDeserializedState() {
            return Optional.of(
                    new TtlDeserializedValueStateSnapshotTransformer<>(ttlTimeProvider, ttl));
        }

        @Override
        public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
            return Optional.of(
                    new TtlSerializedValueStateSnapshotTransformer(ttlTimeProvider, ttl));
        }
    }
}
