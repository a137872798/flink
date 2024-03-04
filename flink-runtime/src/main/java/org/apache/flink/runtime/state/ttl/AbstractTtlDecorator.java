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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

/**
 * Base class for TTL logic wrappers.
 *
 * @param <T> Type of originally wrapped object
 *
 *           表示普通state的ttl包装层
 */
abstract class AbstractTtlDecorator<T> {
    /** Wrapped original state handler.
     * 普通state
     * */
    final T original;

    final StateTtlConfig config;

    final TtlTimeProvider timeProvider;

    /** Whether to renew expiration timestamp on state read access.
     * 读取该state时 是否更新访问时间 这将影响到是否淘汰数据
     * */
    final boolean updateTsOnRead;

    /** Whether to renew expiration timestamp on state read access.
     * 表示在还未清理前 允许返回过期值
     * */
    final boolean returnExpired;

    /** State value time to live in milliseconds.
     * state的存活时间戳
     * */
    final long ttl;

    AbstractTtlDecorator(T original, StateTtlConfig config, TtlTimeProvider timeProvider) {
        Preconditions.checkNotNull(original);
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(timeProvider);
        this.original = original;
        this.config = config;
        this.timeProvider = timeProvider;
        this.updateTsOnRead = config.getUpdateType() == StateTtlConfig.UpdateType.OnReadAndWrite;
        this.returnExpired =
                config.getStateVisibility()
                        == StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp;
        this.ttl = config.getTtl().toMilliseconds();
    }

    /**
     * 使用该函数会考虑returnExpired
     * @param ttlValue
     * @param <V>
     * @return
     */
    <V> V getUnexpired(TtlValue<V> ttlValue) {
        // 本身为null 或者过期 并且不允许返回过期值时返回null     否则返回数值
        return ttlValue == null || (!returnExpired && expired(ttlValue))
                ? null
                : ttlValue.getUserValue();
    }

    /**
     * 判断数值是否过期
     * @param ttlValue
     * @param <V>
     * @return
     */
    <V> boolean expired(TtlValue<V> ttlValue) {
        return TtlUtils.expired(ttlValue, ttl, timeProvider);
    }

    /**
     * 赋予value timeProvider提供的时间
     * @param value
     * @param <V>
     * @return
     */
    <V> TtlValue<V> wrapWithTs(V value) {
        return TtlUtils.wrapWithTs(value, timeProvider.currentTimestamp());
    }

    /**
     * 更新时间
     * @param ttlValue
     * @param <V>
     * @return
     */
    <V> TtlValue<V> rewrapWithNewTs(TtlValue<V> ttlValue) {
        return wrapWithTs(ttlValue.getUserValue());
    }

    /**
     * 仅返回userValue 不包含ttl
     * @param getter
     * @param updater
     * @param stateClear
     * @param <SE>
     * @param <CE>
     * @param <CLE>
     * @param <V>
     * @return
     * @throws SE
     * @throws CE
     * @throws CLE
     */
    <SE extends Throwable, CE extends Throwable, CLE extends Throwable, V>
            V getWithTtlCheckAndUpdate(
                    SupplierWithException<TtlValue<V>, SE> getter,
                    ThrowingConsumer<TtlValue<V>, CE> updater,
                    ThrowingRunnable<CLE> stateClear)
                    throws SE, CE, CLE {
        TtlValue<V> ttlValue = getWrappedWithTtlCheckAndUpdate(getter, updater, stateClear);
        return ttlValue == null ? null : ttlValue.getUserValue();
    }

    /**
     * 通过一系列逻辑链 得到一个 ttlValue
     * @param getter
     * @param updater
     * @param stateClear
     * @param <SE>
     * @param <CE>
     * @param <CLE>
     * @param <V>
     * @return
     * @throws SE
     * @throws CE
     * @throws CLE
     */
    <SE extends Throwable, CE extends Throwable, CLE extends Throwable, V>
            TtlValue<V> getWrappedWithTtlCheckAndUpdate(
                    SupplierWithException<TtlValue<V>, SE> getter,
                    ThrowingConsumer<TtlValue<V>, CE> updater,
                    ThrowingRunnable<CLE> stateClear)
                    throws SE, CE, CLE {

        // 先调用supplier函数
        TtlValue<V> ttlValue = getter.get();
        if (ttlValue == null) {
            return null;
            // 可以拿到值的情况下 判断是否过期
        } else if (expired(ttlValue)) {
            // 过期的话 执行函数进行清理
            stateClear.run();
            // 不允许返回过期值  则返回null
            if (!returnExpired) {
                return null;
            }
            // 使用更新函数  更新value的访问时间
        } else if (updateTsOnRead) {
            updater.accept(rewrapWithNewTs(ttlValue));
        }
        return ttlValue;
    }
}
