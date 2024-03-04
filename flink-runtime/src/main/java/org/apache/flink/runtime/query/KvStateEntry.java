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

package org.apache.flink.runtime.query;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * An entry holding the {@link InternalKvState} along with its {@link KvStateInfo}.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace the state is associated to
 * @param <V> The type of values kept internally in state
 *
 *           将kv状态对象 与 类加载器关联起来
 */
@Internal
public class KvStateEntry<K, N, V> {

    private final InternalKvState<K, N, V> state;
    private final KvStateInfo<K, N, V> stateInfo;

    /**
     * 表示序列化对象是无状态的
     */
    private final boolean areSerializersStateless;

    private final ConcurrentMap<Thread, KvStateInfo<K, N, V>> serializerCache;

    private final ClassLoader userClassLoader;

    public KvStateEntry(final InternalKvState<K, N, V> state, ClassLoader userClassLoader) {
        this.state = Preconditions.checkNotNull(state);
        this.stateInfo =
                new KvStateInfo<>(
                        state.getKeySerializer(),
                        state.getNamespaceSerializer(),
                        state.getValueSerializer());
        this.serializerCache = new ConcurrentHashMap<>();
        this.userClassLoader = userClassLoader;

        this.areSerializersStateless = stateInfo.duplicate() == stateInfo;
    }

    public InternalKvState<K, N, V> getState() {
        return state;
    }

    public ClassLoader getUserClassLoader() {
        return userClassLoader;
    }

    public KvStateInfo<K, N, V> getInfoForCurrentThread() {
        return areSerializersStateless
                ? stateInfo  // 因为无状态 可以复用
                : serializerCache.computeIfAbsent(
                        Thread.currentThread(), t -> stateInfo.duplicate());  // 否则每个线程维护一个副本
    }

    public void clear() {
        serializerCache.clear();
    }

    @VisibleForTesting
    public int getCacheSize() {
        return serializerCache.size();
    }
}
