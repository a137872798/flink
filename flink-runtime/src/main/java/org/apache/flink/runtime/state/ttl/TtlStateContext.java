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
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * 产生一个上下文对象 将相关信息维护起来
 * @param <T>
 * @param <SV>
 */
class TtlStateContext<T, SV> {
    /** Wrapped original state handler.
     * 被包裹的原始state 该对象将一些辅助组件全包含在内  便于其他模块引用和使用ttl
     * */
    final T original;

    final StateTtlConfig config;
    final TtlTimeProvider timeProvider;

    /** Serializer of original user stored value without timestamp.
     * 针对 userValue的序列化对象  不包含时间戳
     * */
    final TypeSerializer<SV> valueSerializer;

    /** This registered callback is to be called whenever state is accessed for read or write.
     * 当state被读写时执行的回调函数  目前就是 incrementalCleanup::stateAccessed
     * */
    final Runnable accessCallback;

    TtlStateContext(
            T original,
            StateTtlConfig config,
            TtlTimeProvider timeProvider,
            TypeSerializer<SV> valueSerializer,
            Runnable accessCallback) {
        this.original = original;
        this.config = config;
        this.timeProvider = timeProvider;
        this.valueSerializer = valueSerializer;
        this.accessCallback = accessCallback;
    }
}
