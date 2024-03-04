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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.internal.InternalKvState;

import javax.annotation.Nonnull;

/** This factory produces concrete internal state objects.
 * 这个工厂可以产生状态对象
 * */
public interface KeyedStateFactory {

    /**
     * Creates or updates internal state and returns a new {@link InternalKvState}.
     *
     * @param namespaceSerializer TypeSerializer for the state namespace.
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     * @param <N> The type of the namespace.
     * @param <SV> The type of the stored state value.
     * @param <S> The type of the public API state.
     * @param <IS> The type of internal state.
     */
    @Nonnull
    default <N, SV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc)
            throws Exception {
        return createOrUpdateInternalState(
                namespaceSerializer, stateDesc, StateSnapshotTransformFactory.noTransform());
    }

    /**
     * Creates or updates internal state and returns a new {@link InternalKvState}.
     *
     * @param namespaceSerializer TypeSerializer for the state namespace.
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     * @param snapshotTransformFactory factory of state snapshot transformer.  默认转换器为 noTransform()
     * @param <N> The type of the namespace.
     * @param <SV> The type of the stored state value.
     * @param <SEV> The type of the stored state value or entry for collection types (list or map).
     * @param <S> The type of the public API state.
     * @param <IS> The type of internal state.
     *
     *            snapshotTransformFactory 表示允许传入一个工厂 之后在通过快照进行恢复的时候 可以用转换函数处理entries
     */
    @Nonnull
    <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception;

    /**
     * Creates or updates internal state and returns a new {@link InternalKvState}.
     *
     * @param namespaceSerializer TypeSerializer for the state namespace.
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     * @param snapshotTransformFactory factory of state snapshot transformer.
     * @param allowFutureMetadataUpdates whether allow metadata to update in the future or not.   表示是否允许元数据在以后发生变化
     * @param <N> The type of the namespace.
     * @param <SV> The type of the stored state value.
     * @param <SEV> The type of the stored state value or entry for collection types (list or map).
     * @param <S> The type of the public API state.
     * @param <IS> The type of internal state.
     */
    @Nonnull
    default <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {

        // 默认不允许变化
        if (allowFutureMetadataUpdates) {
            throw new UnsupportedOperationException(
                    this.getClass().getName() + "doesn't support to allow future metadata update");
        } else {
            return createOrUpdateInternalState(
                    namespaceSerializer, stateDesc, snapshotTransformFactory);
        }
    }
}
