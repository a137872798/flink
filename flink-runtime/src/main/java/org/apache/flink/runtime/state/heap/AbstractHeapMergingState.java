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

import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalMergingState;

import java.util.Collection;

/**
 * Base class for {@link MergingState} ({@link InternalMergingState}) that is stored on the heap.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <IN> The type of the input elements.
 * @param <SV> The type of the values in the state.
 * @param <OUT> The type of the output elements.
 *
 *             InternalMergingState 表示可以将多个源namespace下的state合并到目标namespace的state上
 */
abstract class AbstractHeapMergingState<K, N, IN, SV, OUT>
        extends AbstractHeapAppendingState<K, N, IN, SV, OUT>
        implements InternalMergingState<K, N, IN, SV, OUT> {

    /** The merge transformation function that implements the merge logic.
     * 用于将2个状态合并  具体的合并方法由子类实现
     * */
    private final MergeTransformation mergeTransformation;

    /**
     * Creates a new key/value state for the given hash map of key/value pairs.
     *
     * @param stateTable The state table for which this state is associated to.
     * @param keySerializer The serializer for the keys.
     * @param valueSerializer The serializer for the state.
     * @param namespaceSerializer The serializer for the namespace.
     * @param defaultValue The default value for the state.
     */
    protected AbstractHeapMergingState(
            StateTable<K, N, SV> stateTable,
            TypeSerializer<K> keySerializer,
            TypeSerializer<SV> valueSerializer,
            TypeSerializer<N> namespaceSerializer,
            SV defaultValue) {

        super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
        this.mergeTransformation = new MergeTransformation();
    }

    /**
     * 这个是InternalMergingState 的接口
     * @param target The target namespace where the merged state should be stored.
     * @param sources The source namespaces whose state should be merged.
     * @throws Exception
     */
    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        // 无数据 不需要处理
        if (sources == null || sources.isEmpty()) {
            return; // nothing to do
        }

        final StateTable<K, N, SV> map = stateTable;

        SV merged = null;

        // merge the sources
        for (N source : sources) {

            // get and remove the next source per namespace/key
            // 借助上下文信息以及ns 找到state
            SV sourceState = map.removeAndGetOld(source);

            // 看来要把source找到的所有state都进行合并
            if (merged != null && sourceState != null) {
                merged = mergeState(merged, sourceState);
            } else if (merged == null) {
                merged = sourceState;
            }
        }

        // merge into the target, if needed
        // 存入到目标ns中
        if (merged != null) {
            // 这里还会触发一次mergeState (借由mergeTransformation)
            map.transform(target, merged, mergeTransformation);
        }
    }

    protected abstract SV mergeState(SV a, SV b) throws Exception;

    /**
     * 将2个状态进行合并
     */
    final class MergeTransformation implements StateTransformationFunction<SV, SV> {

        @Override
        public SV apply(SV targetState, SV merged) throws Exception {
            if (targetState != null) {
                return mergeState(targetState, merged);
            } else {
                return merged;
            }
        }
    }
}
