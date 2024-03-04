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

package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Generalized snapshot for meta information about one state in a state backend (e.g. {@link
 * RegisteredKeyValueStateBackendMetaInfo}).
 *
 * 关于状态元数据的快照
 */
public class StateMetaInfoSnapshot {

    /** Enum that defines the different types of state that live in Flink backends.
     * 表示状态存储的形式
     * */
    public enum BackendStateType {
        KEY_VALUE(0),  // 外层是个大Map
        OPERATOR(1),
        BROADCAST(2),
        PRIORITY_QUEUE(3);  // 外层是个优先队列
        private final byte code;

        BackendStateType(int code) {
            this.code = (byte) code;
        }

        public byte getCode() {
            return code;
        }

        public static BackendStateType byCode(int code) {
            for (BackendStateType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown BackendStateType: " + code);
        }
    }

    /** Predefined keys for the most common options in the meta info. */
    public enum CommonOptionsKeys {
        /** Key to define the {@link StateDescriptor.Type} of a key/value keyed-state */
        KEYED_STATE_TYPE,
        /**
         * Key to define {@link org.apache.flink.runtime.state.OperatorStateHandle.Mode}, about how
         * operator state is distributed on restore
         */
        OPERATOR_STATE_DISTRIBUTION_MODE,
    }

    /** Predefined keys for the most common serializer types in the meta info. */
    public enum CommonSerializerKeys {
        KEY_SERIALIZER,
        NAMESPACE_SERIALIZER,
        VALUE_SERIALIZER
    }

    /** The name of the state.
     * 状态名字
     * */
    @Nonnull private final String name;

    /**
     * 使用的状态后端的类型
     */
    @Nonnull private final BackendStateType backendStateType;

    /** Map of options (encoded as strings) for the state.
     * 该状态使用的选项
     * */
    @Nonnull private final Map<String, String> options;

    /** The configurations of all the type serializers used with the state.
     * 需要制作快照的内容 以及包含逻辑的快照对象
     * */
    @Nonnull private final Map<String, TypeSerializerSnapshot<?>> serializerSnapshots;

    // TODO this will go away once all serializers have the restoreSerializer() factory method
    // properly implemented.
    /** The serializers used by the state.
     * 该状态可能需要的各个字段 以及序列化对象   状态可能是个复杂对象
     * */
    @Nonnull private final Map<String, TypeSerializer<?>> serializers;

    public StateMetaInfoSnapshot(
            @Nonnull String name,
            @Nonnull BackendStateType backendStateType,
            @Nonnull Map<String, String> options,
            @Nonnull Map<String, TypeSerializerSnapshot<?>> serializerSnapshots) {
        this(name, backendStateType, options, serializerSnapshots, new HashMap<>());
    }

    /**
     * TODO this variant, which requires providing the serializers, TODO should actually be removed,
     * leaving only {@link #StateMetaInfoSnapshot(String, BackendStateType, Map, Map)}. TODO This is
     * still used by snapshot extracting methods (i.e. computeSnapshot() method of specific state
     * meta TODO info subclasses), and will be removed once all serializers have the
     * restoreSerializer() factory method implemented.
     */
    public StateMetaInfoSnapshot(
            @Nonnull String name,
            @Nonnull BackendStateType backendStateType,
            @Nonnull Map<String, String> options,
            @Nonnull Map<String, TypeSerializerSnapshot<?>> serializerSnapshots,
            @Nonnull Map<String, TypeSerializer<?>> serializers) {
        this.name = name;
        this.backendStateType = backendStateType;
        this.options = options;
        this.serializerSnapshots = serializerSnapshots;
        this.serializers = serializers;
    }

    @Nonnull
    public BackendStateType getBackendStateType() {
        return backendStateType;
    }

    @Nullable
    public TypeSerializerSnapshot<?> getTypeSerializerSnapshot(@Nonnull String key) {
        return serializerSnapshots.get(key);
    }

    @Nullable
    public TypeSerializerSnapshot<?> getTypeSerializerSnapshot(@Nonnull CommonSerializerKeys key) {
        return getTypeSerializerSnapshot(key.toString());
    }

    @Nullable
    public String getOption(@Nonnull String key) {
        return options.get(key);
    }

    @Nullable
    public String getOption(@Nonnull StateMetaInfoSnapshot.CommonOptionsKeys key) {
        return getOption(key.toString());
    }

    @Nonnull
    public Map<String, String> getOptionsImmutable() {
        return Collections.unmodifiableMap(options);
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nonnull
    public Map<String, TypeSerializerSnapshot<?>> getSerializerSnapshotsImmutable() {
        return Collections.unmodifiableMap(serializerSnapshots);
    }

    /** TODO this method should be removed once the serializer map is removed. */
    @Nullable
    public TypeSerializer<?> getTypeSerializer(@Nonnull String key) {
        return serializers.get(key);
    }
}
