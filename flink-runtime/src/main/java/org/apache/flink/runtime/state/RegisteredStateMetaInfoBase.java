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

import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnull;

/** Base class for all registered state in state backends.
 * 表示已经被注册到状态后端的状态
 * */
public abstract class RegisteredStateMetaInfoBase {

    /** The name of the state */
    @Nonnull protected final String name;

    public RegisteredStateMetaInfoBase(@Nonnull String name) {
        this.name = name;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    /**
     * 快照的元数据对象
     * @return
     */
    @Nonnull
    public abstract StateMetaInfoSnapshot snapshot();

    /**
     * create a new metadata object with Lazy serializer provider using existing one as a snapshot.
     * Sometimes metadata was just created or updated, but its StateSerializerProvider will not
     * allow further updates. So this method could replace it with a new one that contains a fresh
     * LazilyRegisteredStateSerializerProvider.
     */
    @Nonnull
    public abstract RegisteredStateMetaInfoBase withSerializerUpgradesAllowed();

    /**
     * 通过快照的元数据对象 产生本对象
     * @param snapshot
     * @return
     */
    public static RegisteredStateMetaInfoBase fromMetaInfoSnapshot(
            @Nonnull StateMetaInfoSnapshot snapshot) {

        final StateMetaInfoSnapshot.BackendStateType backendStateType =
                snapshot.getBackendStateType();
        // 根据后端类型产生不同对象
        switch (backendStateType) {
            case KEY_VALUE:
                return new RegisteredKeyValueStateBackendMetaInfo<>(snapshot);
            case OPERATOR:
                return new RegisteredOperatorStateBackendMetaInfo<>(snapshot);
            case BROADCAST:
                return new RegisteredBroadcastStateBackendMetaInfo<>(snapshot);
            case PRIORITY_QUEUE:
                return new RegisteredPriorityQueueStateBackendMetaInfo<>(snapshot);
            default:
                throw new IllegalArgumentException(
                        "Unknown backend state type: " + backendStateType);
        }
    }
}
