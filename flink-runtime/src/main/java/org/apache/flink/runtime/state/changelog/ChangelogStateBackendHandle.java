/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.CheckpointBoundKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsSavepointStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryKey;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A handle to ChangelogStateBackend state. Consists of the base and delta parts. Base part
 * references materialized state (e.g. SST files), while delta part references state changes that
 * were not not materialized at the time of the snapshot. Both are potentially empty lists as there
 * can be no state or multiple states (e.g. after rescaling).
 */
@Internal
public interface ChangelogStateBackendHandle
        extends KeyedStateHandle, CheckpointBoundKeyedStateHandle {

    /**
     * 获取所有被物化状态 关联的状态管理器
     * @return
     */
    List<KeyedStateHandle> getMaterializedStateHandles();

    /**
     * 获取未物化状态管理器
     * @return
     */
    List<ChangelogStateHandle> getNonMaterializedStateHandles();

    /**
     * 这个物化id 应该关联其他物化状态
     * @return
     */
    long getMaterializationID();

    /**
     * 更换state相关的检查点后  重新产生该对象
     * @param checkpointId rebounded checkpoint id.
     * @return
     */
    @Override
    ChangelogStateBackendHandle rebound(long checkpointId);

    /**
     * 这个对象其实寓意着 remote
     */
    class ChangelogStateBackendHandleImpl implements ChangelogStateBackendHandle {
        private static final long serialVersionUID = 1L;

        private final List<KeyedStateHandle> materialized;
        private final List<ChangelogStateHandle> nonMaterialized;
        private final KeyGroupRange keyGroupRange;

        private final long materializationID;
        private final long checkpointId;
        private final long persistedSizeOfThisCheckpoint;

        /**
         * 这个id是专门指代ChangelogStateBackendHandle的
         */
        private final StateHandleID stateHandleID;

        public ChangelogStateBackendHandleImpl(
                List<KeyedStateHandle> materialized,
                List<ChangelogStateHandle> nonMaterialized,
                KeyGroupRange keyGroupRange,
                long checkpointId,
                long materializationID,
                long persistedSizeOfThisCheckpoint) {
            this(
                    materialized,
                    nonMaterialized,
                    keyGroupRange,
                    checkpointId,
                    materializationID,
                    persistedSizeOfThisCheckpoint,
                    StateHandleID.randomStateHandleId());
        }

        private ChangelogStateBackendHandleImpl(
                List<KeyedStateHandle> materialized,
                List<ChangelogStateHandle> nonMaterialized,
                KeyGroupRange keyGroupRange,
                long checkpointId,
                long materializationID,
                long persistedSizeOfThisCheckpoint,
                StateHandleID stateHandleId) {
            this.materialized = unmodifiableList(materialized);
            this.nonMaterialized = unmodifiableList(nonMaterialized);
            this.keyGroupRange = keyGroupRange;
            this.persistedSizeOfThisCheckpoint = persistedSizeOfThisCheckpoint;
            checkArgument(keyGroupRange.getNumberOfKeyGroups() > 0);
            this.checkpointId = checkpointId;
            this.materializationID = materializationID;
            this.stateHandleID = stateHandleId;
        }


        /**
         * 通过之前维护的信息 还原ChangelogStateBackendHandleImpl
         * @param materialized
         * @param nonMaterialized
         * @param keyGroupRange
         * @param checkpointId
         * @param materializationID
         * @param persistedSizeOfThisCheckpoint
         * @param stateHandleId
         * @return
         */
        public static ChangelogStateBackendHandleImpl restore(
                List<KeyedStateHandle> materialized,
                List<ChangelogStateHandle> nonMaterialized,
                KeyGroupRange keyGroupRange,
                long checkpointId,
                long materializationID,
                long persistedSizeOfThisCheckpoint,
                StateHandleID stateHandleId) {
            return new ChangelogStateBackendHandleImpl(
                    materialized,
                    nonMaterialized,
                    keyGroupRange,
                    checkpointId,
                    materializationID,
                    persistedSizeOfThisCheckpoint,
                    stateHandleId);
        }

        /**
         * 将传入的handle对象 转换成ChangelogStateBackendHandle
         * @param originKeyedStateHandle
         * @return
         */
        public static ChangelogStateBackendHandle getChangelogStateBackendHandle(
                KeyedStateHandle originKeyedStateHandle) {
            if (originKeyedStateHandle instanceof ChangelogStateBackendHandle) {
                return (ChangelogStateBackendHandle) originKeyedStateHandle;
            } else {
                return new ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl(
                        singletonList(castToAbsolutePath(originKeyedStateHandle)),
                        emptyList(),
                        originKeyedStateHandle.getKeyGroupRange(),
                        // 类型不对 就没有检查点id
                        originKeyedStateHandle instanceof CheckpointBoundKeyedStateHandle
                                ? ((CheckpointBoundKeyedStateHandle) originKeyedStateHandle)
                                        .getCheckpointId()
                                : 0L,
                        0L,
                        0L);
            }
        }

        /**
         * 转换成绝对路径
         * @param originKeyedStateHandle
         * @return
         */
        private static KeyedStateHandle castToAbsolutePath(
                KeyedStateHandle originKeyedStateHandle) {
            // For KeyedStateHandle, only KeyGroupsStateHandle and IncrementalKeyedStateHandle
            // contain streamStateHandle, and both of them need to be cast
            // as they all have state handles of private checkpoint scope.
            if (originKeyedStateHandle instanceof KeyGroupsSavepointStateHandle) {
                return originKeyedStateHandle;
            }

            // KeyGroupsStateHandle 跟 KeyGroupsSavepointStateHandle 类似  keyGroup关联offset 可以从stream中检索state
            if (originKeyedStateHandle instanceof KeyGroupsStateHandle) {
                StreamStateHandle streamStateHandle =
                        ((KeyGroupsStateHandle) originKeyedStateHandle).getDelegateStateHandle();

                // 因为是stream handle 所以有一个产生流的地方  如果是文件类型的
                if (streamStateHandle instanceof FileStateHandle) {
                    // 重新生成 FileStateHandle 时  使用了绝对路径
                    StreamStateHandle fileStateHandle = restoreFileStateHandle(streamStateHandle);
                    return KeyGroupsStateHandle.restore(
                            ((KeyGroupsStateHandle) originKeyedStateHandle).getGroupRangeOffsets(),
                            fileStateHandle,
                            originKeyedStateHandle.getStateHandleId());
                }
            }

            // IncrementalRemoteKeyedStateHandle 该对象包含了很多 stateHandle
            if (originKeyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
                IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle =
                        (IncrementalRemoteKeyedStateHandle) originKeyedStateHandle;

                // 也没看出做了什么手脚

                StreamStateHandle castMetaStateHandle =
                        restoreFileStateHandle(
                                incrementalRemoteKeyedStateHandle.getMetaStateHandle());
                List<HandleAndLocalPath> castSharedStates =
                        incrementalRemoteKeyedStateHandle.getSharedState().stream()
                                .map(
                                        e ->
                                                HandleAndLocalPath.of(
                                                        restoreFileStateHandle(e.getHandle()),
                                                        e.getLocalPath()))
                                .collect(Collectors.toList());

                List<HandleAndLocalPath> castPrivateStates =
                        incrementalRemoteKeyedStateHandle.getPrivateState().stream()
                                .map(
                                        e ->
                                                HandleAndLocalPath.of(
                                                        restoreFileStateHandle(e.getHandle()),
                                                        e.getLocalPath()))
                                .collect(Collectors.toList());

                return IncrementalRemoteKeyedStateHandle.restore(
                        incrementalRemoteKeyedStateHandle.getBackendIdentifier(),
                        incrementalRemoteKeyedStateHandle.getKeyGroupRange(),
                        incrementalRemoteKeyedStateHandle.getCheckpointId(),
                        castSharedStates,
                        castPrivateStates,
                        castMetaStateHandle,
                        incrementalRemoteKeyedStateHandle.getCheckpointedSize(),
                        incrementalRemoteKeyedStateHandle.getStateHandleId());
            }
            return originKeyedStateHandle;
        }

        private static StreamStateHandle restoreFileStateHandle(
                StreamStateHandle streamStateHandle) {
            // 产生新的实例
            if (streamStateHandle instanceof FileStateHandle) {
                return new FileStateHandle(
                        ((FileStateHandle) streamStateHandle).getFilePath(),
                        streamStateHandle.getStateSize());
            }
            return streamStateHandle;
        }


        /**
         * 将当前所有物化状态注册
         * @param stateRegistry The registry where shared states are registered.
         * @param checkpointID
         */
        @Override
        public void registerSharedStates(SharedStateRegistry stateRegistry, long checkpointID) {

            for (KeyedStateHandle keyedStateHandle : materialized) {
                // Use the unique and invariant UUID as the state registry key for a specific keyed
                // state handle. To avoid unexpected unregister, this registry key would not change
                // even rescaled.
                // 这里应该是基于key进行存储
                stateRegistry.registerReference(
                        new SharedStateRegistryKey(keyedStateHandle.getStateHandleId().toString()),
                        new StreamStateHandleWrapper(keyedStateHandle),
                        checkpointID,
                        true);
            }
            // 下面应该是更直接的存储  可能在stateRegistry的不同容器中
            stateRegistry.registerAll(materialized, checkpointID);
            stateRegistry.registerAll(nonMaterialized, checkpointID);
        }

        @Override
        public void discardState() throws Exception {
            // Do nothing: state will be discarded by SharedStateRegistry once JM receives it and a
            // newer checkpoint completes without using it.
            // if the checkpoints always failed, it would leave orphan files there.
            // The above cases will be addressed by FLINK-23139 and/or FLINK-24852.
        }

        @Override
        public KeyGroupRange getKeyGroupRange() {
            return keyGroupRange;
        }

        @Nullable
        @Override
        public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
            // todo: revisit/review
            KeyGroupRange intersection = this.keyGroupRange.getIntersection(keyGroupRange);
            if (intersection.getNumberOfKeyGroups() == 0) {
                return null;
            }
            // 物化/非物化 都取交集后 重新产生ChangelogStateBackendHandleImpl
            List<KeyedStateHandle> basePart =
                    this.materialized.stream()
                            .map(entry -> entry.getIntersection(keyGroupRange))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            List<ChangelogStateHandle> deltaPart =
                    this.nonMaterialized.stream()
                            .map(
                                    handle ->
                                            (ChangelogStateHandle)
                                                    handle.getIntersection(keyGroupRange))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            return new ChangelogStateBackendHandleImpl(
                    basePart,
                    deltaPart,
                    intersection,
                    checkpointId,
                    materializationID,
                    persistedSizeOfThisCheckpoint);
        }

        @Override
        public StateHandleID getStateHandleId() {
            return stateHandleID;
        }

        @Override
        public long getStateSize() {
            return materialized.stream().mapToLong(StateObject::getStateSize).sum()
                    + nonMaterialized.stream().mapToLong(StateObject::getStateSize).sum();
        }

        @Override
        public long getCheckpointedSize() {
            return persistedSizeOfThisCheckpoint;
        }

        @Override
        public List<KeyedStateHandle> getMaterializedStateHandles() {
            return materialized;
        }

        @Override
        public List<ChangelogStateHandle> getNonMaterializedStateHandles() {
            return nonMaterialized;
        }

        @Override
        public long getMaterializationID() {
            return materializationID;
        }

        @Override
        public String toString() {
            return String.format(
                    "keyGroupRange=%s, basePartSize=%d, deltaPartSize=%d",
                    keyGroupRange, materialized.size(), nonMaterialized.size());
        }

        @Override
        public long getCheckpointId() {
            return checkpointId;
        }

        /**
         * 更换检查点id
         * @param checkpointId rebounded checkpoint id.
         * @return
         */
        @Override
        public ChangelogStateBackendHandleImpl rebound(long checkpointId) {
            List<KeyedStateHandle> reboundedMaterialized =
                    materialized.stream()
                            .map(
                                    keyedStateHandle ->
                                            keyedStateHandle
                                                            instanceof
                                                            CheckpointBoundKeyedStateHandle
                                                    ? ((CheckpointBoundKeyedStateHandle)
                                                                    keyedStateHandle)
                                                            .rebound(checkpointId)
                                                    : keyedStateHandle)
                            .collect(Collectors.toList());
            return new ChangelogStateBackendHandleImpl(
                    reboundedMaterialized,
                    nonMaterialized,
                    keyGroupRange,
                    checkpointId,
                    materializationID,
                    persistedSizeOfThisCheckpoint,
                    stateHandleID);
        }

        /**
         * This wrapper class is introduced as current {@link SharedStateRegistry} only accept
         * StreamStateHandle to register, remove it once FLINK-25862 is resolved.
         *
         * 生成一个包装对象
         */
        private static class StreamStateHandleWrapper implements StreamStateHandle {
            private static final long serialVersionUID = 1L;

            private final KeyedStateHandle keyedStateHandle;

            StreamStateHandleWrapper(KeyedStateHandle keyedStateHandle) {
                this.keyedStateHandle = keyedStateHandle;
            }

            @Override
            public void discardState() throws Exception {
                keyedStateHandle.discardState();
            }

            @Override
            public long getStateSize() {
                return keyedStateHandle.getStateSize();
            }

            @Override
            public FSDataInputStream openInputStream() throws IOException {
                throw new UnsupportedOperationException("Should not call here.");
            }

            @Override
            public Optional<byte[]> asBytesIfInMemory() {
                throw new UnsupportedOperationException("Should not call here.");
            }

            @Override
            public PhysicalStateHandleID getStreamStateHandleID() {
                throw new UnsupportedOperationException("Should not call here.");
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                StreamStateHandleWrapper that = (StreamStateHandleWrapper) o;
                return Objects.equals(
                        keyedStateHandle.getStateHandleId(),
                        that.keyedStateHandle.getStateHandleId());
            }

            @Override
            public int hashCode() {
                return Objects.hash(keyedStateHandle.getStateHandleId());
            }

            @Override
            public String toString() {
                return "Wrapped{" + keyedStateHandle + '}';
            }
        }
    }
}
