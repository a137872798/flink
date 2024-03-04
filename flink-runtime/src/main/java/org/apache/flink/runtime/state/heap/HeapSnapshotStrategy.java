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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.createDuplicatingStream;
import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.createSimpleStream;
import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult;

/** A strategy how to perform a snapshot of a {@link HeapKeyedStateBackend}.
 * HeapSnapshotResources 就是用来存放state的   还提供了遍历stateEntry的迭代器
 * 该对象可以将状态持久化 并返回一个读取状态的stateHandle
 * */
class HeapSnapshotStrategy<K>
        implements SnapshotStrategy<KeyedStateHandle, HeapSnapshotResources<K>> {

    /**
     * 一个StateTable 对应一个state
     * string 应该是 stateName
     */
    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;

    /**
     * 表示使用优先队列存储的state 同样一个state对应一个HeapPriorityQueueSnapshotRestoreWrapper
     */
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final StreamCompressionDecorator keyGroupCompressionDecorator;

    /**
     * 提供目录
     */
    private final LocalRecoveryConfig localRecoveryConfig;
    private final KeyGroupRange keyGroupRange;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final int totalKeyGroups;

    HeapSnapshotStrategy(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            LocalRecoveryConfig localRecoveryConfig,
            KeyGroupRange keyGroupRange,
            StateSerializerProvider<K> keySerializerProvider,
            int totalKeyGroups) {
        this.registeredKVStates = registeredKVStates;
        this.registeredPQStates = registeredPQStates;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.localRecoveryConfig = localRecoveryConfig;
        this.keyGroupRange = keyGroupRange;
        this.keySerializerProvider = keySerializerProvider;
        this.totalKeyGroups = totalKeyGroups;
    }

    @Override
    public HeapSnapshotResources<K> syncPrepareResources(long checkpointId) {
        // 就是利用这些变量产生资源对象
        return HeapSnapshotResources.create(
                registeredKVStates,
                registeredPQStates,
                keyGroupCompressionDecorator,
                keyGroupRange,
                getKeySerializer(),
                totalKeyGroups);
    }

    /**
     * 将快照数据写入检查点
     * @param syncPartResource
     * @param checkpointId The ID of the checkpoint.
     * @param timestamp The timestamp of the checkpoint.
     * @param streamFactory The factory that we can use for writing our state to streams.
     * @param checkpointOptions Options for how to perform this checkpoint.
     * @return
     */
    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            HeapSnapshotResources<K> syncPartResource,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        // 对应每个state的快照  主要是维护序列化对象
        List<StateMetaInfoSnapshot> metaInfoSnapshots = syncPartResource.getMetaInfoSnapshots();
        // 没有元数据快照 就返回空
        if (metaInfoSnapshots.isEmpty()) {
            return snapshotCloseableRegistry -> SnapshotResult.empty();
        }

        // 该对象用于写入状态元数据快照
        final KeyedBackendSerializationProxy<K> serializationProxy =
                new KeyedBackendSerializationProxy<>(
                        // TODO: this code assumes that writing a serializer is threadsafe, we
                        // should support to
                        // get a serialized form already at state registration time in the future
                        syncPartResource.getKeySerializer(),
                        metaInfoSnapshots,
                        !Objects.equals(
                                UncompressedStreamCompressionDecorator.INSTANCE,
                                keyGroupCompressionDecorator));

        // 根据是否是 savepoint 创建不同的stream
        final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                checkpointStreamSupplier =
                        localRecoveryConfig.isLocalRecoveryEnabled()
                                        && !checkpointOptions.getCheckpointType().isSavepoint()
                                ? () ->
                                        createDuplicatingStream(
                                                checkpointId,
                                                CheckpointedStateScope.EXCLUSIVE,
                                                streamFactory,
                                                localRecoveryConfig
                                                        .getLocalStateDirectoryProvider()
                                                        .orElseThrow(
                                                                LocalRecoveryConfig
                                                                        .localRecoveryNotEnabled()))
                                : () ->
                                        createSimpleStream(
                                                CheckpointedStateScope.EXCLUSIVE, streamFactory);


        // 返回一个惰性调用的对象
        return (snapshotCloseableRegistry) -> {
            final Map<StateUID, Integer> stateNamesToId = syncPartResource.getStateNamesToId();
            final Map<StateUID, StateSnapshot> cowStateStableSnapshots =
                    syncPartResource.getCowStateStableSnapshots();

            // 拿到保存检查点的输出流
            final CheckpointStreamWithResultProvider streamWithResultProvider =
                    checkpointStreamSupplier.get();

            // 注册close对象
            snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

            // 得到输出流 用于写入检查点
            final CheckpointStateOutputStream localStream =
                    streamWithResultProvider.getCheckpointOutputStream();

            // 将元数据信息写入输出流
            final DataOutputViewStreamWrapper outView =
                    new DataOutputViewStreamWrapper(localStream);
            serializationProxy.write(outView);

            // 记录每个key开始的偏移量
            final long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];

            // 按照key 遍历数据 并写入
            for (int keyGroupPos = 0;
                    keyGroupPos < keyGroupRange.getNumberOfKeyGroups();
                    ++keyGroupPos) {
                int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
                keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();

                // 写入key编号
                outView.writeInt(keyGroupId);

                for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
                        cowStateStableSnapshots.entrySet()) {

                    // 获取writer对象
                    StateSnapshot.StateKeyGroupWriter partitionedSnapshot =
                            stateSnapshot.getValue().getKeyGroupWriter();

                    // state数据 需要压缩写入
                    try (OutputStream kgCompressionOut =
                            keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
                        DataOutputViewStreamWrapper kgCompressionView =
                                new DataOutputViewStreamWrapper(kgCompressionOut);

                        // 写入stateId
                        kgCompressionView.writeShort(stateNamesToId.get(stateSnapshot.getKey()));
                        // 利用partitionedSnapshot 写入状态数据
                        partitionedSnapshot.writeStateInKeyGroup(kgCompressionView, keyGroupId);
                    } // this will just close the outer compression stream
                }
            }


            // 可以手动关闭 就注销
            if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {

                // 产生offset对象
                KeyGroupRangeOffsets kgOffs =
                        new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);

                // 关闭流 并得到可以回读state的handle对象
                SnapshotResult<StreamStateHandle> result =
                        streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
                // 包装结果
                return toKeyedStateHandleSnapshotResult(result, kgOffs, KeyGroupsStateHandle::new);
            } else {
                throw new IOException("Stream already unregistered.");
            }
        };
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializerProvider.currentSchemaSerializer();
    }
}
