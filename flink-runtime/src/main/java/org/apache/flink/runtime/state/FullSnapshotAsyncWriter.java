/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import static org.apache.flink.runtime.state.FullSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.runtime.state.FullSnapshotUtil.hasMetaDataFollowsFlag;
import static org.apache.flink.runtime.state.FullSnapshotUtil.setMetaDataFollowsFlagInKey;

/**
 * An asynchronous writer that can write a full snapshot/savepoint from a {@link
 * FullSnapshotResources}.
 *
 * @param <K> type of the backend keys.
 *           调用api可以得到快照结果
 */
public class FullSnapshotAsyncWriter<K>
        implements SnapshotStrategy.SnapshotResultSupplier<KeyedStateHandle> {

    /** Supplier for the stream into which we write the snapshot. */
    @Nonnull
    private final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
            checkpointStreamSupplier;

    /**
     * 包含生成快照时需要的各种资源
     */
    @Nonnull private final FullSnapshotResources<K> snapshotResources;
    @Nonnull private final SnapshotType snapshotType;

    public FullSnapshotAsyncWriter(
            @Nonnull SnapshotType snapshotType,
            @Nonnull
                    SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                            checkpointStreamSupplier,
            @Nonnull FullSnapshotResources<K> snapshotResources) {

        this.checkpointStreamSupplier = checkpointStreamSupplier;
        this.snapshotResources = snapshotResources;
        this.snapshotType = snapshotType;
    }

    /**
     * 读取快照结果
     * @param snapshotCloseableRegistry A registry for io tasks to close on cancel.
     * @return
     * @throws Exception
     */
    @Override
    public SnapshotResult<KeyedStateHandle> get(CloseableRegistry snapshotCloseableRegistry)
            throws Exception {
        final KeyGroupRangeOffsets keyGroupRangeOffsets =
                new KeyGroupRangeOffsets(snapshotResources.getKeyGroupRange());
        final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider =
                checkpointStreamSupplier.get();

        snapshotCloseableRegistry.registerCloseable(checkpointStreamWithResultProvider);

        // 将快照数据写入到outputStream中
        writeSnapshotToOutputStream(checkpointStreamWithResultProvider, keyGroupRangeOffsets);

        if (snapshotCloseableRegistry.unregisterCloseable(checkpointStreamWithResultProvider)) {
            final CheckpointStreamWithResultProvider.KeyedStateHandleFactory stateHandleFactory;
            if (snapshotType.isSavepoint()) {
                stateHandleFactory = KeyGroupsSavepointStateHandle::new;
            } else {
                stateHandleFactory = KeyGroupsStateHandle::new;
            }
            return CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult(
                    checkpointStreamWithResultProvider.closeAndFinalizeCheckpointStreamResult(),
                    keyGroupRangeOffsets,
                    stateHandleFactory);
        } else {
            throw new IOException("Stream is already unregistered/closed.");
        }
    }

    /**
     * 将快照数据写入到输出流
     * @param checkpointStreamWithResultProvider
     * @param keyGroupRangeOffsets
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeSnapshotToOutputStream(
            @Nonnull CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
            @Nonnull KeyGroupRangeOffsets keyGroupRangeOffsets)
            throws IOException, InterruptedException {

        final DataOutputView outputView =
                new DataOutputViewStreamWrapper(
                        checkpointStreamWithResultProvider.getCheckpointOutputStream());

        // 写入元数据
        writeKVStateMetaData(outputView);

        // 获取可以遍历内部所有状态的迭代器  并挨个写入状态
        try (KeyValueStateIterator kvStateIterator = snapshotResources.createKVStateIterator()) {
            writeKVStateData(
                    kvStateIterator, checkpointStreamWithResultProvider, keyGroupRangeOffsets);
        }
    }

    /**
     * 写入元数据
     * @param outputView
     * @throws IOException
     */
    private void writeKVStateMetaData(final DataOutputView outputView) throws IOException {

        KeyedBackendSerializationProxy<K> serializationProxy =
                new KeyedBackendSerializationProxy<>(
                        // TODO: this code assumes that writing a serializer is threadsafe, we
                        // should support to
                        // get a serialized form already at state registration time in the
                        // future
                        snapshotResources.getKeySerializer(),
                        snapshotResources.getMetaInfoSnapshots(),
                        !Objects.equals(
                                UncompressedStreamCompressionDecorator.INSTANCE,
                                snapshotResources.getStreamCompressionDecorator()));

        serializationProxy.write(outputView);
    }

    /**
     * 写入状态数据
     * @param mergeIterator
     * @param checkpointStreamWithResultProvider
     * @param keyGroupRangeOffsets
     * @throws IOException
     * @throws InterruptedException
     */
    private void writeKVStateData(
            final KeyValueStateIterator mergeIterator,
            final CheckpointStreamWithResultProvider checkpointStreamWithResultProvider,
            final KeyGroupRangeOffsets keyGroupRangeOffsets)
            throws IOException, InterruptedException {

        byte[] previousKey = null;
        byte[] previousValue = null;
        DataOutputView kgOutView = null;
        OutputStream kgOutStream = null;
        CheckpointStateOutputStream checkpointOutputStream =
                checkpointStreamWithResultProvider.getCheckpointOutputStream();

        try {

            // preamble: setup with first key-group as our lookahead
            if (mergeIterator.isValid()) {
                // begin first key-group by recording the offset
                // 开始填充第一个offset
                keyGroupRangeOffsets.setKeyGroupOffset(
                        mergeIterator.keyGroup(), checkpointOutputStream.getPos());
                // write the k/v-state id as metadata
                kgOutStream =
                        snapshotResources
                                .getStreamCompressionDecorator()
                                .decorateWithCompression(checkpointOutputStream);
                kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
                // TODO this could be aware of keyGroupPrefixBytes and write only one byte
                // if possible
                kgOutView.writeShort(mergeIterator.kvStateId());
                previousKey = mergeIterator.key();
                previousValue = mergeIterator.value();
                mergeIterator.next();
            }

            // main loop: write k/v pairs ordered by (key-group, kv-state), thereby tracking
            // key-group offsets.
            while (mergeIterator.isValid()) {

                assert (!hasMetaDataFollowsFlag(previousKey));

                // set signal in first key byte that meta data will follow in the stream
                // after this k/v pair
                // 代表切换到下一个key 或者 group了
                if (mergeIterator.isNewKeyGroup() || mergeIterator.isNewKeyValueState()) {

                    // be cooperative and check for interruption from time to time in the
                    // hot loop
                    checkInterrupted();

                    // 给上个key打标 这样在读取时就可以根据提示信息知道 key即将变化
                    setMetaDataFollowsFlagInKey(previousKey);
                }

                // 写入本次的kv
                writeKeyValuePair(previousKey, previousValue, kgOutView);

                // write meta data if we have to
                // 写入一些切换信息
                if (mergeIterator.isNewKeyGroup()) {
                    // TODO this could be aware of keyGroupPrefixBytes and write only one
                    // byte if possible
                    kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
                    // this will just close the outer stream
                    kgOutStream.close();
                    // begin new key-group
                    keyGroupRangeOffsets.setKeyGroupOffset(
                            mergeIterator.keyGroup(), checkpointOutputStream.getPos());
                    // write the kev-state
                    // TODO this could be aware of keyGroupPrefixBytes and write only one
                    // byte if possible
                    kgOutStream =
                            snapshotResources
                                    .getStreamCompressionDecorator()
                                    .decorateWithCompression(checkpointOutputStream);
                    kgOutView = new DataOutputViewStreamWrapper(kgOutStream);
                    kgOutView.writeShort(mergeIterator.kvStateId());
                    // 表示切换了state  同一个state在一个输出流中会出现多次 每次对应keyGroup不同
                } else if (mergeIterator.isNewKeyValueState()) {
                    // write the k/v-state
                    // TODO this could be aware of keyGroupPrefixBytes and write only one
                    // byte if possible
                    kgOutView.writeShort(mergeIterator.kvStateId());
                }

                // request next k/v pair
                previousKey = mergeIterator.key();
                previousValue = mergeIterator.value();
                mergeIterator.next();
            }

            // epilogue: write last key-group
            // 最后做一次收尾操作
            if (previousKey != null) {
                assert (!hasMetaDataFollowsFlag(previousKey));
                setMetaDataFollowsFlagInKey(previousKey);
                writeKeyValuePair(previousKey, previousValue, kgOutView);
                // TODO this could be aware of keyGroupPrefixBytes and write only one byte if
                // possible
                kgOutView.writeShort(END_OF_KEY_GROUP_MARK);
                // this will just close the outer stream
                kgOutStream.close();
                kgOutStream = null;
            }

        } finally {
            // this will just close the outer stream
            IOUtils.closeQuietly(kgOutStream);
        }
    }

    private void writeKeyValuePair(byte[] key, byte[] value, DataOutputView out)
            throws IOException {
        BytePrimitiveArraySerializer.INSTANCE.serialize(key, out);
        BytePrimitiveArraySerializer.INSTANCE.serialize(value, out);
    }

    private void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("RocksDB snapshot interrupted.");
        }
    }
}
