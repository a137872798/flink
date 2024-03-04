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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FileBasedStateOutputStream;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Interface that provides access to a CheckpointStateOutputStream and a method to provide the
 * {@link SnapshotResult}. This abstracts from different ways that a result is obtained from
 * checkpoint output streams.
 *
 * 可用于访问包含检查点数据的stream
 */
public interface CheckpointStreamWithResultProvider extends Closeable {

    Logger LOG = LoggerFactory.getLogger(CheckpointStreamWithResultProvider.class);

    /** Closes the stream ans returns a snapshot result with the stream handle(s).
     * 关闭检查点流 并得到结果
     * StreamStateHandle 表示可以以stream的形式读取state
     * */
    @Nonnull
    SnapshotResult<StreamStateHandle> closeAndFinalizeCheckpointStreamResult() throws IOException;

    /** Returns the encapsulated output stream.
     * 使用该输出流保存检查点
     * */
    @Nonnull
    CheckpointStateOutputStream getCheckpointOutputStream();

    @Override
    default void close() throws IOException {
        getCheckpointOutputStream().close();
    }

    /**
     * Implementation of {@link CheckpointStreamWithResultProvider} that only creates the
     * primary/remote/jm-owned state.
     *
     * 表示只有一个简单的流
     */
    class PrimaryStreamOnly implements CheckpointStreamWithResultProvider {

        @Nonnull private final CheckpointStateOutputStream outputStream;

        public PrimaryStreamOnly(@Nonnull CheckpointStateOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        /**
         * 关闭流 并包装结果
         * @return
         * @throws IOException
         */
        @Nonnull
        @Override
        public SnapshotResult<StreamStateHandle> closeAndFinalizeCheckpointStreamResult()
                throws IOException {
            return SnapshotResult.of(outputStream.closeAndGetHandle());
        }

        @Nonnull
        @Override
        public CheckpointStateOutputStream getCheckpointOutputStream() {
            return outputStream;
        }
    }

    /**
     * Implementation of {@link CheckpointStreamWithResultProvider} that creates both, the
     * primary/remote/jm-owned state and the secondary/local/tm-owned state.
     * 同时包含2个流  会进行双写
     */
    class PrimaryAndSecondaryStream implements CheckpointStreamWithResultProvider {

        private static final Logger LOG = LoggerFactory.getLogger(PrimaryAndSecondaryStream.class);

        @Nonnull private final DuplicatingCheckpointOutputStream outputStream;

        public PrimaryAndSecondaryStream(
                @Nonnull CheckpointStateOutputStream primaryOut,
                CheckpointStateOutputStream secondaryOut)
                throws IOException {
            this(new DuplicatingCheckpointOutputStream(primaryOut, secondaryOut));
        }

        PrimaryAndSecondaryStream(@Nonnull DuplicatingCheckpointOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        /**
         * 关闭并返回结果
         * @return
         * @throws IOException
         */
        @Nonnull
        @Override
        public SnapshotResult<StreamStateHandle> closeAndFinalizeCheckpointStreamResult()
                throws IOException {

            final StreamStateHandle primaryStreamStateHandle;

            try {
                primaryStreamStateHandle = outputStream.closeAndGetPrimaryHandle();
            } catch (IOException primaryEx) {
                try {
                    outputStream.close();
                } catch (IOException closeEx) {
                    primaryEx = ExceptionUtils.firstOrSuppressed(closeEx, primaryEx);
                }
                throw primaryEx;
            }

            StreamStateHandle secondaryStreamStateHandle = null;

            try {
                secondaryStreamStateHandle = outputStream.closeAndGetSecondaryHandle();
            } catch (IOException secondaryEx) {
                LOG.warn("Exception from secondary/local checkpoint stream.", secondaryEx);
            }

            if (primaryStreamStateHandle != null) {
                if (secondaryStreamStateHandle != null) {
                    // SnapshotResult中的2个结果 一个代表 jobManager 一个代表本地
                    return SnapshotResult.withLocalState(
                            primaryStreamStateHandle, secondaryStreamStateHandle);
                } else {
                    // 单个就是  jobManaqer
                    return SnapshotResult.of(primaryStreamStateHandle);
                }
            } else {
                return SnapshotResult.empty();
            }
        }

        @Nonnull
        @Override
        public DuplicatingCheckpointOutputStream getCheckpointOutputStream() {
            return outputStream;
        }
    }

    /**
     * 创建单个流
     * @param checkpointedStateScope
     * @param primaryStreamFactory
     * @return
     * @throws IOException
     */
    @Nonnull
    static CheckpointStreamWithResultProvider createSimpleStream(
            @Nonnull CheckpointedStateScope checkpointedStateScope,
            @Nonnull CheckpointStreamFactory primaryStreamFactory)
            throws IOException {

        // scope会变成类似路径的东西  然后产生文件  (针对基于fileSystem的检查点输出流来说)
        // 注意在flink中 文件系统不一定是本地文件系统 也可能是分布式文件系统(hdfs)
        CheckpointStateOutputStream primaryOut =
                primaryStreamFactory.createCheckpointStateOutputStream(checkpointedStateScope);

        // 产生文件输出流
        return new CheckpointStreamWithResultProvider.PrimaryStreamOnly(primaryOut);
    }

    /**
     * 要产生2个流  一个通往jobManager 一个通往local
     * @param checkpointId
     * @param checkpointedStateScope
     * @param primaryStreamFactory
     * @param secondaryStreamDirProvider
     * @return
     * @throws IOException
     */
    @Nonnull
    static CheckpointStreamWithResultProvider createDuplicatingStream(
            @Nonnegative long checkpointId,
            @Nonnull CheckpointedStateScope checkpointedStateScope,
            @Nonnull CheckpointStreamFactory primaryStreamFactory,
            @Nonnull LocalRecoveryDirectoryProvider secondaryStreamDirProvider)
            throws IOException {

        // 产生fs输出流
        CheckpointStateOutputStream primaryOut =
                primaryStreamFactory.createCheckpointStateOutputStream(checkpointedStateScope);

        try {
            // 这是本地文件
            File outFile =
                    new File(
                            secondaryStreamDirProvider.subtaskSpecificCheckpointDirectory(
                                    checkpointId),
                            String.valueOf(UUID.randomUUID()));
            Path outPath = new Path(outFile.toURI());

            CheckpointStateOutputStream secondaryOut =
                    new FileBasedStateOutputStream(outPath.getFileSystem(), outPath);

            return new CheckpointStreamWithResultProvider.PrimaryAndSecondaryStream(
                    primaryOut, secondaryOut);
        } catch (IOException secondaryEx) {
            LOG.warn(
                    "Exception when opening secondary/local checkpoint output stream. "
                            + "Continue only with the primary stream.",
                    secondaryEx);
        }

        return new CheckpointStreamWithResultProvider.PrimaryStreamOnly(primaryOut);
    }

    /**
     * Factory method for a {@link KeyedStateHandle} to be used in {@link
     * #toKeyedStateHandleSnapshotResult(SnapshotResult, KeyGroupRangeOffsets,
     * KeyedStateHandleFactory)}.
     */
    @FunctionalInterface
    interface KeyedStateHandleFactory {
        KeyedStateHandle create(
                KeyGroupRangeOffsets keyGroupRangeOffsets, StreamStateHandle streamStateHandle);
    }

    /**
     * Helper method that takes a {@link SnapshotResult<StreamStateHandle>} and a {@link
     * KeyGroupRangeOffsets} and creates a {@link SnapshotResult<KeyedStateHandle>} by combining the
     * key groups offsets with all the present stream state handles.
     * 加工StreamStateHandle
     */
    @Nonnull
    static SnapshotResult<KeyedStateHandle> toKeyedStateHandleSnapshotResult(
            @Nonnull SnapshotResult<StreamStateHandle> snapshotResult,
            @Nonnull KeyGroupRangeOffsets keyGroupRangeOffsets,
            @Nonnull KeyedStateHandleFactory stateHandleFactory) {

        // 获取写入到jobManager的句柄
        StreamStateHandle jobManagerOwnedSnapshot = snapshotResult.getJobManagerOwnedSnapshot();

        if (jobManagerOwnedSnapshot != null) {

            // 加工变成KeyedStateHandle
            KeyedStateHandle jmKeyedState =
                    stateHandleFactory.create(keyGroupRangeOffsets, jobManagerOwnedSnapshot);
            StreamStateHandle taskLocalSnapshot = snapshotResult.getTaskLocalSnapshot();

            if (taskLocalSnapshot != null) {

                // 对本地结果也处理一次
                KeyedStateHandle localKeyedState =
                        stateHandleFactory.create(keyGroupRangeOffsets, taskLocalSnapshot);
                return SnapshotResult.withLocalState(jmKeyedState, localKeyedState);
            } else {

                return SnapshotResult.of(jmKeyedState);
            }
        } else {

            return SnapshotResult.empty();
        }
    }
}
