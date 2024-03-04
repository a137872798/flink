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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.parseBufferHeader;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.getSegmentPath;

/** The implementation of {@link PartitionFileReader} with segment file mode.
 * 读取段文件
 * */
public class SegmentPartitionFileReader implements PartitionFileReader {

    /**
     * 存储数据的buffer
     */
    private final ByteBuffer reusedHeaderBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();

    /**
     * Opened file channels and segment id of related segment files stored in map.
     *
     * <p>The key is partition id and subpartition id. The value is file channel and segment id.
     */
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Tuple2<ReadableByteChannel, Integer>>>
            openedChannelAndSegmentIds = new HashMap<>();

    private final String dataFilePath;

    private FileSystem fileSystem;

    public SegmentPartitionFileReader(String dataFilePath) {
        this.dataFilePath = dataFilePath;
        try {
            this.fileSystem = new Path(dataFilePath).getFileSystem();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to initialize the FileSystem.");
        }
    }

    /**
     * 在指定id后读取数据
     * @param partitionId the partition id of the buffer
     * @param subpartitionId the subpartition id of the buffer
     * @param segmentId the segment id of the buffer
     * @param bufferIndex the index of buffer
     * @param memorySegment the empty buffer to store the read buffer
     * @param recycler the buffer recycler
     * @param readProgress the current read progress. The progress comes from the previous
     *     ReadBufferResult. Note that the read progress should be implemented and provided by
     *     Flink, and it should be directly tied to the file format. The field can be null if the
     *     current file reader has no the read progress
     * @param partialBuffer the previous partial buffer. The partial buffer is not null only when
     *     the last read has a partial buffer, it will construct a full buffer during the read
     *     process.
     * @return
     * @throws IOException
     */
    @Override
    public ReadBufferResult readBuffer(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            MemorySegment memorySegment,
            BufferRecycler recycler,
            @Nullable ReadProgress readProgress,
            @Nullable CompositeBuffer partialBuffer)
            throws IOException {

        // Get the channel of the segment file for a subpartition.
        Map<TieredStorageSubpartitionId, Tuple2<ReadableByteChannel, Integer>> subpartitionInfo =
                openedChannelAndSegmentIds.computeIfAbsent(partitionId, ignore -> new HashMap<>());

        // 对应子分区的数据文件
        Tuple2<ReadableByteChannel, Integer> fileChannelAndSegmentId =
                subpartitionInfo.getOrDefault(subpartitionId, Tuple2.of(null, -1));
        ReadableByteChannel channel = fileChannelAndSegmentId.f0;

        // Create the channel if there is a new segment file for a subpartition.
        // 重新创建channel
        if (channel == null || fileChannelAndSegmentId.f1 != segmentId) {
            // 不一致的情况
            if (channel != null) {
                channel.close();
            }
            channel = openNewChannel(partitionId, subpartitionId, segmentId);
            if (channel == null) {
                // return null if the segment file doesn't exist.
                return null;
            }
            subpartitionInfo.put(subpartitionId, Tuple2.of(channel, segmentId));
        }

        // Try to read a buffer from the channel.
        reusedHeaderBuffer.clear();
        int bufferHeaderResult = channel.read(reusedHeaderBuffer);
        if (bufferHeaderResult == -1) {
            channel.close();
            // 数据为空
            openedChannelAndSegmentIds.get(partitionId).remove(subpartitionId);
            return getSingletonReadResult(
                    new NetworkBuffer(memorySegment, recycler, Buffer.DataType.END_OF_SEGMENT));
        }

        // 解析头部 获取长度信息
        reusedHeaderBuffer.flip();
        BufferHeader header = parseBufferHeader(reusedHeaderBuffer);
        // 根据长度读取数据
        int dataBufferResult = channel.read(memorySegment.wrap(0, header.getLength()));
        if (dataBufferResult != header.getLength()) {
            channel.close();
            throw new IOException("The length of data buffer is illegal.");
        }
        Buffer.DataType dataType = header.getDataType();
        return getSingletonReadResult(
                new NetworkBuffer(
                        memorySegment,
                        recycler,
                        dataType,
                        header.isCompressed(),
                        header.getLength()));
    }

    @Override
    public long getPriority(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId,
            int bufferIndex,
            @Nullable ReadProgress readProgress) {
        // noop
        return -1;
    }

    /**
     * 创建channel
     * @param partitionId
     * @param subpartitionId
     * @param segmentId
     * @return
     * @throws IOException
     */
    private ReadableByteChannel openNewChannel(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            int segmentId)
            throws IOException {
        Path currentSegmentPath =
                getSegmentPath(
                        dataFilePath, partitionId, subpartitionId.getSubpartitionId(), segmentId);
        if (!fileSystem.exists(currentSegmentPath)) {
            return null;
        }
        return Channels.newChannel(fileSystem.open(currentSegmentPath));
    }

    /**
     * 关闭相关文件
     */
    @Override
    public void release() {
        openedChannelAndSegmentIds.values().stream()
                .map(Map::values)
                .flatMap(
                        (Function<
                                        Collection<Tuple2<ReadableByteChannel, Integer>>,
                                        Stream<Tuple2<ReadableByteChannel, Integer>>>)
                                Collection::stream)
                .filter(Objects::nonNull)
                .forEach(
                        channel -> {
                            try {
                                channel.f0.close();
                            } catch (IOException e) {
                                ExceptionUtils.rethrow(e);
                            }
                        });
    }

    private static ReadBufferResult getSingletonReadResult(NetworkBuffer buffer) {
        return new ReadBufferResult(Collections.singletonList(buffer), false, null);
    }
}
