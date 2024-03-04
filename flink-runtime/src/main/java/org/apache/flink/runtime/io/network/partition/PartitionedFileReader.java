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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferHeader;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil.HEADER_LENGTH;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Reader which can read all data of the target subpartition from a {@link PartitionedFile}.
 * 读取存储分区数据的文件
 * */
class PartitionedFileReader {

    /** Used to read buffer headers from file channel. */
    private final ByteBuffer headerBuf;

    /** Used to read index entry from index file. */
    private final ByteBuffer indexEntryBuf;

    /** Target {@link PartitionedFile} to read. */
    private final PartitionedFile partitionedFile;

    /** Target subpartition to read. */
    private final int targetSubpartition;

    /** Data file channel of the target {@link PartitionedFile}. */
    private final FileChannel dataFileChannel;

    /** Index file channel of the target {@link PartitionedFile}. */
    private final FileChannel indexFileChannel;

    /** Next data region to be read. */
    private int nextRegionToRead;

    /** Next file offset to be read. */
    private long nextOffsetToRead;

    /** Number of remaining bytes in the current data region read.
     * 当前region还剩余多少
     * */
    private long currentRegionRemainingBytes;


    /**
     * 使用该对象读取某个子分区的数据
     * @param partitionedFile
     * @param targetSubpartition
     * @param dataFileChannel
     * @param indexFileChannel
     * @param headerBuffer
     * @param indexEntryBuffer
     */
    PartitionedFileReader(
            PartitionedFile partitionedFile,
            int targetSubpartition,
            FileChannel dataFileChannel,
            FileChannel indexFileChannel,
            ByteBuffer headerBuffer,
            ByteBuffer indexEntryBuffer) {
        checkArgument(checkNotNull(dataFileChannel).isOpen(), "Data file channel must be opened.");
        checkArgument(
                checkNotNull(indexFileChannel).isOpen(), "Index file channel must be opened.");

        this.partitionedFile = checkNotNull(partitionedFile);
        this.targetSubpartition = targetSubpartition;
        this.dataFileChannel = dataFileChannel;
        this.indexFileChannel = indexFileChannel;
        this.headerBuf = headerBuffer;
        this.indexEntryBuf = indexEntryBuffer;
    }

    /**
     * 切换到下个region
     * @param indexEntryBuf
     * @throws IOException
     */
    private void moveToNextReadableRegion(ByteBuffer indexEntryBuf) throws IOException {

        // 还有region
        while (currentRegionRemainingBytes <= 0
                && nextRegionToRead < partitionedFile.getNumRegions()) {
            // 得到索引数据
            partitionedFile.getIndexEntry(
                    indexFileChannel, indexEntryBuf, nextRegionToRead, targetSubpartition);
            // 一个是偏移量一个是长度
            nextOffsetToRead = indexEntryBuf.getLong();
            currentRegionRemainingBytes = indexEntryBuf.getLong();
            ++nextRegionToRead;
        }
    }

    /**
     * Reads a buffer from the current region of the target {@link PartitionedFile} and moves the
     * read position forward.
     *
     * <p>Note: The caller is responsible for recycling the target buffer if any exception occurs.
     *
     * @param freeSegments The free {@link MemorySegment}s to read data to.
     * @param recycler The {@link BufferRecycler} which is responsible to recycle the target buffer.
     * @param consumer The target {@link Buffer} stores the data read from file channel.
     * @return Whether the file reader has remaining data to read.
     */
    boolean readCurrentRegion(
            Queue<MemorySegment> freeSegments, BufferRecycler recycler, Consumer<Buffer> consumer)
            throws IOException {
        if (currentRegionRemainingBytes == 0) {
            return false;
        }

        checkArgument(!freeSegments.isEmpty(), "No buffer available for data reading.");
        dataFileChannel.position(nextOffsetToRead);

        BufferAndHeader partialBuffer = new BufferAndHeader(null, null);
        try {
            // 还有seg未填充数据  且还有数据
            while (!freeSegments.isEmpty() && currentRegionRemainingBytes > 0) {
                MemorySegment segment = freeSegments.poll();
                int numBytes = (int) Math.min(segment.size(), currentRegionRemainingBytes);
                ByteBuffer byteBuffer = segment.wrap(0, numBytes);

                try {
                    // 读取数据到buffer中   注意header数据和data是挨个写入的
                    BufferReaderWriterUtil.readByteBufferFully(dataFileChannel, byteBuffer);
                    byteBuffer.flip();
                    currentRegionRemainingBytes -= byteBuffer.remaining();
                    nextOffsetToRead += byteBuffer.remaining();
                } catch (Throwable throwable) {
                    freeSegments.add(segment);
                    throw throwable;
                }

                NetworkBuffer buffer = new NetworkBuffer(segment, recycler);
                buffer.setSize(byteBuffer.remaining());
                try {
                    // 产生buffer并处理
                    partialBuffer = processBuffer(byteBuffer, buffer, partialBuffer, consumer);
                } catch (Throwable throwable) {
                    partialBuffer = new BufferAndHeader(null, null);
                    throw throwable;
                } finally {
                    buffer.recycleBuffer();
                }
            }
        } finally {
            // 进行修正工作
            if (headerBuf.position() > 0) {
                nextOffsetToRead -= headerBuf.position();
                currentRegionRemainingBytes += headerBuf.position();
                headerBuf.clear();
            }
            if (partialBuffer.header != null) {
                nextOffsetToRead -= HEADER_LENGTH;
                currentRegionRemainingBytes += HEADER_LENGTH;
            }
            if (partialBuffer.buffer != null) {
                nextOffsetToRead -= partialBuffer.buffer.readableBytes();
                currentRegionRemainingBytes += partialBuffer.buffer.readableBytes();
                partialBuffer.buffer.recycleBuffer();
            }
        }
        return hasRemaining();
    }

    boolean hasRemaining() throws IOException {
        moveToNextReadableRegion(indexEntryBuf);
        return currentRegionRemainingBytes > 0;
    }

    /**
     *
     * @param initIndexEntryBuffer
     * @throws IOException
     */
    void initRegionIndex(ByteBuffer initIndexEntryBuffer) throws IOException {
        moveToNextReadableRegion(initIndexEntryBuffer);
    }

    /** Gets read priority of this file reader. Smaller value indicates higher priority. */
    long getPriority() {
        return nextOffsetToRead;
    }

    /**
     *
     * @param byteBuffer  存储有效数据的buffer
     * @param buffer
     * @param partialBuffer  待填充的buffer
     * @param consumer  支持传入一个消费者处理数据
     * @return
     */
    private BufferAndHeader processBuffer(
            ByteBuffer byteBuffer,
            Buffer buffer,
            BufferAndHeader partialBuffer,
            Consumer<Buffer> consumer) {
        BufferHeader header = partialBuffer.header;
        CompositeBuffer targetBuffer = partialBuffer.buffer;
        while (byteBuffer.hasRemaining()) {
            // 先解析头部数据  返回null表示本次数据不足
            if (header == null && (header = parseBufferHeader(byteBuffer)) == null) {
                break;
            }

            // 这个是存储结果数据的buffer  表示之前已经存储了部分数据了
            if (targetBuffer != null) {
                buffer.retainBuffer();
                int position = byteBuffer.position() + targetBuffer.missingLength();
                targetBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), targetBuffer.missingLength()));
                byteBuffer.position(position);
                // 表示还需要读取下个buffer的数据
            } else if (byteBuffer.remaining() < header.getLength()) {
                if (byteBuffer.hasRemaining()) {
                    buffer.retainBuffer();
                    targetBuffer = new CompositeBuffer(header);
                    targetBuffer.addPartialBuffer(
                            buffer.readOnlySlice(byteBuffer.position(), byteBuffer.remaining()));
                }
                break;
            } else {
                // 表示byteBuffer中有足够的数据
                buffer.retainBuffer();
                targetBuffer = new CompositeBuffer(header);
                // 将剩下的数据作为一个部分加入到 compositeBuffer中
                targetBuffer.addPartialBuffer(
                        buffer.readOnlySlice(byteBuffer.position(), header.getLength()));
                byteBuffer.position(byteBuffer.position() + header.getLength());
            }

            header = null;
            consumer.accept(targetBuffer);
            targetBuffer = null;
        }
        return new BufferAndHeader(targetBuffer, header);
    }

    /**
     * 解析头部数据
     * @param buffer
     * @return
     */
    private BufferHeader parseBufferHeader(ByteBuffer buffer) {
        BufferHeader header = null;
        // headerBuf的长度应该就是  HEADER_LENGTH
        if (headerBuf.position() > 0) {
            while (headerBuf.hasRemaining()) {
                headerBuf.put(buffer.get());
            }
            headerBuf.flip();
            header = BufferReaderWriterUtil.parseBufferHeader(headerBuf);
            headerBuf.clear();
        }

        if (header == null && buffer.remaining() < HEADER_LENGTH) {
            // 此时数据不完整  先写入一部分
            headerBuf.put(buffer);
        } else if (header == null) {
            // buffer的数据足够   直接解析buffer即可
            header = BufferReaderWriterUtil.parseBufferHeader(buffer);
        }
        return header;
    }

    private static class BufferAndHeader {

        private final CompositeBuffer buffer;
        private final BufferHeader header;

        BufferAndHeader(CompositeBuffer buffer, BufferHeader header) {
            this.buffer = buffer;
            this.header = header;
        }
    }
}
