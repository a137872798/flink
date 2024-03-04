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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * File writer which can write buffers and generate {@link PartitionedFile}. Data is written region
 * by region. Before writing a new region, the method {@link PartitionedFileWriter#startNewRegion}
 * must be called. After writing all data, the method {@link PartitionedFileWriter#finish} must be
 * called to close all opened files and return the target {@link PartitionedFile}.
 * 写分区文件数据的对象
 */
@NotThreadSafe
public class PartitionedFileWriter implements AutoCloseable {

    /**
     * 索引缓冲区大小   默认50个
     */
    private static final int MIN_INDEX_BUFFER_SIZE = 50 * PartitionedFile.INDEX_ENTRY_SIZE;

    /** Number of channels. When writing a buffer, target subpartition must be in this range.
     * 表示总计要写多少个文件
     * */
    private final int numSubpartitions;

    /** Opened data file channel of the target {@link PartitionedFile}. */
    private final FileChannel dataFileChannel;

    /** Opened index file channel of the target {@link PartitionedFile}. */
    private final FileChannel indexFileChannel;

    /** Data file path of the target {@link PartitionedFile}. */
    private final Path dataFilePath;

    /** Index file path of the target {@link PartitionedFile}. */
    private final Path indexFilePath;

    /** Offset in the data file for each subpartition in the current region.
     * 记录每个分区的起始位置
     * */
    private final long[] subpartitionOffsets;

    /** Data size written in bytes for each subpartition in the current region.
     * 记录每个分区写入多少数据
     * */
    private final long[] subpartitionBytes;

    /** Maximum number of bytes can be used to buffer index entries. */
    private final int maxIndexBufferSize;

    /** A piece of unmanaged memory for caching of region index entries. */
    private ByteBuffer indexBuffer;

    /** Whether all index entries are cached in the index buffer or not. */
    private boolean allIndexEntriesCached = true;

    /** Number of bytes written to the target {@link PartitionedFile}. */
    private long totalBytesWritten;

    /** Number of regions written to the target {@link PartitionedFile}. */
    private int numRegions;

    /** Total number of buffers in the data file. */
    private long numBuffers;

    /** Current subpartition to write buffers to. */
    private int currentSubpartition = -1;

    /**
     * Broadcast region is an optimization for the broadcast partition which writes the same data to
     * all subpartitions. For a broadcast region, data is only written once and the indexes of all
     * subpartitions point to the same offset in the data file.
     *
     * 当前分区是否是广播数据
     */
    private boolean isBroadcastRegion;

    /** Whether this file writer is finished or not. */
    private boolean isFinished;

    /** Whether this file writer is closed or not. */
    private boolean isClosed;

    public PartitionedFileWriter(int numSubpartitions, int maxIndexBufferSize, String basePath)
            throws IOException {
        this(numSubpartitions, MIN_INDEX_BUFFER_SIZE, maxIndexBufferSize, basePath);
    }

    /**
     *
     * @param numSubpartitions  表示有多少个分区
     * @param minIndexBufferSize
     * @param maxIndexBufferSize
     * @param basePath   存放文件的基础目录
     * @throws IOException
     */
    @VisibleForTesting
    PartitionedFileWriter(
            int numSubpartitions, int minIndexBufferSize, int maxIndexBufferSize, String basePath)
            throws IOException {
        checkArgument(numSubpartitions > 0, "Illegal number of subpartitions.");
        checkArgument(maxIndexBufferSize > 0, "Illegal maximum index cache size.");
        checkArgument(basePath != null, "Base path must not be null.");

        this.numSubpartitions = numSubpartitions;
        this.maxIndexBufferSize = alignMaxIndexBufferSize(maxIndexBufferSize);
        this.subpartitionOffsets = new long[numSubpartitions];
        this.subpartitionBytes = new long[numSubpartitions];
        this.dataFilePath = new File(basePath + PartitionedFile.DATA_FILE_SUFFIX).toPath();
        this.indexFilePath = new File(basePath + PartitionedFile.INDEX_FILE_SUFFIX).toPath();

        this.indexBuffer = ByteBuffer.allocate(minIndexBufferSize);
        BufferReaderWriterUtil.configureByteBuffer(indexBuffer);

        this.dataFileChannel = openFileChannel(dataFilePath);
        try {
            this.indexFileChannel = openFileChannel(indexFilePath);
        } catch (Throwable throwable) {
            // ensure that the data file channel is closed if any exception occurs
            IOUtils.closeQuietly(dataFileChannel);
            IOUtils.deleteFileQuietly(dataFilePath);
            throw throwable;
        }
    }

    private FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    private int alignMaxIndexBufferSize(int maxIndexBufferSize) {
        return maxIndexBufferSize
                / PartitionedFile.INDEX_ENTRY_SIZE
                * PartitionedFile.INDEX_ENTRY_SIZE;
    }

    /**
     * Persists the region index of the current data region and starts a new region to write.
     *
     * <p>Note: The caller is responsible for releasing the failed {@link PartitionedFile} if any
     * exception occurs.
     *
     * @param isBroadcastRegion Whether it's a broadcast region. See {@link #isBroadcastRegion}.
     *                          region其实就是一个分区
     */
    public void startNewRegion(boolean isBroadcastRegion) throws IOException {
        checkState(!isFinished, "File writer is already finished.");
        checkState(!isClosed, "File writer is already closed.");

        writeRegionIndex();
        this.isBroadcastRegion = isBroadcastRegion;
    }

    /**
     * 写入region数据
     * @param subpartitionOffset
     * @param numBytes
     * @throws IOException
     */
    private void writeIndexEntry(long subpartitionOffset, long numBytes) throws IOException {
        if (!indexBuffer.hasRemaining()) {
            if (!extendIndexBufferIfPossible()) {
                // buffer不能扩容 触发刷盘
                flushIndexBuffer();
                indexBuffer.clear();
                allIndexEntriesCached = false;
            }
        }

        // 按顺序写入每个子分区数据的偏移量和长度
        indexBuffer.putLong(subpartitionOffset);
        indexBuffer.putLong(numBytes);
    }

    /**
     * 检查索引buffer能不能扩容
     * @return
     */
    private boolean extendIndexBufferIfPossible() {
        if (indexBuffer.capacity() >= maxIndexBufferSize) {
            return false;
        }

        int newIndexBufferSize = Math.min(maxIndexBufferSize, 2 * indexBuffer.capacity());
        ByteBuffer newIndexBuffer = ByteBuffer.allocate(newIndexBufferSize);
        indexBuffer.flip();
        newIndexBuffer.put(indexBuffer);
        BufferReaderWriterUtil.configureByteBuffer(newIndexBuffer);
        indexBuffer = newIndexBuffer;

        return true;
    }

    /**
     * 切换到新分区
     * @throws IOException
     */
    private void writeRegionIndex() throws IOException {
        if (Arrays.stream(subpartitionBytes).sum() > 0) {
            for (int channel = 0; channel < numSubpartitions; ++channel) {
                // 写入上个region所有子分区的索引数据
                writeIndexEntry(subpartitionOffsets[channel], subpartitionBytes[channel]);
            }

            // 重置当前子分区
            currentSubpartition = -1;
            ++numRegions;
            Arrays.fill(subpartitionBytes, 0);
        }
    }

    private void flushIndexBuffer() throws IOException {
        indexBuffer.flip();
        if (indexBuffer.limit() > 0) {
            BufferReaderWriterUtil.writeBuffer(indexFileChannel, indexBuffer);
        }
    }

    /**
     * Writes a list of {@link Buffer}s to this {@link PartitionedFile}. It guarantees that after
     * the return of this method, the target buffers can be released. In a data region, all data of
     * the same subpartition must be written together.
     *
     * <p>Note: The caller is responsible for recycling the target buffers and releasing the failed
     * {@link PartitionedFile} if any exception occurs.
     * 写入数据
     */
    public void writeBuffers(List<BufferWithChannel> bufferWithChannels) throws IOException {
        checkState(!isFinished, "File writer is already finished.");
        checkState(!isClosed, "File writer is already closed.");

        if (bufferWithChannels.isEmpty()) {
            return;
        }

        numBuffers += bufferWithChannels.size();
        long expectedBytes;

        // 因为要写2类数据  所以 * 2
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * bufferWithChannels.size()];

        // 根据情况使用不同写入方式
        if (isBroadcastRegion) {
            expectedBytes = collectBroadcastBuffers(bufferWithChannels, bufferWithHeaders);
        } else {
            expectedBytes = collectUnicastBuffers(bufferWithChannels, bufferWithHeaders);
        }

        totalBytesWritten += expectedBytes;
        BufferReaderWriterUtil.writeBuffers(dataFileChannel, expectedBytes, bufferWithHeaders);
    }

    /**
     * 创建普通buffer
     * @param bufferWithChannels
     * @param bufferWithHeaders  存储header和buffer的容器
     * @return
     */
    private long collectUnicastBuffers(
            List<BufferWithChannel> bufferWithChannels, ByteBuffer[] bufferWithHeaders) {
        long expectedBytes = 0;
        long fileOffset = totalBytesWritten;
        for (int i = 0; i < bufferWithChannels.size(); i++) {
            int subpartition = bufferWithChannels.get(i).getChannelIndex();
            if (subpartition != currentSubpartition) {
                checkState(
                        subpartitionBytes[subpartition] == 0,
                        "Must write data of the same channel together.");
                subpartitionOffsets[subpartition] = fileOffset;
                currentSubpartition = subpartition;
            }

            Buffer buffer = bufferWithChannels.get(i).getBuffer();
            // 写入数据
            int numBytes = setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
            expectedBytes += numBytes;
            fileOffset += numBytes;
            subpartitionBytes[subpartition] += numBytes;
        }
        return expectedBytes;
    }

    /**
     * 添加广播数据
     * @param bufferWithChannels
     * @param bufferWithHeaders
     * @return
     */
    private long collectBroadcastBuffers(
            List<BufferWithChannel> bufferWithChannels, ByteBuffer[] bufferWithHeaders) {
        // set the file offset of all channels as the current file size on the first call
        // 0号子分区代表广播
        if (subpartitionBytes[0] == 0) {
            for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                // 准备阶段  所有子分区设置相同的起点
                subpartitionOffsets[subpartition] = totalBytesWritten;
            }
        }

        long expectedBytes = 0;
        // 这里所有数据都是广播数据    上面的api 一个channel对应一个子分区数据
        for (int i = 0; i < bufferWithChannels.size(); i++) {
            Buffer buffer = bufferWithChannels.get(i).getBuffer();
            int numBytes = setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
            expectedBytes += numBytes;
        }

        // 所有子分区增加相同的量
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            subpartitionBytes[subpartition] += expectedBytes;
        }
        return expectedBytes;
    }

    /**
     * 表示buffer的数据要写入到bufferWithHeaders 中
     * @param buffer
     * @param bufferWithHeaders
     * @param index
     * @return
     */
    private int setBufferWithHeader(Buffer buffer, ByteBuffer[] bufferWithHeaders, int index) {
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, header);

        bufferWithHeaders[index] = header;
        bufferWithHeaders[index + 1] = buffer.getNioBufferReadable();

        return header.remaining() + buffer.readableBytes();
    }

    /**
     * Finishes writing the {@link PartitionedFile} which closes the file channel and returns the
     * corresponding {@link PartitionedFile}.
     *
     * <p>Note: The caller is responsible for releasing the failed {@link PartitionedFile} if any
     * exception occurs.
     * 表示写入完毕 产生分区数据文件
     */
    public PartitionedFile finish() throws IOException {
        checkState(!isFinished, "File writer is already finished.");
        checkState(!isClosed, "File writer is already closed.");

        isFinished = true;

        // 写入region索引数据
        writeRegionIndex();
        // 索引数据刷盘
        flushIndexBuffer();
        indexBuffer.rewind();

        long dataFileSize = dataFileChannel.size();
        long indexFileSize = indexFileChannel.size();
        close();

        ByteBuffer indexEntryCache = null;
        if (allIndexEntriesCached) {
            indexEntryCache = indexBuffer;
        }
        indexBuffer = null;
        return new PartitionedFile(
                numRegions,
                numSubpartitions,
                dataFilePath,
                indexFilePath,
                dataFileSize,
                indexFileSize,
                numBuffers,
                // 携带索引数据初始化
                indexEntryCache);
    }

    /** Used to close and delete the failed {@link PartitionedFile} when any exception occurs. */
    public void releaseQuietly() {
        IOUtils.closeQuietly(this);
        IOUtils.deleteFileQuietly(dataFilePath);
        IOUtils.deleteFileQuietly(indexFilePath);
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        isClosed = true;

        IOException exception = null;
        try {
            dataFileChannel.close();
        } catch (IOException ioException) {
            exception = ioException;
        }

        try {
            indexFileChannel.close();
        } catch (IOException ioException) {
            exception = ExceptionUtils.firstOrSuppressed(ioException, exception);
        }

        if (exception != null) {
            throw exception;
        }
    }
}
