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
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of the ResultSubpartition for a bounded result transferred in a blocking
 * manner: The result is first produced, then consumed. The result can be consumed possibly multiple
 * times.
 *
 * <p>Depending on the supplied implementation of {@link BoundedData}, the actual data is stored for
 * example in a file, or in a temporary memory mapped file.
 *
 * <h2>Important Notes on Thread Safety</h2>
 *
 * <p>This class does not synchronize every buffer access. It assumes the threading model of the
 * Flink network stack and is not thread-safe beyond that.
 *
 * <p>This class assumes a single writer thread that adds buffers, flushes, and finishes the write
 * phase. That same thread is also assumed to perform the partition release, if the release happens
 * during the write phase.
 *
 * <p>The implementation supports multiple concurrent readers, but assumes a single thread per
 * reader. That same thread must also release the reader. In particular, after the reader was
 * released, no buffers obtained from this reader may be accessed any more, or segmentation faults
 * might occur in some implementations.
 *
 * <p>The method calls to create readers, dispose readers, and dispose the partition are thread-safe
 * vis-a-vis each other.
 *
 * 表示存储子分区数据的对象  并且存储的是算子产生的结果数据   该数据会被下游消费 (下游会拉取数据)
 */
final class BoundedBlockingSubpartition extends ResultSubpartition {

    /** This lock guards the creation of readers and disposal of the memory mapped file. */
    private final Object lock = new Object();

    /** The current buffer, may be filled further over time.
     * 该对象维护了pos
     * */
    @Nullable private BufferConsumer currentBuffer;

    /** The bounded data store that we store the data in.
     * BoundedData 作为子分区数据载体
     * */
    private final BoundedData data;

    /** All created and not yet released readers.
     * 针对同一份数据 可以创建多个reader对象 只要单独维护pos即可
     * */
    @GuardedBy("lock")
    private final Set<ResultSubpartitionView> readers;

    /**
     * Flag to transfer file via FileRegion way in network stack if partition type is file without
     * SSL enabled.
     * 是否直接读取文件
     */
    private final boolean useDirectFileTransfer;

    /** Counter for the number of data buffers (not events!) written.
     * 记录写入了多少data buffer  注意不包含events
     * */
    private int numDataBuffersWritten;

    /** The counter for the number of data buffers and events.
     * 包含buffer 和events
     * */
    private int numBuffersAndEventsWritten;

    /** Flag indicating whether the writing has finished and this is now available for read.
     * finished代表数据已经全部生成完  并且可以进行读取了
     * */
    private boolean isFinished;

    /** Flag indicating whether the subpartition has been released.
     * 本对象已经被关闭
     * */
    private boolean isReleased;

    /**
     *
     * @param index  子分区下标
     * @param parent   父分区下标
     * @param data
     * @param useDirectFileTransfer
     */
    public BoundedBlockingSubpartition(
            int index, ResultPartition parent, BoundedData data, boolean useDirectFileTransfer) {

        super(index, parent);

        this.data = checkNotNull(data);
        this.useDirectFileTransfer = useDirectFileTransfer;
        this.readers = new HashSet<>();
    }

    // ------------------------------------------------------------------------

    /**
     * Checks if writing is finished. Readers cannot be created until writing is finished, and no
     * further writes can happen after that.
     */
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    /**
     * @param bufferConsumer the buffer to add (transferring ownership to this writer)    对象内部是有buffer的
     * @param partialRecordLength the length of bytes to skip in order to start with a complete
     *     record, from position index 0 of the underlying {@cite MemorySegment}.
     * @return
     * @throws IOException
     */
    @Override
    public int add(BufferConsumer bufferConsumer, int partialRecordLength) throws IOException {
        // 本子分区已经禁止写入了    关闭consumer内的buffer
        if (isFinished()) {
            bufferConsumer.close();
            return ADD_BUFFER_ERROR_CODE;
        }

        // 因为要切换buffer了  先将之前的刷盘
        flushCurrentBuffer();
        // 更换当前buffer
        currentBuffer = bufferConsumer;
        return Integer.MAX_VALUE;
    }

    @Override
    public void flush() {
        // unfortunately, the signature of flush does not allow for any exceptions, so we
        // need to do this discouraged pattern of runtime exception wrapping
        try {
            flushCurrentBuffer();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e.getMessage(), e);
        }
    }

    /**
     * 当前数据刷盘
     * @throws IOException
     */
    private void flushCurrentBuffer() throws IOException {
        if (currentBuffer != null) {
            writeAndCloseBufferConsumer(currentBuffer);
            currentBuffer = null;
        }
    }

    private void writeAndCloseBufferConsumer(BufferConsumer bufferConsumer) throws IOException {
        try {
            final Buffer buffer = bufferConsumer.build();
            try {
                // 判断能否压缩数据
                if (parent.canBeCompressed(buffer)) {
                    final Buffer compressedBuffer =
                            parent.bufferCompressor.compressToIntermediateBuffer(buffer);

                    // 将压缩后的数据转移到 data 中
                    data.writeBuffer(compressedBuffer);
                    if (compressedBuffer != buffer) {
                        compressedBuffer.recycleBuffer();
                    }
                } else {
                    // 否则直接写入
                    data.writeBuffer(buffer);
                }

                // 增加计数值
                numBuffersAndEventsWritten++;
                if (buffer.isBuffer()) {
                    numDataBuffersWritten++;
                }
            } finally {
                buffer.recycleBuffer();
            }
        } finally {
            bufferConsumer.close();
        }
    }

    /**
     * 表示写入结束了  内部的数据可以用于读取了
     * @return
     * @throws IOException
     */
    @Override
    public int finish() throws IOException {
        checkState(!isReleased, "data partition already released");
        checkState(!isFinished, "data partition already finished");

        isFinished = true;
        flushCurrentBuffer();
        BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE, false);
        writeAndCloseBufferConsumer(eventBufferConsumer);
        data.finishWrite();
        return eventBufferConsumer.getWrittenBytes();
    }

    /**
     * 释放该对象
     * @throws IOException
     */
    @Override
    public void release() throws IOException {
        synchronized (lock) {
            if (isReleased) {
                return;
            }

            isReleased = true;
            isFinished = true; // for fail fast writes

            if (currentBuffer != null) {
                currentBuffer.close();
                currentBuffer = null;
            }
            checkReaderReferencesAndDispose();
        }
    }

    /**
     * 创建该子分区对应的视图   (也就是reader对象)
     * @param availability  每当有新数据可用时 触发监听器
     * @return
     * @throws IOException
     */
    @Override
    public ResultSubpartitionView createReadView(BufferAvailabilityListener availability)
            throws IOException {
        synchronized (lock) {
            checkState(!isReleased, "data partition already released");
            checkState(isFinished, "writing of blocking partition not yet finished");

            if (!Files.isReadable(data.getFilePath())) {
                throw new PartitionNotFoundException(parent.getPartitionId());
            }

            final ResultSubpartitionView reader;

            // 直接从文件读取数据
            if (useDirectFileTransfer) {
                reader =
                        new BoundedBlockingSubpartitionDirectTransferReader(
                                this,
                                data.getFilePath(),
                                numDataBuffersWritten,
                                numBuffersAndEventsWritten);
            } else {
                // 还是会先将数据读取到缓冲区
                reader =
                        new BoundedBlockingSubpartitionReader(
                                this, data, numDataBuffersWritten, availability);
            }

            // 针对同一份数据可以创建多个reader对象
            readers.add(reader);
            return reader;
        }
    }

    /**
     * 表示某个reader对象被释放了
     * @param reader
     * @throws IOException
     */
    void releaseReaderReference(ResultSubpartitionView reader) throws IOException {
        onConsumedSubpartition();

        synchronized (lock) {
            if (readers.remove(reader) && isReleased) {
                checkReaderReferencesAndDispose();
            }
        }
    }

    /**
     * 只有所有reader对象都读取完后 才能释放数据
     * @throws IOException
     */
    @GuardedBy("lock")
    private void checkReaderReferencesAndDispose() throws IOException {
        assert Thread.holdsLock(lock);

        // To avoid lingering memory mapped files (large resource footprint), we don't
        // wait for GC to unmap the files, but use a Netty utility to directly unmap the file.
        // To avoid segmentation faults, we need to wait until all readers have been released.

        if (readers.isEmpty()) {
            data.close();
        }
    }

    @VisibleForTesting
    public BufferConsumer getCurrentBuffer() {
        return currentBuffer;
    }

    // ---------------------------- statistics --------------------------------

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public void bufferSize(int desirableNewBufferSize) {
        // not supported.
    }

    @Override
    protected long getTotalNumberOfBuffersUnsafe() {
        return numBuffersAndEventsWritten;
    }

    @Override
    protected long getTotalNumberOfBytesUnsafe() {
        return data.getSize();
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) {
        // Nothing to do.
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        // Nothing to do.
    }

    int getBuffersInBacklogUnsafe() {
        return numDataBuffersWritten;
    }

    // ---------------------------- factories --------------------------------

    /**
     * Creates a BoundedBlockingSubpartition that simply stores the partition data in a file. Data
     * is eagerly spilled (written to disk) and readers directly read from the file.
     * 根据相关信息创建子分区
     */
    public static BoundedBlockingSubpartition createWithFileChannel(
            int index,
            ResultPartition parent,
            File tempFile,
            int readBufferSize,
            boolean sslEnabled)
            throws IOException {

        final FileChannelBoundedData bd =
                FileChannelBoundedData.create(tempFile.toPath(), readBufferSize);
        return new BoundedBlockingSubpartition(index, parent, bd, !sslEnabled);
    }

    /**
     * Creates a BoundedBlockingSubpartition that stores the partition data in memory mapped file.
     * Data is written to and read from the mapped memory region. Disk spilling happens lazily, when
     * the OS swaps out the pages from the memory mapped file.
     */
    public static BoundedBlockingSubpartition createWithMemoryMappedFile(
            int index, ResultPartition parent, File tempFile) throws IOException {

        final MemoryMappedBoundedData bd = MemoryMappedBoundedData.create(tempFile.toPath());
        return new BoundedBlockingSubpartition(index, parent, bd, false);
    }

    /**
     * Creates a BoundedBlockingSubpartition that stores the partition data in a file and memory
     * maps that file for reading. Data is eagerly spilled (written to disk) and then mapped into
     * memory. The main difference to the {@link #createWithMemoryMappedFile(int, ResultPartition,
     * File)} variant is that no I/O is necessary when pages from the memory mapped file are
     * evicted.
     */
    public static BoundedBlockingSubpartition createWithFileAndMemoryMappedReader(
            int index, ResultPartition parent, File tempFile) throws IOException {

        final FileChannelMemoryMappedBoundedData bd =
                FileChannelMemoryMappedBoundedData.create(tempFile.toPath());
        return new BoundedBlockingSubpartition(index, parent, bd, false);
    }
}
