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
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SortMergeResultPartition} appends records and events to {@link DataBuffer} and after the
 * {@link DataBuffer} is full, all data in the {@link DataBuffer} will be copied and spilled to a
 * {@link PartitionedFile} in subpartition index order sequentially. Large records that can not be
 * appended to an empty {@link DataBuffer} will be spilled to the result {@link PartitionedFile}
 * separately.
 * 存放分区数据的对象
 */
@NotThreadSafe
public class SortMergeResultPartition extends ResultPartition {

    /**
     * Number of expected buffer size to allocate for data writing. Currently, it is an empirical
     * value (8M) which can not be configured.
     */
    private static final int NUM_WRITE_BUFFER_BYTES = 8 * 1024 * 1024;

    /**
     * Expected number of buffers for data batch writing. 512 mean that at most 1024 buffers
     * (including the headers) will be written in one request. This value is selected because that
     * the writev system call has a limit on the maximum number of buffers can be written in one
     * invoke whose advertised value is 1024 (please see writev man page for more information).
     */
    private static final int EXPECTED_WRITE_BATCH_SIZE = 512;

    private final Object lock = new Object();

    /** {@link PartitionedFile} produced by this result partition.
     * 该文件按照分区存储数据
     * */
    @GuardedBy("lock")
    private PartitionedFile resultFile;

    private boolean hasNotifiedEndOfUserRecords;

    /** Size of network buffer and write buffer.
     * 表示单个buffer的大小
     * */
    private final int networkBufferSize;

    /** File writer for this result partition.
     * 通过该对象生成分区文件
     * */
    @GuardedBy("lock")
    private PartitionedFileWriter fileWriter;

    /**
     * Selected storage path to be used by this result partition to store shuffle data file and
     * index file.
     */
    private final String resultFileBasePath;

    /** Subpartition orders of coping data from {@link DataBuffer} and writing to file.
     * 用于对子分区进行重排序
     * */
    private final int[] subpartitionOrder;

    /**
     * A shared buffer pool to allocate buffers from when reading data from this result partition.
     * 提供内存块的pool对象
     */
    private final BatchShuffleReadBufferPool readBufferPool;

    /**
     * Data read scheduler for this result partition which schedules data read of all subpartitions.
     * 相关的调度器对象
     */
    private final SortMergeResultPartitionReadScheduler readScheduler;

    /** All available network buffers can be used by this result partition for a data region.
     * 存放可用的内存块
     * */
    private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

    /**
     * Number of guaranteed network buffers can be used by {@link #unicastDataBuffer} and {@link
     * #broadcastDataBuffer}.
     */
    private int numBuffersForSort;

    /**
     * If true, {@link HashBasedDataBuffer} will be used, otherwise, {@link SortBasedDataBuffer}
     * will be used.
     */
    private boolean useHashBuffer;

    // 普通数据和广播数据分开存放

    /** {@link DataBuffer} for records sent by {@link #broadcastRecord(ByteBuffer)}. */
    private DataBuffer broadcastDataBuffer;

    /** {@link DataBuffer} for records sent by {@link #emitRecord(ByteBuffer, int)}. */
    private DataBuffer unicastDataBuffer;

    public SortMergeResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            BatchShuffleReadBufferPool readBufferPool,
            Executor readIOExecutor,
            ResultPartitionManager partitionManager,
            String resultFileBasePath,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                numSubpartitions,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.resultFileBasePath = checkNotNull(resultFileBasePath);
        this.readBufferPool = checkNotNull(readBufferPool);
        this.networkBufferSize = readBufferPool.getBufferSize();
        // because IO scheduling will always try to read data in file offset order for better IO
        // performance, when writing data to file, we use a random subpartition order to avoid
        // reading the output of all upstream tasks in the same order, which is better for data
        // input balance of the downstream tasks
        // 打乱子分区顺序
        this.subpartitionOrder = getRandomSubpartitionOrder(numSubpartitions);

        // 生成调度器对象
        this.readScheduler =
                new SortMergeResultPartitionReadScheduler(readBufferPool, readIOExecutor, lock);
    }

    /**
     * 启动该分区时进行初始化
     * @throws IOException
     */
    @Override
    protected void setupInternal() throws IOException {
        synchronized (lock) {
            if (isReleased()) {
                throw new IOException("Result partition has been released.");
            }
            try {
                // allocate at most 4M heap memory for caching of index entries
                // 初始化写对象
                fileWriter =
                        new PartitionedFileWriter(numSubpartitions, 4194304, resultFileBasePath);
            } catch (Throwable throwable) {
                throw new IOException("Failed to create file writer.", throwable);
            }
        }

        // reserve the "guaranteed" buffers for this buffer pool to avoid the case that those
        // buffers are taken by other result partitions and can not be released, which may cause
        // deadlock
        requestGuaranteedBuffers();

        // initialize the buffer pool eagerly to avoid reporting errors such as OOM too late
        readBufferPool.initialize();
        LOG.info("Sort-merge partition {} initialized.", getPartitionId());
    }

    /**
     * 进行清理工作
     */
    @Override
    protected void releaseInternal() {
        synchronized (lock) {
            if (resultFile == null && fileWriter != null) {
                fileWriter.releaseQuietly();
            }
        }

        // delete the produced file only when no reader is reading now
        readScheduler
                .release()
                .thenRun(
                        () -> {
                            synchronized (lock) {
                                if (resultFile != null) {
                                    resultFile.deleteQuietly();
                                    resultFile = null;
                                }
                            }
                        });
    }

    /**
     * 往某个子分区添加一条数据
     * @param record
     * @param targetSubpartition
     * @throws IOException
     */
    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        emit(record, targetSubpartition, DataType.DATA_BUFFER, false);
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        broadcast(record, DataType.DATA_BUFFER);
    }

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        Buffer buffer = EventSerializer.toBuffer(event, isPriorityEvent);
        try {
            ByteBuffer serializedEvent = buffer.getNioBufferReadable();
            broadcast(serializedEvent, buffer.getDataType());
        } finally {
            buffer.recycleBuffer();
        }
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        // Nothing to do.
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        // Nothing to do.
    }

    private void broadcast(ByteBuffer record, DataType dataType) throws IOException {
        emit(record, 0, dataType, true);
    }

    /**
     * 写入数据
     * @param record
     * @param targetSubpartition
     * @param dataType
     * @param isBroadcast
     * @throws IOException
     */
    private void emit(
            ByteBuffer record, int targetSubpartition, DataType dataType, boolean isBroadcast)
            throws IOException {
        checkInProduceState();

        // 找到不同的buffer
        DataBuffer dataBuffer = isBroadcast ? getBroadcastDataBuffer() : getUnicastDataBuffer();

        // 写入数据  append返回true 表示数据没有写完
        if (!dataBuffer.append(record, targetSubpartition, dataType)) {
            return;
        }

        // 表示当前buffer空间不够  本次的record比较大 调用writeLargeRecord进行写入
        if (!dataBuffer.hasRemaining()) {
            dataBuffer.release();
            // 将数据直接写入
            writeLargeRecord(record, targetSubpartition, dataType, isBroadcast);
            return;
        }

        // buffer空间足够  处理buffer内的数据
        flushDataBuffer(dataBuffer, isBroadcast);
        dataBuffer.release();
        if (record.hasRemaining()) {
            emit(record, targetSubpartition, dataType, isBroadcast);
        }
    }

    private void releaseDataBuffer(DataBuffer dataBuffer) {
        if (dataBuffer != null) {
            dataBuffer.release();
        }
    }

    private DataBuffer getUnicastDataBuffer() throws IOException {
        flushBroadcastDataBuffer();

        if (unicastDataBuffer != null
                && !unicastDataBuffer.isFinished()
                && !unicastDataBuffer.isReleased()) {
            return unicastDataBuffer;
        }

        unicastDataBuffer = createNewDataBuffer();
        return unicastDataBuffer;
    }

    private DataBuffer getBroadcastDataBuffer() throws IOException {
        flushUnicastDataBuffer();

        if (broadcastDataBuffer != null
                && !broadcastDataBuffer.isFinished()
                && !broadcastDataBuffer.isReleased()) {
            return broadcastDataBuffer;
        }

        broadcastDataBuffer = createNewDataBuffer();
        return broadcastDataBuffer;
    }

    private DataBuffer createNewDataBuffer() throws IOException {
        requestNetworkBuffers();

        if (useHashBuffer) {
            return new HashBasedDataBuffer(
                    freeSegments,
                    bufferPool,
                    numSubpartitions,
                    networkBufferSize,
                    numBuffersForSort,
                    subpartitionOrder);
        } else {
            return new SortBasedDataBuffer(
                    freeSegments,
                    bufferPool,
                    numSubpartitions,
                    networkBufferSize,
                    numBuffersForSort,
                    subpartitionOrder);
        }
    }

    /**
     * 申请内存块 存入到freeSegments中
     * @throws IOException
     */
    private void requestGuaranteedBuffers() throws IOException {
        int numRequiredBuffer = bufferPool.getNumberOfRequiredMemorySegments();
        if (numRequiredBuffer < 2) {
            throw new IOException(
                    String.format(
                            "Too few sort buffers, please increase %s.",
                            NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS));
        }

        try {
            while (freeSegments.size() < numRequiredBuffer) {
                freeSegments.add(checkNotNull(bufferPool.requestMemorySegmentBlocking()));
            }
        } catch (InterruptedException exception) {
            releaseFreeBuffers();
            throw new IOException("Failed to allocate buffers for result partition.", exception);
        }
    }

    /**
     * 初始化 networkBuffer
     * @throws IOException
     */
    private void requestNetworkBuffers() throws IOException {
        requestGuaranteedBuffers();

        // avoid taking too many buffers in one result partition
        while (freeSegments.size() < bufferPool.getMaxNumberOfMemorySegments()) {
            MemorySegment segment = bufferPool.requestMemorySegment();
            if (segment == null) {
                break;
            }
            freeSegments.add(segment);
        }

        useHashBuffer = false;
        int numWriteBuffers = 0;
        if (freeSegments.size() >= 2 * numSubpartitions) {
            useHashBuffer = true;
        } else if (networkBufferSize >= NUM_WRITE_BUFFER_BYTES) {
            numWriteBuffers = 1;
        } else {
            numWriteBuffers =
                    Math.min(EXPECTED_WRITE_BATCH_SIZE, NUM_WRITE_BUFFER_BYTES / networkBufferSize);
        }
        numWriteBuffers = Math.min(freeSegments.size() / 2, numWriteBuffers);
        numBuffersForSort = freeSegments.size() - numWriteBuffers;
    }

    /**
     * 数据刷盘
     * @param dataBuffer
     * @param isBroadcast
     * @throws IOException
     */
    private void flushDataBuffer(DataBuffer dataBuffer, boolean isBroadcast) throws IOException {
        if (dataBuffer == null || dataBuffer.isReleased() || !dataBuffer.hasRemaining()) {
            return;
        }
        dataBuffer.finish();

        Queue<MemorySegment> segments = new ArrayDeque<>(freeSegments);

        // 表示每当凑齐多少个buffer时  需要进行一次写入
        int numBuffersToWrite =
                useHashBuffer
                        ? EXPECTED_WRITE_BATCH_SIZE
                        : Math.min(EXPECTED_WRITE_BATCH_SIZE, segments.size());  // 本次要写入的内存块数量
        List<BufferWithChannel> toWrite = new ArrayList<>(numBuffersToWrite);

        fileWriter.startNewRegion(isBroadcast);
        do {
            if (toWrite.size() >= numBuffersToWrite) {
                writeBuffers(toWrite);
                segments = new ArrayDeque<>(freeSegments);
            }

            BufferWithChannel bufferWithChannel = dataBuffer.getNextBuffer(segments.poll());
            if (bufferWithChannel == null) {
                // 没数据了 将buffer的数据通过writer写入
                writeBuffers(toWrite);
                break;
            }

            updateStatistics(bufferWithChannel, isBroadcast);
            toWrite.add(compressBufferIfPossible(bufferWithChannel));
        } while (true);

        releaseFreeBuffers();
    }

    private void flushBroadcastDataBuffer() throws IOException {
        if (broadcastDataBuffer != null) {
            flushDataBuffer(broadcastDataBuffer, true);
            broadcastDataBuffer.release();
            broadcastDataBuffer = null;
        }
    }

    private void flushUnicastDataBuffer() throws IOException {
        if (unicastDataBuffer != null) {
            flushDataBuffer(unicastDataBuffer, false);
            unicastDataBuffer.release();
            unicastDataBuffer = null;
        }
    }

    /**
     * 尝试压缩数据
     * @param bufferWithChannel
     * @return
     */
    private BufferWithChannel compressBufferIfPossible(BufferWithChannel bufferWithChannel) {
        Buffer buffer = bufferWithChannel.getBuffer();
        if (!canBeCompressed(buffer)) {
            return bufferWithChannel;
        }

        buffer = checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
        return new BufferWithChannel(buffer, bufferWithChannel.getChannelIndex());
    }

    private void updateStatistics(BufferWithChannel bufferWithChannel, boolean isBroadcast) {
        numBuffersOut.inc(isBroadcast ? numSubpartitions : 1);
        long readableBytes = bufferWithChannel.getBuffer().readableBytes();
        if (isBroadcast) {
            resultPartitionBytes.incAll(readableBytes);
        } else {
            resultPartitionBytes.inc(bufferWithChannel.getChannelIndex(), readableBytes);
        }
        numBytesOut.inc(isBroadcast ? readableBytes * numSubpartitions : readableBytes);
    }

    /**
     * Spills the large record into the target {@link PartitionedFile} as a separate data region.
     * 直接写大数据
     */
    private void writeLargeRecord(
            ByteBuffer record, int targetSubpartition, DataType dataType, boolean isBroadcast)
            throws IOException {
        // a large record will be spilled to a separated data region
        // 生成新的region
        fileWriter.startNewRegion(isBroadcast);

        // 存放结果
        List<BufferWithChannel> toWrite = new ArrayList<>();
        Queue<MemorySegment> segments = new ArrayDeque<>(freeSegments);

        // 直到写完record的数据
        while (record.hasRemaining()) {

            // 此时没有空闲的内存块了   将囤积的内存块数据全部写入
            if (segments.isEmpty()) {
                fileWriter.writeBuffers(toWrite);
                toWrite.clear();
                // 准备重新利用内存块
                segments = new ArrayDeque<>(freeSegments);
            }

            // 表示要拷贝多少数据
            int toCopy = Math.min(record.remaining(), networkBufferSize);
            // 拉取空闲的内存块
            MemorySegment writeBuffer = checkNotNull(segments.poll());
            writeBuffer.put(0, record, toCopy);

            // 包装成buffer
            NetworkBuffer buffer = new NetworkBuffer(writeBuffer, (buf) -> {}, dataType, toCopy);
            BufferWithChannel bufferWithChannel = new BufferWithChannel(buffer, targetSubpartition);
            updateStatistics(bufferWithChannel, isBroadcast);
            toWrite.add(compressBufferIfPossible(bufferWithChannel));
        }

        // 最终写入数据
        fileWriter.writeBuffers(toWrite);
        // 回收内存块
        releaseFreeBuffers();
    }

    private void writeBuffers(List<BufferWithChannel> buffers) throws IOException {
        fileWriter.writeBuffers(buffers);
        buffers.forEach(buffer -> buffer.getBuffer().recycleBuffer());
        buffers.clear();
    }

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        if (!hasNotifiedEndOfUserRecords) {
            broadcastEvent(new EndOfData(mode), false);
            hasNotifiedEndOfUserRecords = true;
        }
    }

    /**
     * 往下游写入end数据
     * @throws IOException
     */
    @Override
    public void finish() throws IOException {
        broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
        checkState(
                unicastDataBuffer == null,
                "The unicast sort buffer should be either null or released.");
        flushBroadcastDataBuffer();

        synchronized (lock) {
            checkState(!isReleased(), "Result partition is already released.");

            resultFile = fileWriter.finish();
            super.finish();
            LOG.info("New partitioned file produced: {}.", resultFile);
        }
    }

    private void releaseFreeBuffers() {
        if (bufferPool != null) {
            freeSegments.forEach(buffer -> bufferPool.recycle(buffer));
            freeSegments.clear();
        }
    }

    @Override
    public void close() {
        releaseFreeBuffers();
        // the close method will always be called by the task thread, so there is need to make
        // the sort buffer fields volatile and visible to the cancel thread intermediately
        releaseDataBuffer(unicastDataBuffer);
        releaseDataBuffer(broadcastDataBuffer);
        super.close();

        IOUtils.closeQuietly(fileWriter);
    }

    /**
     * 产生视图  用于读取数据
     * @param subpartitionIndex
     * @param availabilityListener
     * @return
     * @throws IOException
     */
    @Override
    public ResultSubpartitionView createSubpartitionView(
            int subpartitionIndex, BufferAvailabilityListener availabilityListener)
            throws IOException {
        synchronized (lock) {
            checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
            checkState(!isReleased(), "Partition released.");
            checkState(isFinished(), "Trying to read unfinished blocking partition.");

            if (!resultFile.isReadable()) {
                throw new PartitionNotFoundException(getPartitionId());
            }

            return readScheduler.createSubpartitionReader(
                    availabilityListener, subpartitionIndex, resultFile);
        }
    }

    @Override
    public void flushAll() {}

    @Override
    public void flush(int subpartitionIndex) {}

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return AVAILABLE;
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        return 0;
    }

    /**
     * 产生一个随机顺序
     * @param numSubpartitions
     * @return
     */
    private int[] getRandomSubpartitionOrder(int numSubpartitions) {
        int[] order = new int[numSubpartitions];
        Random random = new Random();
        int shift = random.nextInt(numSubpartitions);
        for (int channel = 0; channel < numSubpartitions; ++channel) {
            order[(channel + shift) % numSubpartitions] = channel;
        }
        return order;
    }

    @VisibleForTesting
    PartitionedFile getResultFile() {
        synchronized (lock) {
            return resultFile;
        }
    }
}
