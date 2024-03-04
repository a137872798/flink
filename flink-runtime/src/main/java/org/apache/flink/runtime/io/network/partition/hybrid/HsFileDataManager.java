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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * File data manager for HsResultPartition, which schedules {@link HsSubpartitionFileReaderImpl} for
 * loading data w.r.t. their offset in the file.
 * 管理文件数据
 */
@ThreadSafe
public class HsFileDataManager implements Runnable, BufferRecycler {
    private static final Logger LOG = LoggerFactory.getLogger(HsFileDataManager.class);

    /** Executor to run the shuffle data reading task.
     * */
    private final ScheduledExecutorService ioExecutor;

    /** Maximum number of buffers can be allocated by this partition reader. */
    private final int maxRequestedBuffers;

    /**
     * Maximum time to wait when requesting read buffers from the buffer pool before throwing an
     * exception.
     */
    private final Duration bufferRequestTimeout;

    /** Lock used to synchronize multi-thread access to thread-unsafe fields. */
    private final Object lock = new Object();

    /**
     * A {@link CompletableFuture} to be completed when this data manager including all resources is
     * released.
     */
    @GuardedBy("lock")
    private final CompletableFuture<?> releaseFuture = new CompletableFuture<>();

    /** Buffer pool from which to allocate buffers for shuffle data reading. */
    private final BatchShuffleReadBufferPool bufferPool;

    private final Path dataFilePath;

    /**
     * 存储的是索引数据 根据bufferIndex 可以找到region
     */
    private final HsFileDataIndex dataIndex;

    private final HsSubpartitionFileReader.Factory fileReaderFactory;

    private final HybridShuffleConfiguration hybridShuffleConfiguration;

    private final ByteBuffer headerBuf = BufferReaderWriterUtil.allocatedHeaderBuffer();

    /** All readers waiting to read data of different subpartitions.
     * 每个子分区有一个reader对象
     * */
    @GuardedBy("lock")
    private final Set<HsSubpartitionFileReader> allReaders = new HashSet<>();

    /**
     * Whether the data reading task is currently running or not. This flag is used when trying to
     * submit the data reading task.
     */
    @GuardedBy("lock")
    private boolean isRunning;

    /** Number of buffers already allocated and still not recycled by this partition reader. */
    @GuardedBy("lock")
    private volatile int numRequestedBuffers;

    /** Whether this file data manager has been released or not. */
    @GuardedBy("lock")
    private volatile boolean isReleased;

    @GuardedBy("lock")
    private FileChannel dataFileChannel;

    public HsFileDataManager(
            BatchShuffleReadBufferPool bufferPool,
            ScheduledExecutorService ioExecutor,
            HsFileDataIndex dataIndex,
            Path dataFilePath,
            HsSubpartitionFileReader.Factory fileReaderFactory,
            HybridShuffleConfiguration hybridShuffleConfiguration) {
        this.fileReaderFactory = fileReaderFactory;
        this.hybridShuffleConfiguration = checkNotNull(hybridShuffleConfiguration);
        this.dataIndex = checkNotNull(dataIndex);
        this.dataFilePath = checkNotNull(dataFilePath);
        this.bufferPool = checkNotNull(bufferPool);
        this.ioExecutor = checkNotNull(ioExecutor);
        this.maxRequestedBuffers = hybridShuffleConfiguration.getMaxRequestedBuffers();
        this.bufferRequestTimeout =
                checkNotNull(hybridShuffleConfiguration.getBufferRequestTimeout());
    }

    /** Setup read buffer pool. */
    public void setup() {
        bufferPool.initialize();
    }

    @Override
    // Note, this method is synchronized on `this`, not `lock`. The purpose here is to prevent
    // concurrent `run()` executions. Concurrent calls to other methods are allowed.
    public synchronized void run() {
        // 申请buffer 让reader完成准备工作 触发所有reader的读取
        // 返回的表示使用了多少buffer
        int numBuffersRead = tryRead();
        endCurrentRoundOfReading(numBuffersRead);
    }

    /** This method only called by result partition to create subpartitionFileReader.
     * 追加一个reader对象
     * */
    public HsDataView registerNewConsumer(
            int subpartitionId,
            HsConsumerId consumerId,
            HsSubpartitionConsumerInternalOperations operation)
            throws IOException {
        synchronized (lock) {
            checkState(!isReleased, "HsFileDataManager is already released.");
            lazyInitialize();

            HsSubpartitionFileReader subpartitionReader =
                    fileReaderFactory.createFileReader(
                            subpartitionId,
                            consumerId,
                            dataFileChannel,
                            operation,
                            dataIndex,
                            hybridShuffleConfiguration.getMaxBuffersReadAhead(),
                            this::releaseSubpartitionReader,
                            headerBuf);

            allReaders.add(subpartitionReader);

            // 因为有了新的reader 立即触发读取
            mayTriggerReading();
            return subpartitionReader;
        }
    }

    public void closeDataIndexAndDeleteShuffleFile() {
        // 关闭索引对象
        dataIndex.close();
        IOUtils.deleteFileQuietly(dataFilePath);
    }

    /**
     * Release specific {@link HsSubpartitionFileReader} from {@link HsFileDataManager}.
     *
     * @param subpartitionFileReader to release.
     */
    public void releaseSubpartitionReader(HsSubpartitionFileReader subpartitionFileReader) {
        synchronized (lock) {
            removeSubpartitionReaders(Collections.singleton(subpartitionFileReader));
        }
    }

    /** Releases this file data manager and delete shuffle data after all readers is removed.
     * 释放本对象
     * */
    public void release() {
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;

            List<HsSubpartitionFileReader> pendingReaders = new ArrayList<>(allReaders);
            mayNotifyReleased();
            // 以失败方式触发所有reader
            failSubpartitionReaders(
                    pendingReaders,
                    new IllegalStateException("Result partition has been already released."));
            // close data index and delete shuffle file only when no reader is reading now.
            releaseFuture.thenRun(this::closeDataIndexAndDeleteShuffleFile);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    /** @return number of buffers read.
     * 触发数据读取
     * */
    private int tryRead() {

        // 所有reader做好准备工作
        Queue<HsSubpartitionFileReader> availableReaders = prepareAndGetAvailableReaders();
        if (availableReaders.isEmpty()) {
            return 0;
        }

        Queue<MemorySegment> buffers;
        try {
            // 申请buffer
            buffers = allocateBuffers();
        } catch (Exception exception) {
            // fail all pending subpartition readers immediately if any exception occurs
            failSubpartitionReaders(availableReaders, exception);
            LOG.error("Failed to request buffers for data reading.", exception);
            return 0;
        }

        int numBuffersAllocated = buffers.size();
        if (numBuffersAllocated <= 0) {
            return 0;
        }

        // 申请完buffer后 通过reader将数据读取进去
        readData(availableReaders, buffers);
        int numBuffersRead = numBuffersAllocated - buffers.size();

        // 剩余的buffer就是多出来的  归还即可
        releaseBuffers(buffers);

        return numBuffersRead;
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // read-only access to volatile isReleased and numRequestedBuffers
    // 申请buffer
    private Queue<MemorySegment> allocateBuffers() throws Exception {
        long timeoutTime = getBufferRequestTimeoutTime();
        do {
            List<MemorySegment> buffers = bufferPool.requestBuffers();
            if (!buffers.isEmpty()) {
                return new ArrayDeque<>(buffers);
            }
            checkState(!isReleased, "Result partition has been already released.");
        } while (System.currentTimeMillis() < timeoutTime
                || System.currentTimeMillis() < (timeoutTime = getBufferRequestTimeoutTime()));

        // This is a safe net against potential deadlocks.
        //
        // A deadlock can happen when the downstream task needs to consume multiple result
        // partitions (e.g., A and B) in specific order (cannot consume B before finishing
        // consuming A). Since the reading buffer pool is shared across the TM, if B happens to
        // take all the buffers, A cannot be consumed due to lack of buffers, which also blocks
        // B from being consumed and releasing the buffers.
        //
        // The imperfect solution here is to fail all the subpartitionReaders (A), which
        // consequently fail all the downstream tasks, unregister their other
        // subpartitionReaders (B) and release the read buffers.
        throw new TimeoutException(
                String.format(
                        "Buffer request timeout, this means there is a fierce contention of"
                                + " the batch shuffle read memory, please increase '%s'.",
                        TaskManagerOptions.NETWORK_BATCH_SHUFFLE_READ_MEMORY.key()));
    }

    /**
     * 记录触发read
     */
    private void mayTriggerReading() {
        synchronized (lock) {
            if (!isRunning
                    && !allReaders.isEmpty()
                    && numRequestedBuffers + bufferPool.getNumBuffersPerRequest()
                            <= maxRequestedBuffers
                    && numRequestedBuffers < bufferPool.getAverageBuffersPerRequester()) {
                isRunning = true;
                ioExecutor.execute(
                        () -> {
                            try {
                                run();
                            } catch (Throwable throwable) {
                                // handle un-expected exception as unhandledExceptionHandler is not
                                // worked for ScheduledExecutorService.
                                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                                        Thread.currentThread(), throwable);
                            }
                        });
            }
        }
    }

    @GuardedBy("lock")
    private void mayNotifyReleased() {
        assert Thread.holdsLock(lock);

        if (isReleased && allReaders.isEmpty()) {
            releaseFuture.complete(null);
        }
    }

    private long getBufferRequestTimeoutTime() {
        return bufferPool.getLastBufferOperationTimestamp() + bufferRequestTimeout.toMillis();
    }

    private void releaseBuffers(Queue<MemorySegment> buffers) {
        if (!buffers.isEmpty()) {
            try {
                bufferPool.recycle(buffers);
                buffers.clear();
            } catch (Throwable throwable) {
                // this should never happen so just trigger fatal error
                FatalExitExceptionHandler.INSTANCE.uncaughtException(
                        Thread.currentThread(), throwable);
            }
        }
    }

    /**
     * 触发所有reader的 prepareForScheduling
     * @return
     */
    private Queue<HsSubpartitionFileReader> prepareAndGetAvailableReaders() {
        synchronized (lock) {
            if (isReleased) {
                return new ArrayDeque<>();
            }

            for (HsSubpartitionFileReader reader : allReaders) {
                reader.prepareForScheduling();
            }
            return new PriorityQueue<>(allReaders);
        }
    }

    /**
     * 利用reader 将数据读取到buffer
     * @param availableReaders
     * @param buffers
     */
    private void readData(
            Queue<HsSubpartitionFileReader> availableReaders, Queue<MemorySegment> buffers) {
        while (!availableReaders.isEmpty() && !buffers.isEmpty()) {
            HsSubpartitionFileReader subpartitionReader = availableReaders.poll();
            try {
                subpartitionReader.readBuffers(buffers, this);
            } catch (IOException throwable) {
                failSubpartitionReaders(Collections.singletonList(subpartitionReader), throwable);
                LOG.debug("Failed to read shuffle data.", throwable);
            }
        }
    }

    private void failSubpartitionReaders(
            Collection<HsSubpartitionFileReader> readers, Throwable failureCause) {
        synchronized (lock) {
            removeSubpartitionReaders(readers);
        }

        for (HsSubpartitionFileReader reader : readers) {
            reader.fail(failureCause);
        }
    }

    /**
     * 移除相关的reader
     * @param readers
     */
    @GuardedBy("lock")
    private void removeSubpartitionReaders(Collection<HsSubpartitionFileReader> readers) {
        allReaders.removeAll(readers);
        if (allReaders.isEmpty()) {
            bufferPool.unregisterRequester(this);
            closeFileChannel();
        }
    }

    /**
     * 当前这一轮read结束时触发
     * @param numBuffersRead
     */
    private void endCurrentRoundOfReading(int numBuffersRead) {
        synchronized (lock) {
            numRequestedBuffers += numBuffersRead;
            isRunning = false;
            // 检查是否触发了 release
            mayNotifyReleased();
        }
        // 本次没有读取到任何数据 代表某些条件暂时不满足  稍作等待后重试
        if (numBuffersRead == 0) {
            // When fileReader has no data to read, for example, most of the data is
            // consumed from memory. HsFileDataManager will encounter busy-loop
            // problem, which will lead to a meaningless surge in CPU utilization
            // and seriously affect performance.
            ioExecutor.schedule(this::mayTriggerReading, 5, TimeUnit.MILLISECONDS);
        } else {
            // 立即触发read
            mayTriggerReading();
        }
    }

    @GuardedBy("lock")
    private void lazyInitialize() throws IOException {
        assert Thread.holdsLock(lock);
        try {
            if (allReaders.isEmpty()) {
                dataFileChannel = openFileChannel(dataFilePath);
                bufferPool.registerRequester(this);
            }
        } catch (IOException exception) {
            if (allReaders.isEmpty()) {
                bufferPool.unregisterRequester(this);
                closeFileChannel();
            }
            throw exception;
        }
    }

    private FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    @GuardedBy("lock")
    private void closeFileChannel() {
        assert Thread.holdsLock(lock);

        IOUtils.closeQuietly(dataFileChannel);
        dataFileChannel = null;
    }

    // ------------------------------------------------------------------------
    //  Implementation Methods of BufferRecycler
    // ------------------------------------------------------------------------

    @Override
    public void recycle(MemorySegment segment) {
        synchronized (lock) {
            bufferPool.recycle(segment);
            --numRequestedBuffers;

            mayTriggerReading();
        }
    }
}
