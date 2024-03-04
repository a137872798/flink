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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.ConsumeStatus;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.ConsumeStatusWithId;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.SpillStatus;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is responsible for managing the data in a single subpartition. One {@link
 * HsMemoryDataManager} will hold multiple {@link HsSubpartitionMemoryDataManager}.
 * 每个子分区关联一个
 */
public class HsSubpartitionMemoryDataManager {
    private final int targetChannel;

    private final int bufferSize;

    /**
     * 用于对 manager发起操作
     */
    private final HsMemoryDataManagerOperation memoryDataManagerOperation;

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private final Queue<BufferBuilder> unfinishedBuffers = new LinkedList<>();

    // Not guarded by lock because it is expected only accessed from task's main thread.
    private int finishedBufferIndex;

    /**
     * 使用HsBufferContext 来维护数据  还可以记录该buffer已经被哪些consumer消费过
     */
    @GuardedBy("subpartitionLock")
    private final Deque<HsBufferContext> allBuffers = new LinkedList<>();

    /**
     * key 代表 bufferIndex
     */
    @GuardedBy("subpartitionLock")
    private final Map<Integer, HsBufferContext> bufferIndexToContexts = new HashMap<>();

    /** DO NOT USE DIRECTLY. Use {@link #runWithLock} or {@link #callWithLock} instead. */
    private final Lock resultPartitionLock;

    /** DO NOT USE DIRECTLY. Use {@link #runWithLock} or {@link #callWithLock} instead. */
    private final ReentrantReadWriteLock subpartitionLock = new ReentrantReadWriteLock();

    /**
     * 每个HsSubpartitionConsumerMemoryDataManager 对应一个消费者  同一个子分区数据 可以有多个消费者进行重复消费
     */
    @GuardedBy("subpartitionLock")
    private final Map<HsConsumerId, HsSubpartitionConsumerMemoryDataManager> consumerMap;

    @Nullable private final BufferCompressor bufferCompressor;

    @Nullable private HsOutputMetrics outputMetrics;

    HsSubpartitionMemoryDataManager(
            int targetChannel,
            int bufferSize,
            Lock resultPartitionLock,
            @Nullable BufferCompressor bufferCompressor,
            HsMemoryDataManagerOperation memoryDataManagerOperation) {
        this.targetChannel = targetChannel;
        this.bufferSize = bufferSize;
        this.resultPartitionLock = resultPartitionLock;
        this.memoryDataManagerOperation = memoryDataManagerOperation;
        this.bufferCompressor = bufferCompressor;
        this.consumerMap = new HashMap<>();
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryDataManager
    // ------------------------------------------------------------------------

    /**
     * Append record to {@link HsSubpartitionMemoryDataManager}.
     *
     * @param record to be managed by this class.
     * @param dataType the type of this record. In other words, is it data or event.
     */
    public void append(ByteBuffer record, DataType dataType) throws InterruptedException {
        if (dataType.isEvent()) {
            writeEvent(record, dataType);
        } else {
            writeRecord(record, dataType);
        }
    }

    /**
     * Get buffers in {@link #allBuffers} that satisfy expected {@link SpillStatus} and {@link
     * ConsumeStatus}.
     *
     * @param spillStatus the status of spilling expected.
     * @param consumeStatusWithId the status and consumerId expected.
     * @return buffers satisfy expected status in order.
     * 找到满足条件的buffer
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public Deque<BufferIndexAndChannel> getBuffersSatisfyStatus(
            SpillStatus spillStatus, ConsumeStatusWithId consumeStatusWithId) {
        return callWithLock(
                () -> {
                    // TODO return iterator to avoid completely traversing the queue for each call.
                    Deque<BufferIndexAndChannel> targetBuffers = new ArrayDeque<>();
                    // traverse buffers in order.
                    allBuffers.forEach(
                            (bufferContext -> {
                                if (isBufferSatisfyStatus(
                                        bufferContext, spillStatus, consumeStatusWithId)) {
                                    targetBuffers.add(bufferContext.getBufferIndexAndChannel());
                                }
                            }));
                    return targetBuffers;
                });
    }

    /**
     * Spill this subpartition's buffers in a decision.
     *
     * @param toSpill All buffers that need to be spilled belong to this subpartition in a decision.
     * @param spillDoneFuture completed when spill is finished.
     * @return {@link BufferWithIdentity}s about these spill buffers.
     * 开始倾倒buffer
     * 触发这组buffer的startSpill后 返回
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public List<BufferWithIdentity> spillSubpartitionBuffers(
            List<BufferIndexAndChannel> toSpill, CompletableFuture<Void> spillDoneFuture) {
        return callWithLock(
                () ->
                        toSpill.stream()
                                .map(
                                        indexAndChannel -> {
                                            int bufferIndex = indexAndChannel.getBufferIndex();
                                            // 开始倾倒数据
                                            return startSpillingBuffer(bufferIndex, spillDoneFuture)
                                                    .map(
                                                            (context) ->
                                                                    new BufferWithIdentity(
                                                                            context.getBuffer(),
                                                                            bufferIndex,
                                                                            targetChannel));
                                        })
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .collect(Collectors.toList()));
    }

    /**
     * Release this subpartition's buffers in a decision.
     *
     * @param toRelease All buffers that need to be released belong to this subpartition in a
     *     decision.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: runWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    public void releaseSubpartitionBuffers(List<BufferIndexAndChannel> toRelease) {
        runWithLock(
                () ->
                        toRelease.forEach(
                                (indexAndChannel) -> {
                                    int bufferIndex = indexAndChannel.getBufferIndex();
                                    HsBufferContext bufferContext =
                                            bufferIndexToContexts.get(bufferIndex);
                                    if (bufferContext != null) {
                                        // 标记成释放状态
                                        checkAndMarkBufferReadable(bufferContext);
                                        // 调用 release
                                        releaseBuffer(bufferIndex);
                                    }
                                }));
    }

    public void setOutputMetrics(HsOutputMetrics outputMetrics) {
        this.outputMetrics = checkNotNull(outputMetrics);
    }

    /**
     * 添加一个消费者
     * @param consumerId
     * @return
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    public HsSubpartitionConsumerMemoryDataManager registerNewConsumer(HsConsumerId consumerId) {
        return callWithLock(
                () -> {
                    checkState(!consumerMap.containsKey(consumerId));
                    // 封装一个消费者
                    HsSubpartitionConsumerMemoryDataManager newConsumer =
                            new HsSubpartitionConsumerMemoryDataManager(
                                    resultPartitionLock,
                                    subpartitionLock.readLock(),
                                    targetChannel,
                                    consumerId,
                                    memoryDataManagerOperation);
                    // 把这里的所有buffer设置进去 便于给消费者消费
                    newConsumer.addInitialBuffers(allBuffers);
                    consumerMap.put(consumerId, newConsumer);
                    return newConsumer;
                });
    }

    /**
     * 表示某个消费者不再消费数据了
     * @param consumerId
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    public void releaseConsumer(HsConsumerId consumerId) {
        runWithLock(() -> checkNotNull(consumerMap.remove(consumerId)));
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void writeEvent(ByteBuffer event, DataType dataType) {
        checkArgument(dataType.isEvent());

        // each Event must take an exclusive buffer
        // 事件一定要独占一个buffer  所以如果有部分数据 一定要先处理掉
        finishCurrentWritingBufferIfNotEmpty();

        // store Events in adhoc heap segments, for network memory efficiency
        MemorySegment data = MemorySegmentFactory.wrap(event.array());
        Buffer buffer =
                new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, dataType, data.size());

        HsBufferContext bufferContext =
                new HsBufferContext(buffer, finishedBufferIndex, targetChannel);
        // 事件产生时 要立即发送
        addFinishedBuffer(bufferContext);
        memoryDataManagerOperation.onBufferFinished();
    }

    private void writeRecord(ByteBuffer record, DataType dataType) throws InterruptedException {
        checkArgument(!dataType.isEvent());

        ensureCapacityForRecord(record);

        writeRecord(record);
    }

    /**
     * 先确保有足够空间
     * @param record
     * @throws InterruptedException
     */
    private void ensureCapacityForRecord(ByteBuffer record) throws InterruptedException {
        final int numRecordBytes = record.remaining();
        int availableBytes =
                Optional.ofNullable(unfinishedBuffers.peek())
                        .map(
                                currentWritingBuffer ->
                                        currentWritingBuffer.getWritableBytes()
                                                + bufferSize * (unfinishedBuffers.size() - 1))
                        .orElse(0);

        while (availableBytes < numRecordBytes) {
            // request unfinished buffer.
            BufferBuilder bufferBuilder = memoryDataManagerOperation.requestBufferFromPool();
            unfinishedBuffers.add(bufferBuilder);
            availableBytes += bufferSize;
        }
    }

    private void writeRecord(ByteBuffer record) {
        while (record.hasRemaining()) {
            BufferBuilder currentWritingBuffer =
                    checkNotNull(
                            unfinishedBuffers.peek(), "Expect enough capacity for the record.");
            currentWritingBuffer.append(record);

            // 当一个buffer写满时 触发finish
            if (currentWritingBuffer.isFull()) {
                finishCurrentWritingBuffer();
            }
        }
    }

    /**
     *
     */
    private void finishCurrentWritingBufferIfNotEmpty() {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.peek();
        // 表示buffer是空的  不需要处理
        if (currentWritingBuffer == null || currentWritingBuffer.getWritableBytes() == bufferSize) {
            return;
        }

        finishCurrentWritingBuffer();
    }

    /**
     * 处理已经写入的数据
     */
    private void finishCurrentWritingBuffer() {
        BufferBuilder currentWritingBuffer = unfinishedBuffers.poll();

        if (currentWritingBuffer == null) {
            return;
        }

        currentWritingBuffer.finish();
        BufferConsumer bufferConsumer = currentWritingBuffer.createBufferConsumerFromBeginning();
        Buffer buffer = bufferConsumer.build();
        currentWritingBuffer.close();
        bufferConsumer.close();
        // 将buffer包装成context
        HsBufferContext bufferContext =
                new HsBufferContext(
                        compressBuffersIfPossible(buffer), finishedBufferIndex, targetChannel);
        addFinishedBuffer(bufferContext);
        memoryDataManagerOperation.onBufferFinished();
    }

    private Buffer compressBuffersIfPossible(Buffer buffer) {
        if (!canBeCompressed(buffer)) {
            return buffer;
        }
        return checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
    }

    /**
     * Whether the buffer can be compressed or not. Note that event is not compressed because it is
     * usually small and the size can become even larger after compression.
     */
    private boolean canBeCompressed(Buffer buffer) {
        return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    // 添加一个已经准备好的buffer  (已经压缩过)
    private void addFinishedBuffer(HsBufferContext bufferContext) {
        finishedBufferIndex++;
        List<HsConsumerId> needNotify = new ArrayList<>(consumerMap.size());
        runWithLock(
                () -> {
                    // 加入容器中
                    allBuffers.add(bufferContext);
                    bufferIndexToContexts.put(
                            bufferContext.getBufferIndexAndChannel().getBufferIndex(),
                            bufferContext);
                    // 同步到每个consumer的容器中
                    for (Map.Entry<HsConsumerId, HsSubpartitionConsumerMemoryDataManager>
                            consumerEntry : consumerMap.entrySet()) {
                        if (consumerEntry.getValue().addBuffer(bufferContext)) {
                            needNotify.add(consumerEntry.getKey());
                        }
                    }
                    updateStatistics(bufferContext.getBuffer());
                });
        // 通知这些consumer 有数据可用了
        memoryDataManagerOperation.onDataAvailable(targetChannel, needNotify);
    }

    /**
     * Remove all released buffer from head of queue until buffer queue is empty or meet un-released
     * buffer.
     */
    @GuardedBy("subpartitionLock")
    private void trimHeadingReleasedBuffers(Deque<HsBufferContext> bufferQueue) {
        while (!bufferQueue.isEmpty() && bufferQueue.peekFirst().isReleased()) {
            bufferQueue.removeFirst();
        }
    }

    /**
     * 释放buffer
     * @param bufferIndex
     */
    @GuardedBy("subpartitionLock")
    private void releaseBuffer(int bufferIndex) {
        HsBufferContext bufferContext = bufferIndexToContexts.remove(bufferIndex);
        if (bufferContext == null) {
            return;
        }
        bufferContext.release();
        // remove released buffers from head lazy.
        trimHeadingReleasedBuffers(allBuffers);
    }

    /**
     * 开始倾倒数据
     * @param bufferIndex
     * @param spillFuture
     * @return
     */
    @GuardedBy("subpartitionLock")
    private Optional<HsBufferContext> startSpillingBuffer(
            int bufferIndex, CompletableFuture<Void> spillFuture) {
        HsBufferContext bufferContext = bufferIndexToContexts.get(bufferIndex);
        if (bufferContext == null) {
            return Optional.empty();
        }
        return bufferContext.startSpilling(spillFuture)
                ? Optional.of(bufferContext)
                : Optional.empty();
    }

    /**
     * 检查该buffer 是否可以标记成release
     * @param bufferContext
     */
    @GuardedBy("subpartitionLock")
    private void checkAndMarkBufferReadable(HsBufferContext bufferContext) {
        // only spill buffer needs to be marked as released.
        if (isBufferSatisfyStatus(bufferContext, SpillStatus.SPILL, ConsumeStatusWithId.ALL_ANY)) {
            bufferContext
                    .getSpilledFuture()
                    .orElseThrow(
                            () ->
                                    new IllegalStateException(
                                            "Buffer in spill status should already set spilled future."))
                    .thenRun(
                            () -> {
                                BufferIndexAndChannel bufferIndexAndChannel =
                                        bufferContext.getBufferIndexAndChannel();
                                // 将该buffer标记成释放状态
                                memoryDataManagerOperation.markBufferReleasedFromFile(
                                        bufferIndexAndChannel.getChannel(),
                                        bufferIndexAndChannel.getBufferIndex());
                            });
        }
    }

    /**
     * 判断该buffer是否满足条件
     * @param bufferContext
     * @param spillStatus
     * @param consumeStatusWithId  是否被本对象消费
     * @return
     */
    @GuardedBy("subpartitionLock")
    private boolean isBufferSatisfyStatus(
            HsBufferContext bufferContext,
            SpillStatus spillStatus,
            ConsumeStatusWithId consumeStatusWithId) {
        // released buffer is not needed. 本buffer已经被释放了  不应该再处理
        if (bufferContext.isReleased()) {
            return false;
        }
        boolean match = true;
        switch (spillStatus) {
            case NOT_SPILL:
                match = !bufferContext.isSpillStarted();
                break;
            case SPILL:
                match = bufferContext.isSpillStarted();
                break;
        }
        switch (consumeStatusWithId.status) {
            case NOT_CONSUMED:
                match &= !bufferContext.isConsumed(consumeStatusWithId.consumerId);
                break;
            case CONSUMED:
                match &= bufferContext.isConsumed(consumeStatusWithId.consumerId);
                break;
        }
        return match;
    }

    private void updateStatistics(Buffer buffer) {
        checkNotNull(outputMetrics).getNumBuffersOut().inc();
        checkNotNull(outputMetrics).getNumBytesOut().inc(buffer.readableBytes());
    }

    private <E extends Exception> void runWithLock(ThrowingRunnable<E> runnable) throws E {
        try {
            resultPartitionLock.lock();
            subpartitionLock.writeLock().lock();
            runnable.run();
        } finally {
            subpartitionLock.writeLock().unlock();
            resultPartitionLock.unlock();
        }
    }

    private <R, E extends Exception> R callWithLock(SupplierWithException<R, E> callable) throws E {
        try {
            resultPartitionLock.lock();
            subpartitionLock.writeLock().lock();
            return callable.get();
        } finally {
            subpartitionLock.writeLock().unlock();
            resultPartitionLock.unlock();
        }
    }
}
