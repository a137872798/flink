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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The read view of HsResultPartition, data can be read from memory or disk.
 * ResultSubpartitionView 暴露获取数据的api
 * HsSubpartitionConsumerInternalOperations 则是暴露可以对consumer发起的操作  比如通知消费者有数据可用  或者获取当前消费偏移量
 * */
public class HsSubpartitionConsumer
        implements ResultSubpartitionView, HsSubpartitionConsumerInternalOperations {
    private final BufferAvailabilityListener availabilityListener;
    private final Object lock = new Object();

    /** Index of last consumed buffer. */
    @GuardedBy("lock")
    private int lastConsumedBufferIndex = -1;

    @GuardedBy("lock")
    private boolean needNotify = true;

    /**
     * 缓存下个数据的类型
     */
    @Nullable
    @GuardedBy("lock")
    private Buffer.DataType cachedNextDataType = null;

    @Nullable
    @GuardedBy("lock")
    private Throwable failureCause = null;

    @GuardedBy("lock")
    private boolean isReleased = false;

    /**
     * 对应的是磁盘数据视图
     */
    @Nullable
    @GuardedBy("lock")
    // diskDataView can be null only before initialization.
    private HsDataView diskDataView;

    /**
     * 对应内存数据视图
     */
    @Nullable
    @GuardedBy("lock")
    // memoryDataView can be null only before initialization.
    private HsDataView memoryDataView;

    public HsSubpartitionConsumer(BufferAvailabilityListener availabilityListener) {
        this.availabilityListener = availabilityListener;
    }


    /**
     * 获取下个数据
     * 简单来看就是从2个view中获取了下条buffer
     * @return
     */
    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() {
        Queue<Buffer> buffersToRecycle = new ArrayDeque<>();
        try {
            synchronized (lock) {
                checkNotNull(diskDataView, "disk data view must be not null.");
                checkNotNull(memoryDataView, "memory data view must be not null.");

                // 先从磁盘加载数据
                Optional<BufferAndBacklog> bufferToConsume = tryReadFromDisk(buffersToRecycle);
                if (!bufferToConsume.isPresent()) {
                    // 否则从内存加载数据
                    bufferToConsume =
                            memoryDataView.consumeBuffer(
                                    lastConsumedBufferIndex + 1, buffersToRecycle);
                }

                // 更新本地变量
                updateConsumingStatus(bufferToConsume);
                return bufferToConsume.map(this::handleBacklog).orElse(null);
            }
        } catch (Throwable cause) {
            // release subpartition reader outside of lock to avoid deadlock.
            releaseInternal(cause);
            return null;
        } finally {
            // recycle buffers outside of lock to avoid deadlock.
            while (!buffersToRecycle.isEmpty()) {
                buffersToRecycle.poll().recycleBuffer();
            }
        }
    }

    @Override
    public void notifyDataAvailable() {
        boolean notifyDownStream = false;
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            // 可以看到只有 needNotify 为true时 才能触发通知
            if (needNotify) {
                notifyDownStream = true;
                needNotify = false;
            }
        }
        // notify outside of lock to avoid deadlock
        if (notifyDownStream) {
            availabilityListener.notifyDataAvailable();
        }
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        synchronized (lock) {
            boolean availability = numCreditsAvailable > 0;
            if (numCreditsAvailable <= 0
                    && cachedNextDataType != null
                    && cachedNextDataType == Buffer.DataType.EVENT_BUFFER) {
                availability = true;
            }

            int backlog = getSubpartitionBacklog();
            // 此时没数据 那么当下次有数据时就需要通知了
            if (backlog == 0) {
                needNotify = true;
            }
            return new AvailabilityWithBacklog(availability, backlog);
        }
    }

    @Override
    public void releaseAllResources() throws IOException {
        releaseInternal(null);
    }

    @Override
    public boolean isReleased() {
        synchronized (lock) {
            return isReleased;
        }
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int getConsumingOffset(boolean withLock) {
        if (!withLock) {
            return lastConsumedBufferIndex;
        }
        synchronized (lock) {
            return lastConsumedBufferIndex;
        }
    }

    @Override
    public Throwable getFailureCause() {
        synchronized (lock) {
            return failureCause;
        }
    }

    /**
     * Set {@link HsDataView} for this subpartition, this method only called when {@link
     * HsSubpartitionFileReader} is creating.
     */
    void setDiskDataView(HsDataView diskDataView) {
        synchronized (lock) {
            checkState(this.diskDataView == null, "repeatedly set disk data view is not allowed.");
            this.diskDataView = diskDataView;
        }
    }

    /**
     * Set {@link HsDataView} for this subpartition, this method only called when {@link
     * HsSubpartitionFileReader} is creating.
     */
    void setMemoryDataView(HsDataView memoryDataView) {
        synchronized (lock) {
            checkState(
                    this.memoryDataView == null, "repeatedly set memory data view is not allowed.");
            this.memoryDataView = memoryDataView;
        }
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("resumeConsumption should never be called.");
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        // in case of bounded partitions there is no upstream to acknowledge, we simply ignore
        // the ack, as there are no checkpoints
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return getSubpartitionBacklog();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (lock) {
            return getSubpartitionBacklog();
        }
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    @SuppressWarnings("FieldAccessNotGuarded")
    private int getSubpartitionBacklog() {
        if (memoryDataView == null || diskDataView == null) {
            return 0;
        }
        return Math.max(memoryDataView.getBacklog(), diskDataView.getBacklog());
    }

    /**
     * 处理 backlog相关的
     * @param bufferToConsume
     * @return
     */
    private BufferAndBacklog handleBacklog(BufferAndBacklog bufferToConsume) {
        return bufferToConsume.buffersInBacklog() == 0
                ? new BufferAndBacklog(
                        bufferToConsume.buffer(),
                        getSubpartitionBacklog(),  // 获取囤积数量
                        bufferToConsume.getNextDataType(),
                        bufferToConsume.getSequenceNumber())
                : bufferToConsume;
    }

    /**
     * 尝试从磁盘读取数据
     * @param buffersToRecycle
     * @return
     * @throws Throwable
     */
    @GuardedBy("lock")
    private Optional<BufferAndBacklog> tryReadFromDisk(Queue<Buffer> buffersToRecycle)
            throws Throwable {
        final int nextBufferIndexToConsume = lastConsumedBufferIndex + 1;
        return checkNotNull(diskDataView)
                // 读取数据
                .consumeBuffer(nextBufferIndexToConsume, buffersToRecycle)
                .map(
                        bufferAndBacklog -> {
                            if (bufferAndBacklog.getNextDataType() == Buffer.DataType.NONE) {
                                return new BufferAndBacklog(
                                        bufferAndBacklog.buffer(),
                                        bufferAndBacklog.buffersInBacklog(),
                                        // 磁盘没有类型时  使用内存的
                                        checkNotNull(memoryDataView)
                                                .peekNextToConsumeDataType(
                                                        nextBufferIndexToConsume + 1,
                                                        buffersToRecycle),
                                        bufferAndBacklog.getSequenceNumber());
                            }
                            // 本来是直接返回的
                            return bufferAndBacklog;
                        });
    }

    /**
     * 使用本次加载到的buffer数据 更新消费者状态
     * @param bufferAndBacklog
     */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @GuardedBy("lock")
    private void updateConsumingStatus(Optional<BufferAndBacklog> bufferAndBacklog) {
        assert Thread.holdsLock(lock);
        // if consumed, update and check consume offset
        if (bufferAndBacklog.isPresent()) {
            ++lastConsumedBufferIndex;
            checkState(bufferAndBacklog.get().getSequenceNumber() == lastConsumedBufferIndex);
        }

        // update need-notify
        boolean dataAvailable =
                bufferAndBacklog.map(BufferAndBacklog::isDataAvailable).orElse(false);
        // 当类型是NONE时 表明没有数据了  那么当有数据时才需要通知
        needNotify = !dataAvailable;
        // update cached next data type
        cachedNextDataType = bufferAndBacklog.map(BufferAndBacklog::getNextDataType).orElse(null);
    }

    private void releaseInternal(@Nullable Throwable throwable) {
        boolean releaseDiskView;
        boolean releaseMemoryView;
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            failureCause = throwable;
            releaseDiskView = diskDataView != null;
            releaseMemoryView = memoryDataView != null;
        }
        // release subpartition reader outside of lock to avoid deadlock.
        if (releaseDiskView) {
            //noinspection FieldAccessNotGuarded
            diskDataView.releaseDataView();
        }
        if (releaseMemoryView) {
            //noinspection FieldAccessNotGuarded
            memoryDataView.releaseDataView();
        }
    }
}
