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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An input channel consumes a single {@link ResultSubpartitionView}.
 *
 * <p>For each channel, the consumption life cycle is as follows:
 *
 * <ol>
 *   <li>{@link #requestSubpartition()}
 *   <li>{@link #getNextBuffer()}
 *   <li>{@link #releaseAllResources()}
 * </ol>
 *
 * 一个channel对应一个子分区
 */
public abstract class InputChannel {
    /** The info of the input channel to identify it globally within a task.
     * 描述该channel的下标
     * */
    protected final InputChannelInfo channelInfo;

    /** The parent partition of the subpartition consumed by this channel.
     * 此时相关的一个结果集  对应一个分区 一个分区下有很多子分区
     * */
    protected final ResultPartitionID partitionId;

    /** The index of the subpartition consumed by this channel.
     * 对应的子分区
     * */
    protected final int consumedSubpartitionIndex;

    /**
     * 该channel所属的gate
     */
    protected final SingleInputGate inputGate;

    // - Asynchronous error notification --------------------------------------

    private final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();

    // - Partition request backoff --------------------------------------------

    /** The initial backoff (in ms).
     * 初始的补偿时间
     * */
    protected final int initialBackoff;

    /** The maximum backoff (in ms). */
    protected final int maxBackoff;

    protected final Counter numBytesIn;

    protected final Counter numBuffersIn;

    /** The current backoff (in ms).
     * 当前补偿时间
     * */
    private int currentBackoff;

    /**
     *
     * @param inputGate  channel所属的gate
     * @param channelIndex
     * @param partitionId
     * @param consumedSubpartitionIndex
     * @param initialBackoff
     * @param maxBackoff
     * @param numBytesIn
     * @param numBuffersIn
     */
    protected InputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            int consumedSubpartitionIndex,
            int initialBackoff,
            int maxBackoff,
            Counter numBytesIn,
            Counter numBuffersIn) {

        checkArgument(channelIndex >= 0);

        int initial = initialBackoff;
        int max = maxBackoff;

        checkArgument(initial >= 0 && initial <= max);

        this.inputGate = checkNotNull(inputGate);

        // gateIndex + channelIndex 组成channelInfo
        this.channelInfo = new InputChannelInfo(inputGate.getGateIndex(), channelIndex);
        this.partitionId = checkNotNull(partitionId);

        checkArgument(consumedSubpartitionIndex >= 0);
        this.consumedSubpartitionIndex = consumedSubpartitionIndex;

        this.initialBackoff = initial;
        this.maxBackoff = max;
        this.currentBackoff = initial == 0 ? -1 : 0;

        this.numBytesIn = numBytesIn;
        this.numBuffersIn = numBuffersIn;
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    /** Returns the index of this channel within its {@link SingleInputGate}. */
    public int getChannelIndex() {
        return channelInfo.getInputChannelIdx();
    }

    /**
     * Returns the info of this channel, which uniquely identifies the channel in respect to its
     * operator instance.
     */
    public InputChannelInfo getChannelInfo() {
        return channelInfo;
    }

    public ResultPartitionID getPartitionId() {
        return partitionId;
    }

    public int getConsumedSubpartitionIndex() {
        return consumedSubpartitionIndex;
    }

    /**
     * After sending a {@link org.apache.flink.runtime.io.network.api.CheckpointBarrier} of
     * exactly-once mode, the upstream will be blocked and become unavailable. This method tries to
     * unblock the corresponding upstream and resume data consumption.
     */
    public abstract void resumeConsumption() throws IOException;

    /**
     * When received {@link EndOfData} from one channel, it need to acknowledge after this event get
     * processed.
     */
    public abstract void acknowledgeAllRecordsProcessed() throws IOException;

    /**
     * Notifies the owning {@link SingleInputGate} that this channel became non-empty.
     *
     * <p>This is guaranteed to be called only when a Buffer was added to a previously empty input
     * channel. The notion of empty is atomically consistent with the flag {@link
     * BufferAndAvailability#moreAvailable()} when polling the next buffer from this channel.
     *
     * <p><b>Note:</b> When the input channel observes an exception, this method is called
     * regardless of whether the channel was empty before. That ensures that the parent InputGate
     * will always be notified about the exception.
     */
    protected void notifyChannelNonEmpty() {
        inputGate.notifyChannelNonEmpty(this);
    }

    /**
     * 通知收到了一个优先级事件
     * @param priorityBufferNumber
     */
    public void notifyPriorityEvent(int priorityBufferNumber) {
        inputGate.notifyPriorityEvent(this, priorityBufferNumber);
    }

    /**
     * 通知此时有多少buffer可用
     * @param numAvailableBuffers
     * @throws IOException
     */
    protected void notifyBufferAvailable(int numAvailableBuffers) throws IOException {}

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    /**
     * Requests the subpartition specified by {@link #partitionId} and {@link
     * #consumedSubpartitionIndex}.
     * 请求指定子分区
     */
    abstract void requestSubpartition() throws IOException, InterruptedException;

    /**
     * Returns the next buffer from the consumed subpartition or {@code Optional.empty()} if there
     * is no data to return.
     * 获取下个包含数据的buffer
     */
    public abstract Optional<BufferAndAvailability> getNextBuffer()
            throws IOException, InterruptedException;

    /**
     * Called by task thread when checkpointing is started (e.g., any input channel received
     * barrier).
     * 表示开始产生检查点了    本channel会收到一个屏障  在此时应该会暂停数据的消费
     */
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {}

    /** Called by task thread on cancel/complete to clean-up temporary data.
     * 表示检查点完成或者取消
     * */
    public void checkpointStopped(long checkpointId) {}

    /**
     * 通过编号查找buffer 里面存储了优先级事件
     * @param sequenceNumber
     * @throws IOException
     */
    public void convertToPriorityEvent(int sequenceNumber) throws IOException {}

    // ------------------------------------------------------------------------
    // Task events
    // ------------------------------------------------------------------------

    /**
     * Sends a {@link TaskEvent} back to the task producing the consumed result partition.
     *
     * <p><strong>Important</strong>: The producing task has to be running to receive backwards
     * events. This means that the result type needs to be pipelined and the task logic has to
     * ensure that the producer will wait for all backwards events. Otherwise, this will lead to an
     * Exception at runtime.
     * 往channel发送一个事件  (既可以从channel读取数据 也可以往内部发送事件)
     */
    abstract void sendTaskEvent(TaskEvent event) throws IOException;

    // ------------------------------------------------------------------------
    // Life cycle
    // ------------------------------------------------------------------------

    /**
     * 表示该channel是否已经被释放了
     * @return
     */
    abstract boolean isReleased();

    /** Releases all resources of the channel.
     * 释放channel的所有资源
     * */
    abstract void releaseAllResources() throws IOException;

    /**
     * 更新buffer的大小
     * @param newBufferSize
     */
    abstract void announceBufferSize(int newBufferSize);

    abstract int getBuffersInUseCount();

    // ------------------------------------------------------------------------
    // Error notification
    // ------------------------------------------------------------------------

    /**
     * Checks for an error and rethrows it if one was reported.
     *
     * <p>Note: Any {@link PartitionException} instances should not be transformed and make sure
     * they are always visible in task failure cause.
     */
    protected void checkError() throws IOException {
        final Throwable t = cause.get();

        if (t != null) {
            if (t instanceof CancelTaskException) {
                throw (CancelTaskException) t;
            }
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException(t);
            }
        }
    }

    /**
     * Atomically sets an error for this channel and notifies the input gate about available data to
     * trigger querying this channel by the task thread.
     */
    protected void setError(Throwable cause) {
        if (this.cause.compareAndSet(null, checkNotNull(cause))) {
            // Notify the input gate.
            notifyChannelNonEmpty();
        }
    }

    // ------------------------------------------------------------------------
    // Partition request exponential backoff
    // ------------------------------------------------------------------------

    /** Returns the current backoff in ms. */
    protected int getCurrentBackoff() {
        return currentBackoff <= 0 ? 0 : currentBackoff;
    }

    /**
     * Increases the current backoff and returns whether the operation was successful.
     *
     * @return <code>true</code>, iff the operation was successful. Otherwise, <code>false</code>.
     * 增加惩罚时间
     */
    protected boolean increaseBackoff() {
        // Backoff is disabled
        if (currentBackoff < 0) {
            return false;
        }

        // This is the first time backing off
        if (currentBackoff == 0) {
            currentBackoff = initialBackoff;

            return true;
        }

        // Continue backing off
        else if (currentBackoff < maxBackoff) {
            currentBackoff = Math.min(currentBackoff * 2, maxBackoff);

            return true;
        }

        // Reached maximum backoff
        return false;
    }

    // ------------------------------------------------------------------------
    // Metric related method
    // ------------------------------------------------------------------------

    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return 0;
    }

    public long unsynchronizedGetSizeOfQueuedBuffers() {
        return 0;
    }

    /**
     * Notify the upstream the id of required segment that should be sent to netty connection.
     *
     * @param segmentId segment id indicates the id of segment.
     *                  通知当前访问的段发生了变化
     */
    public void notifyRequiredSegmentId(int segmentId) throws IOException {}

    // ------------------------------------------------------------------------

    /**
     * A combination of a {@link Buffer} and a flag indicating availability of further buffers, and
     * the backlog length indicating how many non-event buffers are available in the subpartition.
     *
     * 从channel中获得的对象
     */
    public static final class BufferAndAvailability {

        /**
         * 存储数据的容器
         */
        private final Buffer buffer;
        /**
         * 表示下个数据类型
         */
        private final Buffer.DataType nextDataType;
        /**
         * buffer中还有多少记录
         */
        private final int buffersInBacklog;

        /**
         * 每个buffer有序列号
         */
        private final int sequenceNumber;

        public BufferAndAvailability(
                Buffer buffer,
                Buffer.DataType nextDataType,
                int buffersInBacklog,
                int sequenceNumber) {
            this.buffer = checkNotNull(buffer);
            this.nextDataType = checkNotNull(nextDataType);
            this.buffersInBacklog = buffersInBacklog;
            this.sequenceNumber = sequenceNumber;
        }

        public Buffer buffer() {
            return buffer;
        }

        public boolean moreAvailable() {
            return nextDataType != Buffer.DataType.NONE;
        }

        /**
         * 下条记录类型包含优先级属性 就代表是优先级事件
         * @return
         */
        public boolean morePriorityEvents() {
            return nextDataType.hasPriority();
        }

        public int buffersInBacklog() {
            return buffersInBacklog;
        }

        /**
         * 是否是优先数据
         * @return
         */
        public boolean hasPriority() {
            return buffer.getDataType().hasPriority();
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public String toString() {
            return "BufferAndAvailability{"
                    + "buffer="
                    + buffer
                    + ", nextDataType="
                    + nextDataType
                    + ", buffersInBacklog="
                    + buffersInBacklog
                    + ", sequenceNumber="
                    + sequenceNumber
                    + '}';
        }
    }

    void setup() throws IOException {}
}
