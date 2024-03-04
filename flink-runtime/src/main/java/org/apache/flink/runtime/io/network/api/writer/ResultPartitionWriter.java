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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * A record-oriented runtime result writer API for producing results.
 *
 * <p>If {@link ResultPartitionWriter#close()} is called before {@link
 * ResultPartitionWriter#fail(Throwable)} or {@link ResultPartitionWriter#finish()}, it abruptly
 * triggers failure and cancellation of production. In this case {@link
 * ResultPartitionWriter#fail(Throwable)} still needs to be called afterwards to fully release all
 * resources associated the partition and propagate failure cause to the consumer if possible.
 *
 * 以分区为单位写入数据
 * RecordWriter内主要就是依靠该对象
 */
public interface ResultPartitionWriter extends AutoCloseable, AvailabilityProvider {

    /** Setup partition, potentially heavy-weight, blocking operation comparing to just creation. */
    void setup() throws IOException;

    /**
     * 获取分区id
     * @return
     */
    ResultPartitionID getPartitionId();

    /**
     * channel数量
     * @return
     */
    int getNumberOfSubpartitions();

    int getNumTargetKeyGroups();

    /** Sets the max overdraft buffer size of per gate.
     * 每个gate最多透支的buffer大小
     * */
    void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate);

    /** Writes the given serialized record to the target subpartition.
     * 写入一条记录到指定的子分区
     * */
    void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException;

    /**
     * Writes the given serialized record to all subpartitions. One can also achieve the same effect
     * by emitting the same record to all subpartitions one by one, however, this method can have
     * better performance for the underlying implementation can do some optimizations, for example
     * coping the given serialized record only once to a shared channel which can be consumed by all
     * subpartitions.
     */
    void broadcastRecord(ByteBuffer record) throws IOException;

    /** Writes the given {@link AbstractEvent} to all channels.
     * 广播一个事件到所有channel
     * */
    void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException;

    /** Timeout the aligned barrier to unaligned barrier.
     * 表示某个对齐的屏障超时 变成了非对齐状态
     * */
    void alignedBarrierTimeout(long checkpointId) throws IOException;

    /** Abort the checkpoint.
     * 终止生成检查点操作
     * */
    void abortCheckpoint(long checkpointId, CheckpointException cause);

    /**
     * Notifies the downstream tasks that this {@code ResultPartitionWriter} have emitted all the
     * user records.
     *
     * @param mode tells if we should flush all records or not (it is false in case of
     *     stop-with-savepoint (--no-drain))
     */
    void notifyEndOfData(StopMode mode) throws IOException;

    /**
     * Gets the future indicating whether all the records has been processed by the downstream
     * tasks.
     */
    CompletableFuture<Void> getAllDataProcessedFuture();

    /** Sets the metric group for the {@link ResultPartitionWriter}. */
    void setMetricGroup(TaskIOMetricGroup metrics);

    /** Returns a reader for the subpartition with the given index. */
    ResultSubpartitionView createSubpartitionView(
            int index, BufferAvailabilityListener availabilityListener) throws IOException;

    /** Manually trigger the consumption of data from all subpartitions.
     * 为所有channel刷盘
     * */
    void flushAll();

    /** Manually trigger the consumption of data from the given subpartitions.
     * 为某channel数据刷盘
     * */
    void flush(int subpartitionIndex);

    /**
     * Fail the production of the partition.
     *
     * <p>This method propagates non-{@code null} failure causes to consumers on a best-effort
     * basis. This call also leads to the release of all resources associated with the partition.
     * Closing of the partition is still needed afterwards if it has not been done before.
     *
     * @param throwable failure cause
     */
    void fail(@Nullable Throwable throwable);

    /**
     * Successfully finish the production of the partition.
     *
     * <p>Closing of partition is still needed afterwards.
     */
    void finish() throws IOException;

    boolean isFinished();

    /**
     * Releases the partition writer which releases the produced data and no reader can consume the
     * partition any more.
     */
    void release(Throwable cause);

    boolean isReleased();

    /**
     * Closes the partition writer which releases the allocated resource, for example the buffer
     * pool.
     */
    void close() throws Exception;
}
