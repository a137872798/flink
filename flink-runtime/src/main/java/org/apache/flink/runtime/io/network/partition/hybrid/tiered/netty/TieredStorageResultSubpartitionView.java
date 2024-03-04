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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartition;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link TieredStorageResultSubpartitionView} is the implementation of {@link
 * ResultSubpartitionView} of {@link TieredResultPartition}.
 * 视图是用来读取数据的
 */
public class TieredStorageResultSubpartitionView implements ResultSubpartitionView {

    private final BufferAvailabilityListener availabilityListener;

    private final List<NettyPayloadManager> nettyPayloadManagers;

    private final List<NettyServiceProducer> serviceProducers;

    private final List<NettyConnectionId> nettyConnectionIds;

    private volatile boolean isReleased = false;

    /**
     * 当前正在查看的seg
     */
    private int requiredSegmentId = 0;

    private boolean stopSendingData = false;

    /**
     * 对应manager的下标
     */
    private int managerIndexContainsCurrentSegment = -1;

    private int currentSequenceNumber = -1;

    /**
     * 当生产者为空时  会产生一个全为空列表的对象
     * @param availabilityListener
     * @param nettyPayloadManagers  每个manager对应一个连接 对应一个生产者
     * @param nettyConnectionIds
     * @param serviceProducers
     */
    public TieredStorageResultSubpartitionView(
            BufferAvailabilityListener availabilityListener,
            List<NettyPayloadManager> nettyPayloadManagers,
            List<NettyConnectionId> nettyConnectionIds,
            List<NettyServiceProducer> serviceProducers) {
        this.availabilityListener = availabilityListener;
        this.nettyPayloadManagers = nettyPayloadManagers;
        this.nettyConnectionIds = nettyConnectionIds;
        this.serviceProducers = serviceProducers;
    }

    /**
     * 获取下个buffer数据
     * @return
     * @throws IOException
     */
    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        // 表示当前seg已经标识了停止发送数据 或者表示队列中没有其他数据了 返回null
        if (stopSendingData || !findCurrentNettyPayloadQueue()) {
            return null;
        }

        // 找到当前正在访问的manager
        NettyPayloadManager nettyPayloadManager =
                nettyPayloadManagers.get(managerIndexContainsCurrentSegment);

        // 从manager上获取buffer
        Optional<Buffer> nextBuffer = readNettyPayload(nettyPayloadManager);
        if (nextBuffer.isPresent()) {
            // 表示当前segment的数据已经读完了
            stopSendingData = nextBuffer.get().getDataType() == END_OF_SEGMENT;
            if (stopSendingData) {
                // 重置游标
                managerIndexContainsCurrentSegment = -1;
            }
            currentSequenceNumber++;
            return BufferAndBacklog.fromBufferAndLookahead(
                    nextBuffer.get(),
                    getDataType(nettyPayloadManager.peek()),
                    getBacklog(),
                    currentSequenceNumber);
        }
        return null;
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        if (findCurrentNettyPayloadQueue()) {
            NettyPayloadManager currentQueue =
                    nettyPayloadManagers.get(managerIndexContainsCurrentSegment);
            boolean availability = numCreditsAvailable > 0;
            // TODO 这个availability是什么意思 ?
            if (numCreditsAvailable == 0 && isEventOrError(currentQueue)) {
                availability = true;
            }
            return new AvailabilityWithBacklog(availability, getBacklog());
        }
        return new AvailabilityWithBacklog(false, 0);
    }

    /**
     * 更新当前要读取的seg  这样就可以继续调用 nextBuffer了
     * @param segmentId segment id is the id indicating the required id.
     */
    @Override
    public void notifyRequiredSegmentId(int segmentId) {
        if (segmentId > requiredSegmentId) {
            requiredSegmentId = segmentId;
            stopSendingData = false;
            availabilityListener.notifyDataAvailable();
        }
    }

    /**
     * 释放所有资源
     * @throws IOException
     */
    @Override
    public void releaseAllResources() throws IOException {
        if (isReleased) {
            return;
        }
        isReleased = true;
        for (int index = 0; index < nettyPayloadManagers.size(); ++index) {
            releaseQueue(
                    nettyPayloadManagers.get(index),
                    serviceProducers.get(index),
                    nettyConnectionIds.get(index));
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public Throwable getFailureCause() {
        // nothing to do
        return null;
    }

    /**
     * 返回当前囤积的报文
     * @return
     */
    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        if (findCurrentNettyPayloadQueue()) {
            return getBacklog();
        }
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        if (findCurrentNettyPayloadQueue()) {
            return getBacklog();
        }
        return 0;
    }

    @Override
    public void notifyDataAvailable() {
        throw new UnsupportedOperationException(
                "Method notifyDataAvailable should never be called.");
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("Method resumeConsumption should never be called.");
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        // nothing to do.
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        throw new UnsupportedOperationException(
                "Method notifyNewBufferSize should never be called.");
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    /**
     * 读取buffer数据
     * @param nettyPayloadManager
     * @return
     * @throws IOException
     */
    private Optional<Buffer> readNettyPayload(NettyPayloadManager nettyPayloadManager)
            throws IOException {
        NettyPayload nettyPayload = nettyPayloadManager.poll();
        if (nettyPayload == null) {
            return Optional.empty();
        } else {
            checkState(nettyPayload.getSegmentId() == -1);
            Optional<Throwable> error = nettyPayload.getError();
            if (error.isPresent()) {
                releaseAllResources();
                throw new IOException(error.get());
            } else {
                return nettyPayload.getBuffer();
            }
        }
    }

    private int getBacklog() {
        return managerIndexContainsCurrentSegment == -1
                ? 0
                : nettyPayloadManagers.get(managerIndexContainsCurrentSegment).getBacklog();
    }

    /**
     * 下个数据是error 或者 event
     * @param nettyPayloadManager
     * @return
     */
    private boolean isEventOrError(NettyPayloadManager nettyPayloadManager) {
        NettyPayload nettyPayload = nettyPayloadManager.peek();
        return nettyPayload != null
                && (nettyPayload.getError().isPresent()
                        || (nettyPayload.getBuffer().isPresent()
                                && !nettyPayload.getBuffer().get().isBuffer()));
    }

    private Buffer.DataType getDataType(NettyPayload nettyPayload) {
        if (nettyPayload == null || !nettyPayload.getBuffer().isPresent()) {
            return Buffer.DataType.NONE;
        } else {
            return nettyPayload.getBuffer().get().getDataType();
        }
    }


    /**
     *
     * @param nettyPayloadManager
     * @param serviceProducer
     * @param id
     */
    private void releaseQueue(
            NettyPayloadManager nettyPayloadManager,
            NettyServiceProducer serviceProducer,
            NettyConnectionId id) {
        NettyPayload nettyPayload;
        // 回收payload 并释放内存
        while ((nettyPayload = nettyPayloadManager.poll()) != null) {
            nettyPayload.getBuffer().ifPresent(Buffer::recycleBuffer);
        }
        // 断开连接
        serviceProducer.connectionBroken(id);
    }

    /**
     * 查看当前队列中是否有囤积数据
     * @return
     */
    private boolean findCurrentNettyPayloadQueue() {
        // 在读到表示end的事件前 认为还有数据
        if (managerIndexContainsCurrentSegment != -1 && !stopSendingData) {
            return true;
        }

        for (int managerIndex = 0; managerIndex < nettyPayloadManagers.size(); managerIndex++) {
            NettyPayload firstNettyPayload = nettyPayloadManagers.get(managerIndex).peek();
            if (firstNettyPayload == null
                    || firstNettyPayload.getSegmentId() != requiredSegmentId) {
                continue;
            }
            managerIndexContainsCurrentSegment = managerIndex;
            NettyPayload segmentId =
                    nettyPayloadManagers.get(managerIndexContainsCurrentSegment).poll();
            checkState(segmentId.getSegmentId() != -1);
            return true;
        }
        return false;
    }
}
