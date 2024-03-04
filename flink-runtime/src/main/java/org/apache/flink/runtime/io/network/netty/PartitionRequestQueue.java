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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A nonEmptyReader of partition queues, which listens for channel writability changed events before
 * writing and flushing {@link Buffer} instances.
 * 该对象作为netty服务器的最后一环
 */
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

    /**
     * 用于监听写入刷盘结果
     */
    private final ChannelFutureListener writeListener =
            new WriteAndFlushNextMessageIfPossibleListener();

    /** The readers which are already enqueued available for transferring data.
     * 当某个子分区被请求数据时  会加入到该队列中
     * */
    private final ArrayDeque<NetworkSequenceViewReader> availableReaders = new ArrayDeque<>();

    /** All the readers created for the consumers' partition requests.
     * 每当下游请求本节点某个分区数据时  会生成一个reader对象 并维护在该容器中
     * */
    private final ConcurrentMap<InputChannelID, NetworkSequenceViewReader> allReaders =
            new ConcurrentHashMap<>();

    private boolean fatalError;

    private ChannelHandlerContext ctx;

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        }

        super.channelRegistered(ctx);
    }

    /**
     * 将reader加入队列
     * @param reader
     */
    void notifyReaderNonEmpty(final NetworkSequenceViewReader reader) {
        // The notification might come from the same thread. For the initial writes this
        // might happen before the reader has set its reference to the view, because
        // creating the queue and the initial notification happen in the same method call.
        // This can be resolved by separating the creation of the view and allowing
        // notifications.

        // TODO This could potentially have a bad performance impact as in the
        // worst case (network consumes faster than the producer) each buffer
        // will trigger a separate event loop task being scheduled.
        ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(reader));
    }

    /**
     * Try to enqueue the reader once receiving credit notification from the consumer or receiving
     * non-empty reader notification from the producer.
     *
     * <p>NOTE: Only one thread would trigger the actual enqueue after checking the reader's
     * availability, so there is no race condition here.
     * 将reader加入队列
     */
    private void enqueueAvailableReader(final NetworkSequenceViewReader reader) throws Exception {

        // 避免重复注册
        if (reader.isRegisteredAsAvailable()) {
            return;
        }

        ResultSubpartitionView.AvailabilityWithBacklog availabilityWithBacklog =
                reader.getAvailabilityAndBacklog();

        // 表示当前不可用
        if (!availabilityWithBacklog.isAvailable()) {
            int backlog = availabilityWithBacklog.getBacklog();
            if (backlog > 0 && reader.needAnnounceBacklog()) {
                // 通知下游此时有多少数据
                announceBacklog(reader, backlog);
            }
            return;
        }

        // Queue an available reader for consumption. If the queue is empty,
        // we try trigger the actual write. Otherwise this will be handled by
        // the writeAndFlushNextMessageIfPossible calls.
        boolean triggerWrite = availableReaders.isEmpty();
        registerAvailableReader(reader);

        if (triggerWrite) {
            writeAndFlushNextMessageIfPossible(ctx.channel());
        }
    }

    /**
     * Accesses internal state to verify reader registration in the unit tests.
     *
     * <p><strong>Do not use anywhere else!</strong>
     *
     * @return readers which are enqueued available for transferring data
     */
    @VisibleForTesting
    ArrayDeque<NetworkSequenceViewReader> getAvailableReaders() {
        return availableReaders;
    }

    public void notifyReaderCreated(final NetworkSequenceViewReader reader) {
        allReaders.put(reader.getReceiverId(), reader);
    }

    public void cancel(InputChannelID receiverId) {
        ctx.pipeline().fireUserEventTriggered(receiverId);
    }

    public void close() throws IOException {
        if (ctx != null) {
            ctx.channel().close();
        }

        releaseAllResources();
    }

    /**
     * Adds unannounced credits from the consumer or resumes data consumption after an exactly-once
     * checkpoint and enqueues the corresponding reader for this consumer (if not enqueued yet).
     *
     * @param receiverId The input channel id to identify the consumer.
     * @param operation The operation to be performed (add credit or resume data consumption).
     */
    void addCreditOrResumeConsumption(
            InputChannelID receiverId, Consumer<NetworkSequenceViewReader> operation)
            throws Exception {
        if (fatalError) {
            return;
        }

        NetworkSequenceViewReader reader = obtainReader(receiverId);

        operation.accept(reader);
        enqueueAvailableReader(reader);
    }

    void acknowledgeAllRecordsProcessed(InputChannelID receiverId) {
        if (fatalError) {
            return;
        }

        obtainReader(receiverId).acknowledgeAllRecordsProcessed();
    }

    void notifyNewBufferSize(InputChannelID receiverId, int newBufferSize) {
        if (fatalError) {
            return;
        }

        // It is possible to receive new buffer size before the reader would be created since the
        // downstream task could calculate buffer size even using the data from one channel but it
        // sends new buffer size into all upstream even if they don't ready yet. In this case, just
        // ignore the new buffer size.
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader != null) {
            reader.notifyNewBufferSize(newBufferSize);
        }
    }

    /**
     * Notify the id of required segment from the consumer.
     *
     * @param receiverId The input channel id to identify the consumer.
     * @param segmentId The id of required segment.
     */
    void notifyRequiredSegmentId(InputChannelID receiverId, int segmentId) {
        if (fatalError) {
            return;
        }
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader != null) {
            reader.notifyRequiredSegmentId(segmentId);
        }
    }

    NetworkSequenceViewReader obtainReader(InputChannelID receiverId) {
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader == null) {
            throw new IllegalStateException(
                    "No reader for receiverId = " + receiverId + " exists.");
        }

        return reader;
    }

    /**
     * Announces remaining backlog to the consumer after the available data notification or data
     * consumption resumption.
     * 通知下游有多少数据
     */
    private void announceBacklog(NetworkSequenceViewReader reader, int backlog) {
        checkArgument(backlog > 0, "Backlog must be positive.");

        NettyMessage.BacklogAnnouncement announcement =
                new NettyMessage.BacklogAnnouncement(backlog, reader.getReceiverId());
        ctx.channel()
                .writeAndFlush(announcement)
                .addListener(
                        (ChannelFutureListener)
                                future -> {
                                    if (!future.isSuccess()) {
                                        onChannelFutureFailure(future);
                                    }
                                });
    }

    /**
     * 处理自定义事件
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
        // The user event triggered event loop callback is used for thread-safe
        // hand over of reader queues and cancelled producers.

        if (msg instanceof NetworkSequenceViewReader) {
            // 表示某个reader可用
            enqueueAvailableReader((NetworkSequenceViewReader) msg);

            // 表示丢弃该reader
        } else if (msg.getClass() == InputChannelID.class) {
            // Release partition view that get a cancel request.
            InputChannelID toCancel = (InputChannelID) msg;

            // remove reader from queue of available readers
            availableReaders.removeIf(reader -> reader.getReceiverId().equals(toCancel));

            // remove reader from queue of all readers and release its resource
            final NetworkSequenceViewReader toRelease = allReaders.remove(toCancel);
            if (toRelease != null) {
                releaseViewReader(toRelease);
            }
        } else {
            ctx.fireUserEventTriggered(msg);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        writeAndFlushNextMessageIfPossible(ctx.channel());
    }

    /**
     * 当出现了第一个可用的reader时  触发该方法
     * @param channel
     * @throws IOException
     */
    private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
        if (fatalError || !channel.isWritable()) {
            return;
        }

        // The logic here is very similar to the combined input gate and local
        // input channel logic. You can think of this class acting as the input
        // gate and the consumed views as the local input channels.

        BufferAndAvailability next = null;
        try {
            while (true) {
                NetworkSequenceViewReader reader = pollAvailableReader();

                // No queue with available data. We allow this here, because
                // of the write callbacks that are executed after each write.
                if (reader == null) {
                    return;
                }

                next = reader.getNextBuffer();
                if (next == null) {
                    if (!reader.isReleased()) {
                        continue;
                    }

                    Throwable cause = reader.getFailureCause();
                    if (cause != null) {
                        ErrorResponse msg = new ErrorResponse(cause, reader.getReceiverId());

                        // 通知错误信息
                        ctx.writeAndFlush(msg);
                    }
                } else {
                    // This channel was now removed from the available reader queue.
                    // We re-add it into the queue if it is still available
                    if (next.moreAvailable()) {
                        registerAvailableReader(reader);
                    }

                    BufferResponse msg =
                            new BufferResponse(
                                    next.buffer(),
                                    next.getSequenceNumber(),
                                    reader.getReceiverId(),
                                    next.buffersInBacklog());

                    // Write and flush and wait until this is done before
                    // trying to continue with the next buffer.
                    channel.writeAndFlush(msg).addListener(writeListener);

                    return;
                }
            }
        } catch (Throwable t) {
            if (next != null) {
                next.buffer().recycleBuffer();
            }

            throw new IOException(t.getMessage(), t);
        }
    }

    private void registerAvailableReader(NetworkSequenceViewReader reader) {
        availableReaders.add(reader);
        reader.setRegisteredAsAvailable(true);
    }

    @Nullable
    private NetworkSequenceViewReader pollAvailableReader() {
        // 出队列 之后会重新进队列
        NetworkSequenceViewReader reader = availableReaders.poll();
        if (reader != null) {
            // 每次reader只要拉取一个消息 都会自动变成不可用  只要检测到还有未处理的数据  还会变成可用状态
            reader.setRegisteredAsAvailable(false);
        }
        return reader;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releaseAllResources();

        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        handleException(ctx.channel(), cause);
    }

    /**
     * 当发送给对端失败时
     * @param channel
     * @param cause
     * @throws IOException
     */
    private void handleException(Channel channel, Throwable cause) throws IOException {
        LOG.error("Encountered error while consuming partitions", cause);

        fatalError = true;
        releaseAllResources();

        // 写入一个错误信息
        if (channel.isActive()) {
            channel.writeAndFlush(new ErrorResponse(cause))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    /**
     * 释放所有相关资源
     * @throws IOException
     */
    private void releaseAllResources() throws IOException {
        // note: this is only ever executed by one thread: the Netty IO thread!
        for (NetworkSequenceViewReader reader : allReaders.values()) {
            releaseViewReader(reader);
        }

        availableReaders.clear();
        allReaders.clear();
    }

    private void releaseViewReader(NetworkSequenceViewReader reader) throws IOException {
        reader.setRegisteredAsAvailable(false);
        reader.releaseAllResources();
    }

    private void onChannelFutureFailure(ChannelFuture future) throws Exception {
        if (future.cause() != null) {
            handleException(future.channel(), future.cause());
        } else {
            handleException(
                    future.channel(), new IllegalStateException("Sending cancelled by user."));
        }
    }

    // This listener is called after an element of the current nonEmptyReader has been
    // flushed. If successful, the listener triggers further processing of the
    // queues.
    // 监听写入/刷盘操作的结果
    private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            try {
                if (future.isSuccess()) {
                    writeAndFlushNextMessageIfPossible(future.channel());
                } else {
                    onChannelFutureFailure(future);
                }
            } catch (Throwable t) {
                handleException(future.channel(), t);
            }
        }
    }
}
