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
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.netty.exception.TransportException;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Channel handler to read the messages of buffer response or error response from the producer, to
 * write and flush the unannounced credits for the producer.
 *
 * <p>It is used in the new network credit-based mode.
 * 处理接收到的消息  每个连接对应一个该对象
 */
class CreditBasedPartitionRequestClientHandler extends ChannelInboundHandlerAdapter
        implements NetworkClientHandler {

    private static final Logger LOG =
            LoggerFactory.getLogger(CreditBasedPartitionRequestClientHandler.class);

    /** Channels, which already requested partitions from the producers.
     * 通过该容器管理channel   一个目标节点可能有多个子分区的数据  连接就可以复用
     * */
    private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels =
            new ConcurrentHashMap<>();

    /** Messages to be sent to the producers (credit announcement or resume consumption request). */
    private final ArrayDeque<ClientOutboundMessage> clientOutboundMessages = new ArrayDeque<>();

    private final AtomicReference<Throwable> channelError = new AtomicReference<>();

    private final ChannelFutureListener writeListener =
            new WriteAndFlushNextMessageIfPossibleListener();

    /**
     * The channel handler context is initialized in channel active event by netty thread, the
     * context may also be accessed by task thread or canceler thread to cancel partition request
     * during releasing resources.
     */
    private volatile ChannelHandlerContext ctx;

    /**
     * 指向目标节点
     */
    private ConnectionID connectionID;

    // ------------------------------------------------------------------------
    // Input channel/receiver registration
    // ------------------------------------------------------------------------

    /**
     * 添加channel
     * @param listener
     * @throws IOException
     */
    @Override
    public void addInputChannel(RemoteInputChannel listener) throws IOException {
        checkError();

        inputChannels.putIfAbsent(listener.getInputChannelId(), listener);
    }

    @Override
    public void removeInputChannel(RemoteInputChannel listener) {
        inputChannels.remove(listener.getInputChannelId());
    }

    @Override
    public RemoteInputChannel getInputChannel(InputChannelID inputChannelId) {
        return inputChannels.get(inputChannelId);
    }

    /**
     * 通知上游某个channel不存在
     * @param inputChannelId
     */
    @Override
    public void cancelRequestFor(InputChannelID inputChannelId) {
        if (inputChannelId == null || ctx == null) {
            return;
        }

        ctx.writeAndFlush(new NettyMessage.CancelPartitionRequest(inputChannelId));
    }

    // ------------------------------------------------------------------------
    // Network events
    // ------------------------------------------------------------------------

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        }

        super.channelActive(ctx);
    }

    /**
     * 当连接关闭时  关闭所有channel
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        final SocketAddress remoteAddr = ctx.channel().remoteAddress();

        // 关闭下面所有channel  这个channel代表子分区
        notifyAllChannelsOfErrorAndClose(
                new RemoteTransportException(
                        "Connection unexpectedly closed by remote task manager '"
                                + remoteAddr
                                + " [ "
                                + connectionID.getResourceID().getStringWithMetadata()
                                + " ] "
                                + "'. "
                                + "This might indicate that the remote task manager was lost.",
                        remoteAddr));

        super.channelInactive(ctx);
    }

    /**
     * Called on exceptions in the client handler pipeline.
     *
     * <p>Remote exceptions are received as regular payload.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof TransportException) {

            // 捕获到异常时 关闭所有channel
            notifyAllChannelsOfErrorAndClose(cause);
        } else {
            final SocketAddress remoteAddr = ctx.channel().remoteAddress();

            final TransportException tex;

            // Improve on the connection reset by peer error message
            if (cause.getMessage() != null
                    && cause.getMessage().contains("Connection reset by peer")) {
                tex =
                        new RemoteTransportException(
                                "Lost connection to task manager '"
                                        + remoteAddr
                                        + " [ "
                                        + connectionID.getResourceID().getStringWithMetadata()
                                        + " ] "
                                        + "'. "
                                        + "This indicates that the remote task manager was lost.",
                                remoteAddr,
                                cause);
            } else {
                final SocketAddress localAddr = ctx.channel().localAddress();
                tex =
                        new LocalTransportException(
                                String.format(
                                        "%s (connection to '%s [%s]')",
                                        cause.getMessage(),
                                        remoteAddr,
                                        connectionID.getResourceID().getStringWithMetadata()),
                                localAddr,
                                cause);
            }

            notifyAllChannelsOfErrorAndClose(tex);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            // 接收到数据时  进行解码
            decodeMsg(msg);
        } catch (Throwable t) {
            notifyAllChannelsOfErrorAndClose(t);
        }
    }

    /**
     * Triggered by notifying credit available in the client handler pipeline.
     *
     * <p>Enqueues the input channel and will trigger write&flush unannounced credits for this input
     * channel if it is the first one in the queue.
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
        // 表示用户触发了一条需要发出的消息
        if (msg instanceof ClientOutboundMessage) {
            boolean triggerWrite = clientOutboundMessages.isEmpty();

            clientOutboundMessages.add((ClientOutboundMessage) msg);

            if (triggerWrite) {
                // 尝试发送消息
                writeAndFlushNextMessageIfPossible(ctx.channel());
            }
        } else if (msg instanceof ConnectionErrorMessage) {
            notifyAllChannelsOfErrorAndClose(((ConnectionErrorMessage) msg).getCause());
        } else {
            ctx.fireUserEventTriggered(msg);
        }
    }

    @Override
    public boolean hasChannelError() {
        return channelError.get() != null;
    }

    @Override
    public void setConnectionId(ConnectionID connectionId) {
        this.connectionID = checkNotNull(connectionId);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        writeAndFlushNextMessageIfPossible(ctx.channel());
    }

    /**
     * 检测到了异常  通知所有inputChannel
     * @param cause
     */
    @VisibleForTesting
    void notifyAllChannelsOfErrorAndClose(Throwable cause) {
        if (channelError.compareAndSet(null, cause)) {
            try {
                for (RemoteInputChannel inputChannel : inputChannels.values()) {
                    inputChannel.onError(cause);
                }
            } catch (Throwable t) {
                // We can only swallow the Exception at this point. :(
                LOG.warn(
                        "An Exception was thrown during error notification of a remote input channel.",
                        t);
            } finally {
                inputChannels.clear();
                clientOutboundMessages.clear();

                if (ctx != null) {
                    ctx.close();
                }
            }
        }
    }

    // ------------------------------------------------------------------------

    /** Checks for an error and rethrows it if one was reported. */
    @VisibleForTesting
    void checkError() throws IOException {
        final Throwable t = channelError.get();

        if (t != null) {
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException("There has been an error in the channel.", t);
            }
        }
    }

    /**
     * 解码收到的消息
     * @param msg
     */
    private void decodeMsg(Object msg) {
        final Class<?> msgClazz = msg.getClass();

        // ---- Buffer --------------------------------------------------------
        // 正常情况就是收到其他节点产生的数据
        if (msgClazz == NettyMessage.BufferResponse.class) {
            NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) msg;

            RemoteInputChannel inputChannel = inputChannels.get(bufferOrEvent.receiverId);
            if (inputChannel == null || inputChannel.isReleased()) {
                bufferOrEvent.releaseBuffer();

                // 通知上游本channel已经不存在
                cancelRequestFor(bufferOrEvent.receiverId);

                return;
            }

            try {
                // 让channel接收数据
                decodeBufferOrEvent(inputChannel, bufferOrEvent);
            } catch (Throwable t) {
                inputChannel.onError(t);
            }

            // 收到错误信息
        } else if (msgClazz == NettyMessage.ErrorResponse.class) {
            // ---- Error ---------------------------------------------------------
            NettyMessage.ErrorResponse error = (NettyMessage.ErrorResponse) msg;

            SocketAddress remoteAddr = ctx.channel().remoteAddress();

            // 关闭所有channel
            if (error.isFatalError()) {
                notifyAllChannelsOfErrorAndClose(
                        new RemoteTransportException(
                                "Fatal error at remote task manager '"
                                        + remoteAddr
                                        + " [ "
                                        + connectionID.getResourceID().getStringWithMetadata()
                                        + " ] "
                                        + "'.",
                                remoteAddr,
                                error.cause));
            } else {
                // 表示仅某个channel出现错误
                RemoteInputChannel inputChannel = inputChannels.get(error.receiverId);

                if (inputChannel != null) {
                    if (error.cause.getClass() == PartitionNotFoundException.class) {
                        // 通知分区错误  并进行检查
                        inputChannel.onFailedPartitionRequest();
                    } else {
                        inputChannel.onError(
                                new RemoteTransportException(
                                        "Error at remote task manager '"
                                                + remoteAddr
                                                + " [ "
                                                + connectionID
                                                        .getResourceID()
                                                        .getStringWithMetadata()
                                                + " ] "
                                                + "'.",
                                        remoteAddr,
                                        error.cause));
                    }
                }
            }
            // 收到提示信息  表示还有一定量的数据待消费
        } else if (msgClazz == NettyMessage.BacklogAnnouncement.class) {
            NettyMessage.BacklogAnnouncement announcement = (NettyMessage.BacklogAnnouncement) msg;

            // 也是找到对应的channel
            RemoteInputChannel inputChannel = inputChannels.get(announcement.receiverId);
            if (inputChannel == null || inputChannel.isReleased()) {
                // 此时channel已经不存在了   将关闭信息反馈给上游
                cancelRequestFor(announcement.receiverId);
                return;
            }

            try {
                inputChannel.onSenderBacklog(announcement.backlog);
            } catch (Throwable throwable) {
                inputChannel.onError(throwable);
            }
        } else {
            throw new IllegalStateException(
                    "Received unknown message from producer: " + msg.getClass());
        }
    }

    /**
     * 让目标channel接收数据
     * @param inputChannel
     * @param bufferOrEvent
     * @throws Throwable
     */
    private void decodeBufferOrEvent(
            RemoteInputChannel inputChannel, NettyMessage.BufferResponse bufferOrEvent)
            throws Throwable {
        if (bufferOrEvent.isBuffer() && bufferOrEvent.bufferSize == 0) {
            inputChannel.onEmptyBuffer(bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
        } else if (bufferOrEvent.getBuffer() != null) {
            inputChannel.onBuffer(
                    bufferOrEvent.getBuffer(), bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
        } else {
            throw new IllegalStateException(
                    "The read buffer is null in credit-based input channel.");
        }
    }

    /**
     * Tries to write&flush unannounced credits for the next input channel in queue.
     *
     * <p>This method may be called by the first input channel enqueuing, or the complete future's
     * callback in previous input channel, or the channel writability changed event.
     * 尝试发送消息
     */
    private void writeAndFlushNextMessageIfPossible(Channel channel) {
        if (channelError.get() != null || !channel.isWritable()) {
            return;
        }

        while (true) {
            ClientOutboundMessage outboundMessage = clientOutboundMessages.poll();

            // The input channel may be null because of the write callbacks
            // that are executed after each write.
            if (outboundMessage == null) {
                return;
            }

            // It is no need to notify credit or resume data consumption for the released channel.
            // 表示channel还有效
            if (!outboundMessage.inputChannel.isReleased()) {
                Object msg = outboundMessage.buildMessage();
                if (msg == null) {
                    continue;
                }

                // Write and flush and wait until this is done before
                // trying to continue with the next input channel.
                channel.writeAndFlush(msg).addListener(writeListener);

                return;
            }
        }
    }

    /**
     * 监听发送结果
     */
    private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            try {
                if (future.isSuccess()) {
                    // 尝试继续发送
                    writeAndFlushNextMessageIfPossible(future.channel());
                } else if (future.cause() != null) {
                    // 关闭本连接
                    notifyAllChannelsOfErrorAndClose(future.cause());
                } else {
                    notifyAllChannelsOfErrorAndClose(
                            new IllegalStateException("Sending cancelled by user."));
                }
            } catch (Throwable t) {
                notifyAllChannelsOfErrorAndClose(t);
            }
        }
    }
}
