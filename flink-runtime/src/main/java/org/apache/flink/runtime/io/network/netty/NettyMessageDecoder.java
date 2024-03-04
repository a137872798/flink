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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import javax.annotation.Nullable;

/** Base class of decoders for specified netty messages.
 * netty的消息解码器
 * */
abstract class NettyMessageDecoder implements AutoCloseable {

    /** ID of the message under decoding.
     * 当前消息的id
     * */
    protected int msgId;

    /** Length of the message under decoding.
     * 消息体长度
     * */
    protected int messageLength;

    /** The result of decoding one {@link ByteBuf}. */
    static class DecodingResult {
        static final DecodingResult NOT_FINISHED = new DecodingResult(false, null);

        /**
         * 本次是否解析到完整数据
         */
        private final boolean finished;

        /**
         * 解码后得到消息
         */
        @Nullable private final NettyMessage message;

        private DecodingResult(boolean finished, @Nullable NettyMessage message) {
            this.finished = finished;
            this.message = message;
        }

        public boolean isFinished() {
            return finished;
        }

        @Nullable
        public NettyMessage getMessage() {
            return message;
        }

        static DecodingResult fullMessage(NettyMessage message) {
            return new DecodingResult(true, message);
        }
    }

    /**
     * Notifies that the underlying channel becomes active.
     *
     * @param ctx The context for the callback.
     *            当某个连接生效时触发
     */
    abstract void onChannelActive(ChannelHandlerContext ctx);

    /**
     * Notifies that a new message is to be decoded.
     *
     * @param msgId The type of the message to be decoded.
     * @param messageLength The length of the message to be decoded.
     *                      解析头部成功时触发   早于onChannelRead
     */
    void onNewMessageReceived(int msgId, int messageLength) {
        this.msgId = msgId;
        this.messageLength = messageLength;
    }

    /**
     * Notifies that more data is received to continue decoding.
     *
     * @param data The data received.
     * @return The result of decoding received data.
     * 读取到数据时
     */
    abstract DecodingResult onChannelRead(ByteBuf data) throws Exception;
}
