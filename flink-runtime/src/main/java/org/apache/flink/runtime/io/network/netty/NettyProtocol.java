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

import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

/** Defines the server and client channel handlers, i.e. the protocol, used by netty. */
public class NettyProtocol {

    /**
     * 这个是消息编码器 用于处理写出的数据
     */
    private final NettyMessage.NettyMessageEncoder messageEncoder =
            new NettyMessage.NettyMessageEncoder();

    /**
     * 该对象可以打开本地子分区  并查看内部数据
     */
    private final ResultPartitionProvider partitionProvider;

    /**
     * 往某个分区发送事件
     */
    private final TaskEventPublisher taskEventPublisher;

    NettyProtocol(
            ResultPartitionProvider partitionProvider, TaskEventPublisher taskEventPublisher) {
        this.partitionProvider = partitionProvider;
        this.taskEventPublisher = taskEventPublisher;
    }


    // 下面2个api分别获取作为客户端和服务器需要的 netty channel handler

    /**
     * Returns the server channel handlers.
     *
     * <pre>
     * +-------------------------------------------------------------------+
     * |                        SERVER CHANNEL PIPELINE                    |
     * |                                                                   |
     * |    +----------+----------+ (3) write  +----------------------+    |
     * |    | Queue of queues     +----------->| Message encoder      |    |
     * |    +----------+----------+            +-----------+----------+    |
     * |              /|\                                 \|/              |
     * |               | (2) enqueue                       |               |
     * |    +----------+----------+                        |               |
     * |    | Request handler     |                        |               |
     * |    +----------+----------+                        |               |
     * |              /|\                                  |               |
     * |               |                                   |               |
     * |   +-----------+-----------+                       |               |
     * |   | Message+Frame decoder |                       |               |
     * |   +-----------+-----------+                       |               |
     * |              /|\                                  |               |
     * +---------------+-----------------------------------+---------------+
     * |               | (1) client request               \|/
     * +---------------+-----------------------------------+---------------+
     * |               |                                   |               |
     * |       [ Socket.read() ]                    [ Socket.write() ]     |
     * |                                                                   |
     * |  Netty Internal I/O Threads (Transport Implementation)            |
     * +-------------------------------------------------------------------+
     * </pre>
     *
     * @return channel handlers
     * 获得服务器端的handler
     */
    public ChannelHandler[] getServerChannelHandlers() {
        PartitionRequestQueue queueOfPartitionQueues = new PartitionRequestQueue();
        PartitionRequestServerHandler serverHandler =
                new PartitionRequestServerHandler(
                        partitionProvider, taskEventPublisher, queueOfPartitionQueues);

        return new ChannelHandler[] {
            messageEncoder,
            new NettyMessage.NettyMessageDecoder(),
            serverHandler,  // 处理请求的核心对象  主要会转发给queueOfPartitionQueues
            queueOfPartitionQueues
        };
    }

    /**
     * Returns the client channel handlers.
     *
     * <pre>
     *     +-----------+----------+            +----------------------+
     *     | Remote input channel |            | request client       |
     *     +-----------+----------+            +-----------+----------+
     *                 |                                   | (1) write
     * +---------------+-----------------------------------+---------------+
     * |               |     CLIENT CHANNEL PIPELINE       |               |
     * |               |                                  \|/              |
     * |    +----------+----------+            +----------------------+    |
     * |    | Request handler     +            | Message encoder      |    |
     * |    +----------+----------+            +-----------+----------+    |
     * |              /|\                                 \|/              |
     * |               |                                   |               |
     * |    +----------+------------+                      |               |
     * |    | Message+Frame decoder |                      |               |
     * |    +----------+------------+                      |               |
     * |              /|\                                  |               |
     * +---------------+-----------------------------------+---------------+
     * |               | (3) server response              \|/ (2) client request
     * +---------------+-----------------------------------+---------------+
     * |               |                                   |               |
     * |       [ Socket.read() ]                    [ Socket.write() ]     |
     * |                                                                   |
     * |  Netty Internal I/O Threads (Transport Implementation)            |
     * +-------------------------------------------------------------------+
     * </pre>
     *
     * @return channel handlers
     */
    public ChannelHandler[] getClientChannelHandlers() {
        // 注意每次都是产生新的对象  该对象接收数据解析后 传给RemoteChannel
        NetworkClientHandler networkClientHandler = new CreditBasedPartitionRequestClientHandler();

        return new ChannelHandler[] {
            messageEncoder,  // 这个是共用的
            new NettyMessageClientDecoderDelegate(networkClientHandler),
            networkClientHandler
        };
    }
}
