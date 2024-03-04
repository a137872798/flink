/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import java.io.IOException;
import java.util.List;

/**
 * Input, with just basic methods for blocking and resuming consumption. It can be for example an
 * {@link InputGate} or a chained source.
 * 表示支持产生检查点
 */
@Internal
public interface CheckpointableInput {

    /**
     * 可能是因为要生成检查点的关系  暂停某个channel的数据消费
     * @param channelInfo
     */
    void blockConsumption(InputChannelInfo channelInfo);

    /**
     * 恢复消费
     * @param channelInfo
     * @throws IOException
     */
    void resumeConsumption(InputChannelInfo channelInfo) throws IOException;

    /**
     * 获取相关的所有channel信息
     * @return
     */
    List<InputChannelInfo> getChannelInfos();

    int getNumberOfInputChannels();

    void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException;

    void checkpointStopped(long cancelledCheckpointId);

    int getInputGateIndex();

    /**
     * 将某个事件转换成优先事件
     * @param channelIndex
     * @param sequenceNumber
     * @throws IOException
     */
    void convertToPriorityEvent(int channelIndex, int sequenceNumber) throws IOException;
}
