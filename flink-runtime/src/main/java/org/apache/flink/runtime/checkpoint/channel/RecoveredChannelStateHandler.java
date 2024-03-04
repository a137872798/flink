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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrap;

/**
 * 表示一个可恢复的状态
 * @param <Info>
 * @param <Context>
 */
interface RecoveredChannelStateHandler<Info, Context> extends AutoCloseable {
    class BufferWithContext<Context> {
        final ChannelStateByteBuffer buffer;
        final Context context;

        BufferWithContext(ChannelStateByteBuffer buffer, Context context) {
            this.buffer = buffer;
            this.context = context;
        }

        public void close() {
            buffer.close();
        }
    }

    /**
     * 获取空的缓冲区
     * @param info
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    BufferWithContext<Context> getBuffer(Info info) throws IOException, InterruptedException;

    /**
     * Recover the data from buffer. This method is taking over the ownership of the
     * bufferWithContext and is fully responsible for cleaning it up both on the happy path and in
     * case of an error.
     * @param info 对应一个input/output信息 或者说key
     * @param oldSubtaskIndex 该数据流针对的子任务  因为检查点在写完时会按照子任务进行拆分
     * @param bufferWithContext 将数据加载到缓冲区中
     */
    void recover(Info info, int oldSubtaskIndex, BufferWithContext<Context> bufferWithContext)
            throws IOException;
}

/**
 * 对应一个input 或者说分区
 */
class InputChannelRecoveredStateHandler
        implements RecoveredChannelStateHandler<InputChannelInfo, Buffer> {

    /**
     * 可以从该对象读取数据
     */
    private final InputGate[] inputGates;

    /**
     * 存储了重映射需要的基础信息   (内部也有RescaleMappings)
     */
    private final InflightDataRescalingDescriptor channelMapping;

    /**
     * 每个子任务 都可以写入多个 InputChannel
     * 同时数据发送到下游时可能会变成多个channel  所以是一个list
     */
    private final Map<InputChannelInfo, List<RecoveredInputChannel>> rescaledChannels =
            new HashMap<>();

    /**
     * RescaleMappings 用于分区映射
     * key 代表gateIndex
     */
    private final Map<Integer, RescaleMappings> oldToNewMappings = new HashMap<>();

    InputChannelRecoveredStateHandler(
            InputGate[] inputGates, InflightDataRescalingDescriptor channelMapping) {
        this.inputGates = inputGates;
        this.channelMapping = channelMapping;
    }

    /**
     * 获取某个channel的缓冲区   (无数据)
     * @param channelInfo
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BufferWithContext<Buffer> getBuffer(InputChannelInfo channelInfo)
            throws IOException, InterruptedException {
        // request the buffer from any mapped channel as they all will receive the same buffer
        // 这里总是返回第一个channel  (查找时会使用重映射)
        RecoveredInputChannel channel = getMappedChannels(channelInfo).get(0);
        // 申请缓冲区  但是内部应该没数据
        Buffer buffer = channel.requestBufferBlocking();
        return new BufferWithContext<>(wrap(buffer), buffer);
    }

    /**
     * 这里要进行数据恢复
     * @param channelInfo
     * @param oldSubtaskIndex 该数据流针对的子任务  因为检查点在写完时会按照子任务进行拆分
     * @param bufferWithContext 将数据加载到缓冲区中
     * @throws IOException
     */
    @Override
    public void recover(
            InputChannelInfo channelInfo,
            int oldSubtaskIndex,
            BufferWithContext<Buffer> bufferWithContext)
            throws IOException {
        Buffer buffer = bufferWithContext.context;
        try {
            if (buffer.readableBytes() > 0) {
                // 获取所有相关的channel
                for (final RecoveredInputChannel channel : getMappedChannels(channelInfo)) {

                    // 这里将存有分区信息和数据的buffer 加入channel
                    channel.onRecoveredStateBuffer(
                            EventSerializer.toBuffer(
                                    new SubtaskConnectionDescriptor(
                                            oldSubtaskIndex, channelInfo.getInputChannelIdx()),
                                    false));
                    channel.onRecoveredStateBuffer(buffer.retainBuffer());
                }
            }
        } finally {
            buffer.recycleBuffer();
        }
    }

    @Override
    public void close() throws IOException {
        // note that we need to finish all RecoveredInputChannels, not just those with state
        for (final InputGate inputGate : inputGates) {
            // 表示所有gate都完成了数据恢复
            inputGate.finishReadRecoveredState();
        }
    }

    /**
     * 找到channel
     * @param gateIndex
     * @param subPartitionIndex
     * @return
     */
    private RecoveredInputChannel getChannel(int gateIndex, int subPartitionIndex) {
        final InputChannel inputChannel = inputGates[gateIndex].getChannel(subPartitionIndex);
        if (!(inputChannel instanceof RecoveredInputChannel)) {
            throw new IllegalStateException(
                    "Cannot restore state to a non-recovered input channel: " + inputChannel);
        }
        return (RecoveredInputChannel) inputChannel;
    }

    private List<RecoveredInputChannel> getMappedChannels(InputChannelInfo channelInfo) {
        return rescaledChannels.computeIfAbsent(channelInfo, this::calculateMapping);
    }

    /**
     * 当对应的分区数据不存在时  触发该方法
     * @param info
     * @return
     */
    private List<RecoveredInputChannel> calculateMapping(InputChannelInfo info) {
        final RescaleMappings oldToNewMapping =
                oldToNewMappings.computeIfAbsent(
                        info.getGateIdx(), idx -> channelMapping.getChannelMapping(idx).invert());
        final List<RecoveredInputChannel> channels =
                // 传入的是旧分区信息 在getMappedChannels没有数据时 进入该方法 那么此时就要将旧分区映射到新分区
                Arrays.stream(oldToNewMapping.getMappedIndexes(info.getInputChannelIdx()))
                        // 此时可能会映射到多个新分区
                        .mapToObj(newChannelIndex -> getChannel(info.getGateIdx(), newChannelIndex))
                        // 因为映射到多个分区  所以可能返回一个list
                        .collect(Collectors.toList());
        if (channels.isEmpty()) {
            throw new IllegalStateException(
                    "Recovered a buffer from old "
                            + info
                            + " that has no mapping in "
                            + channelMapping.getChannelMapping(info.getGateIdx()));
        }
        return channels;
    }
}

/**
 * 这个对应output
 */
class ResultSubpartitionRecoveredStateHandler
        implements RecoveredChannelStateHandler<ResultSubpartitionInfo, BufferBuilder> {

    private final ResultPartitionWriter[] writers;
    private final boolean notifyAndBlockOnCompletion;

    /**
     * 维护rescaling的相关信息
     */
    private final InflightDataRescalingDescriptor channelMapping;

    private final Map<ResultSubpartitionInfo, List<ResultSubpartitionInfo>> rescaledChannels =
            new HashMap<>();
    private final Map<Integer, RescaleMappings> oldToNewMappings = new HashMap<>();

    ResultSubpartitionRecoveredStateHandler(
            ResultPartitionWriter[] writers,
            boolean notifyAndBlockOnCompletion,
            InflightDataRescalingDescriptor channelMapping) {
        this.writers = writers;
        this.channelMapping = channelMapping;
        this.notifyAndBlockOnCompletion = notifyAndBlockOnCompletion;
    }

    /**
     * 也是申请buffer
     * @param subpartitionInfo
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BufferWithContext<BufferBuilder> getBuffer(ResultSubpartitionInfo subpartitionInfo)
            throws IOException, InterruptedException {
        // request the buffer from any mapped subpartition as they all will receive the same buffer
        BufferBuilder bufferBuilder =
                getCheckpointedResultPartition(subpartitionInfo.getPartitionIdx())
                        .requestBufferBuilderBlocking();
        return new BufferWithContext<>(wrap(bufferBuilder), bufferBuilder);
    }

    /**
     * 进行数据恢复
     * @param subpartitionInfo
     * @param oldSubtaskIndex 该数据流针对的子任务  因为检查点在写完时会按照子任务进行拆分
     * @param bufferWithContext 将数据加载到缓冲区中
     * @throws IOException
     */
    @Override
    public void recover(
            ResultSubpartitionInfo subpartitionInfo,
            int oldSubtaskIndex,
            BufferWithContext<BufferBuilder> bufferWithContext)
            throws IOException {
        try (BufferBuilder bufferBuilder = bufferWithContext.context;
                BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumerFromBeginning()) {
            bufferBuilder.finish();
            if (!bufferConsumer.isDataAvailable()) {
                return;
            }
            // 找到映射后的分区
            final List<ResultSubpartitionInfo> mappedSubpartitions =
                    getMappedSubpartitions(subpartitionInfo);
            CheckpointedResultPartition checkpointedResultPartition =
                    getCheckpointedResultPartition(subpartitionInfo.getPartitionIdx());
            for (final ResultSubpartitionInfo mappedSubpartition : mappedSubpartitions) {
                // channel selector is created from the downstream's point of view: the
                // subtask of downstream = subpartition index of recovered buffer
                // 这个连接顺序 好像跟input相反   第二个是old
                final SubtaskConnectionDescriptor channelSelector =
                        new SubtaskConnectionDescriptor(
                                subpartitionInfo.getSubPartitionIdx(), oldSubtaskIndex);

                // 生成事件 以及数据  写入分区
                checkpointedResultPartition.addRecovered(
                        mappedSubpartition.getSubPartitionIdx(),
                        EventSerializer.toBufferConsumer(channelSelector, false));
                checkpointedResultPartition.addRecovered(
                        mappedSubpartition.getSubPartitionIdx(), bufferConsumer.copy());
            }
        }
    }

    private ResultSubpartitionInfo getSubpartitionInfo(int partitionIndex, int subPartitionIdx) {
        CheckpointedResultPartition writer = getCheckpointedResultPartition(partitionIndex);
        return writer.getCheckpointedSubpartitionInfo(subPartitionIdx);
    }

    private CheckpointedResultPartition getCheckpointedResultPartition(int partitionIndex) {
        ResultPartitionWriter writer = writers[partitionIndex];
        if (!(writer instanceof CheckpointedResultPartition)) {
            throw new IllegalStateException(
                    "Cannot restore state to a non-checkpointable partition type: " + writer);
        }
        return (CheckpointedResultPartition) writer;
    }

    private List<ResultSubpartitionInfo> getMappedSubpartitions(
            ResultSubpartitionInfo subpartitionInfo) {
        return rescaledChannels.computeIfAbsent(subpartitionInfo, this::calculateMapping);
    }

    /**
     * 进行重映射
     * @param info
     * @return
     */
    private List<ResultSubpartitionInfo> calculateMapping(ResultSubpartitionInfo info) {
        final RescaleMappings oldToNewMapping =
                oldToNewMappings.computeIfAbsent(
                        info.getPartitionIdx(),
                        idx -> channelMapping.getChannelMapping(idx).invert());
        final List<ResultSubpartitionInfo> subpartitions =
                Arrays.stream(oldToNewMapping.getMappedIndexes(info.getSubPartitionIdx()))
                        .mapToObj(
                                newIndexes ->
                                        getSubpartitionInfo(info.getPartitionIdx(), newIndexes))
                        .collect(Collectors.toList());
        if (subpartitions.isEmpty()) {
            throw new IllegalStateException(
                    "Recovered a buffer from old "
                            + info
                            + " that has no mapping in "
                            + channelMapping.getChannelMapping(info.getPartitionIdx()));
        }
        return subpartitions;
    }

    @Override
    public void close() throws IOException {
        for (ResultPartitionWriter writer : writers) {
            if (writer instanceof CheckpointedResultPartition) {
                ((CheckpointedResultPartition) writer)
                        .finishReadRecoveredState(notifyAndBlockOnCompletion);
            }
        }
    }
}
