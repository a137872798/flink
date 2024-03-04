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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/** {@link SequentialChannelStateReader} implementation.
 * 表示按顺序读取输入输出数据
 * */
public class SequentialChannelStateReaderImpl implements SequentialChannelStateReader {

    /**
     * 简单理解就是一个快照 内部维护各子任务的状态
     * 本对象就是基于快照进行数据恢复
     */
    private final TaskStateSnapshot taskStateSnapshot;

    /**
     * 该对象用于数据序列化
     */
    private final ChannelStateSerializer serializer;

    /**
     * 表示按块读取序列化的数据 用于state recover
     */
    private final ChannelStateChunkReader chunkReader;

    public SequentialChannelStateReaderImpl(TaskStateSnapshot taskStateSnapshot) {
        this.taskStateSnapshot = taskStateSnapshot;
        serializer = new ChannelStateSerializerImpl();
        chunkReader = new ChannelStateChunkReader(serializer);
    }

    @Override
    public void readInputData(InputGate[] inputGates) throws IOException, InterruptedException {
        try (InputChannelRecoveredStateHandler stateHandler =
                new InputChannelRecoveredStateHandler(
                        inputGates, taskStateSnapshot.getInputRescalingDescriptor())) {
            read(
                    stateHandler,
                    groupByDelegate(
                            streamSubtaskStates(), OperatorSubtaskState::getInputChannelState));
        }
    }

    @Override
    public void readOutputData(ResultPartitionWriter[] writers, boolean notifyAndBlockOnCompletion)
            throws IOException, InterruptedException {
        try (ResultSubpartitionRecoveredStateHandler stateHandler =
                new ResultSubpartitionRecoveredStateHandler(
                        writers,
                        notifyAndBlockOnCompletion,
                        taskStateSnapshot.getOutputRescalingDescriptor())) {
            read(
                    stateHandler,
                    groupByDelegate(
                            streamSubtaskStates(),
                            OperatorSubtaskState::getResultSubpartitionState));
        }
    }

    /**
     * 串行读取
     * @param stateHandler
     * @param streamStateHandleListMap
     * @param <Info>
     * @param <Context>
     * @param <Handle>
     * @throws IOException
     * @throws InterruptedException
     */
    private <Info, Context, Handle extends AbstractChannelStateHandle<Info>> void read(
            RecoveredChannelStateHandler<Info, Context> stateHandler,
            Map<StreamStateHandle, List<Handle>> streamStateHandleListMap)
            throws IOException, InterruptedException {
        for (Map.Entry<StreamStateHandle, List<Handle>> delegateAndHandles :
                streamStateHandleListMap.entrySet()) {
            readSequentially(
                    delegateAndHandles.getKey(), delegateAndHandles.getValue(), stateHandler);
        }
    }

    /**
     * 串行读取
     * @param streamStateHandle
     * @param channelStateHandles
     * @param stateHandler
     * @param <Info>
     * @param <Context>
     * @param <Handle>
     * @throws IOException
     * @throws InterruptedException
     */
    private <Info, Context, Handle extends AbstractChannelStateHandle<Info>> void readSequentially(
            StreamStateHandle streamStateHandle,
            List<Handle> channelStateHandles,  // 表示这组state使用同一个数据源进行初始化 (streamStateHandle)
            RecoveredChannelStateHandler<Info, Context> stateHandler)
            throws IOException, InterruptedException {
        try (FSDataInputStream is = streamStateHandle.openInputStream()) {
            // 这个是校验头部
            serializer.readHeader(is);
            for (RescaledOffset<Info> offsetAndChannelInfo :
                    extractOffsetsSorted(channelStateHandles)) {
                chunkReader.readChunk(
                        is,
                        offsetAndChannelInfo.offset,
                        stateHandler,  // 将数据读取到用于recover的 stateHandler
                        offsetAndChannelInfo.channelInfo,
                        offsetAndChannelInfo.oldSubtaskIndex);
            }
        }
    }

    /**
     * 获取快照内所有子任务状态
     * @return
     */
    private Stream<OperatorSubtaskState> streamSubtaskStates() {
        return taskStateSnapshot.getSubtaskStateMappings().stream().map(Map.Entry::getValue);
    }

    /**
     * 将所有子任务状态  基于函数分组
     * @param states
     * @param stateHandleExtractor
     * @param <Info>
     * @param <Handle>
     * @return
     */
    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            Map<StreamStateHandle, List<Handle>> groupByDelegate(
                    Stream<OperatorSubtaskState> states,
                    Function<OperatorSubtaskState, StateObjectCollection<Handle>>
                            stateHandleExtractor) {
        return states.map(stateHandleExtractor)  // 提取某些状态
                .flatMap(Collection::stream)
                .peek(validate())
                .collect(groupingBy(AbstractChannelStateHandle::getDelegate));
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            Consumer<Handle> validate() {
        Set<Tuple2<Info, Integer>> seen = new HashSet<>();
        // expect each channel/subtask to be described only once; otherwise, buffers in channel
        // could be
        // re-ordered
        return handle -> {
            if (!seen.add(new Tuple2<>(handle.getInfo(), handle.getSubtaskIndex()))) {
                throw new IllegalStateException("Duplicate channel info: " + handle);
            }
        };
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            List<RescaledOffset<Info>> extractOffsetsSorted(List<Handle> channelStateHandles) {
        return channelStateHandles.stream()
                .flatMap(SequentialChannelStateReaderImpl::extractOffsets)
                .sorted(comparingLong(offsetAndInfo -> offsetAndInfo.offset))
                .collect(toList());
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            Stream<RescaledOffset<Info>> extractOffsets(Handle handle) {
        return handle.getOffsets().stream()
                .map(
                        offset ->
                                new RescaledOffset<>(
                                        offset, handle.getInfo(), handle.getSubtaskIndex()));
    }

    @Override
    public void close() throws Exception {}

    /**
     * 抽取出关键信息
     * @param <Info>
     */
    static class RescaledOffset<Info> {
        final Long offset;
        final Info channelInfo;
        final int oldSubtaskIndex;

        RescaledOffset(Long offset, Info channelInfo, int oldSubtaskIndex) {
            this.offset = offset;
            this.channelInfo = channelInfo;
            this.oldSubtaskIndex = oldSubtaskIndex;
        }
    }
}

/**
 * 以chunk为单位读取数据
 */
class ChannelStateChunkReader {
    private final ChannelStateSerializer serializer;

    ChannelStateChunkReader(ChannelStateSerializer serializer) {
        this.serializer = serializer;
    }

    <Info, Context> void readChunk(
            FSDataInputStream source,
            long sourceOffset,
            RecoveredChannelStateHandler<Info, Context> stateHandler,
            Info channelInfo,
            int oldSubtaskIndex)
            throws IOException, InterruptedException {
        if (source.getPos() != sourceOffset) {
            source.seek(sourceOffset);
        }
        int length = serializer.readLength(source);
        while (length > 0) {

            // 此时buffer是空的
            RecoveredChannelStateHandler.BufferWithContext<Context> bufferWithContext =
                    stateHandler.getBuffer(channelInfo);
            try (Closeable ignored =
                    NetworkActionsLogger.measureIO(
                            "ChannelStateChunkReader#readChunk", bufferWithContext.buffer)) {
                while (length > 0 && bufferWithContext.buffer.isWritable()) {
                    // 将source的数据读取到buffer中
                    length -= serializer.readData(source, bufferWithContext.buffer, length);
                }
            } catch (Exception e) {
                bufferWithContext.close();
                throw e;
            }

            // Passing the ownership of buffer to inside.
            // 这会为handle设置恢复用的数据
            stateHandler.recover(channelInfo, oldSubtaskIndex, bufferWithContext);
        }
    }
}
