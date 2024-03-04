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

import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.AbstractChannelStateHandle.StateContentMetaInfo;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** The pending result of channel state for a specific checkpoint-subtask.
 * 对应一个subtask的结果
 * */
public class ChannelStatePendingResult {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelStatePendingResult.class);

    // Subtask information  子任务下标
    private final int subtaskIndex;

    /**
     * 本次对应的检查点
     */
    private final long checkpointId;

    // Result related
    /**
     * 使用该对象序列化state
     */
    private final ChannelStateSerializer serializer;

    /**
     * 表示写入的结果
     */
    private final ChannelStateWriter.ChannelStateWriteResult result;

    // 针对input/output 记录写入数据的offset/size
    private final Map<InputChannelInfo, AbstractChannelStateHandle.StateContentMetaInfo>
            inputChannelOffsets = new HashMap<>();
    private final Map<ResultSubpartitionInfo, AbstractChannelStateHandle.StateContentMetaInfo>
            resultSubpartitionOffsets = new HashMap<>();

    // 标识所有输入/输出 都已经写完
    private boolean allInputsReceived = false;
    private boolean allOutputsReceived = false;

    public ChannelStatePendingResult(
            int subtaskIndex,
            long checkpointId,
            ChannelStateWriter.ChannelStateWriteResult result,
            ChannelStateSerializer serializer) {
        this.subtaskIndex = subtaskIndex;
        this.checkpointId = checkpointId;
        this.result = result;
        this.serializer = serializer;
    }

    public boolean isAllInputsReceived() {
        return allInputsReceived;
    }

    public boolean isAllOutputsReceived() {
        return allOutputsReceived;
    }

    public Map<InputChannelInfo, StateContentMetaInfo> getInputChannelOffsets() {
        return inputChannelOffsets;
    }

    public Map<ResultSubpartitionInfo, StateContentMetaInfo> getResultSubpartitionOffsets() {
        return resultSubpartitionOffsets;
    }

    void completeInput() {
        LOG.debug("complete input, output completed: {}", allOutputsReceived);
        checkArgument(!allInputsReceived);
        allInputsReceived = true;
    }

    void completeOutput() {
        LOG.debug("complete output, input completed: {}", allInputsReceived);
        checkArgument(!allOutputsReceived);
        allOutputsReceived = true;
    }

    /**
     * 当检查点完成时触发
     * @param stateHandle  此前通过ChannelStateCheckpointWriter 写入数据时  所有子分区是混在一起的 现在需要通过offset信息 将子分区数据分离开
     * @throws IOException
     */
    public void finishResult(@Nullable StreamStateHandle stateHandle) throws IOException {
        checkState(
                stateHandle != null
                        || (inputChannelOffsets.isEmpty() && resultSubpartitionOffsets.isEmpty()),
                "The stateHandle just can be null when no data is written.");
        complete(
                stateHandle,
                result.inputChannelStateHandles,
                inputChannelOffsets,
                HandleFactory.INPUT_CHANNEL);
        complete(
                stateHandle,
                result.resultSubpartitionStateHandles,
                resultSubpartitionOffsets,
                HandleFactory.RESULT_SUBPARTITION);
    }

    /**
     *
     * @param underlying  包含所有检查点数据
     * @param future
     * @param offsets
     * @param handleFactory
     * @param <I>
     * @param <H>
     * @throws IOException
     */
    private <I, H extends AbstractChannelStateHandle<I>> void complete(
            StreamStateHandle underlying,
            CompletableFuture<Collection<H>> future,
            Map<I, StateContentMetaInfo> offsets,
            HandleFactory<I, H> handleFactory)
            throws IOException {
        final Collection<H> handles = new ArrayList<>();
        for (Map.Entry<I, StateContentMetaInfo> e : offsets.entrySet()) {
            handles.add(createHandle(handleFactory, underlying, e.getKey(), e.getValue()));
        }
        future.complete(handles);
        LOG.debug(
                "channel state write completed, checkpointId: {}, handles: {}",
                checkpointId,
                handles);
    }

    /**
     * 将维护某个input/output信息的对象 包装成handle
     * @param handleFactory
     * @param underlying  检查点句柄
     * @param channelInfo  input/output
     * @param contentMetaInfo  记录数据的offset/size
     * @param <I>
     * @param <H>
     * @return
     * @throws IOException
     */
    private <I, H extends AbstractChannelStateHandle<I>> H createHandle(
            HandleFactory<I, H> handleFactory,
            StreamStateHandle underlying,
            I channelInfo,
            StateContentMetaInfo contentMetaInfo)
            throws IOException {
        Optional<byte[]> bytes =
                underlying.asBytesIfInMemory(); // todo: consider restructuring channel state and
        // removing this method:
        // https://issues.apache.org/jira/browse/FLINK-17972
        if (bytes.isPresent()) {
            // 把该子分区的数据抽取出来
            StreamStateHandle extracted =
                    new ByteStreamStateHandle(
                            randomUUID().toString(),
                            serializer.extractAndMerge(bytes.get(), contentMetaInfo.getOffsets()));

            // 基于抽取后的handle 生成AbstractChannelStateHandle
            return handleFactory.create(
                    subtaskIndex,
                    channelInfo,
                    extracted,
                    singletonList(serializer.getHeaderLength()),  // 表示从header后的所有数据都是state
                    extracted.getStateSize());
        } else {
            // 表示使用文件存储的 没法优化 基本是使用原参数生成AbstractChannelStateHandle对象
            return handleFactory.create(
                    subtaskIndex,
                    channelInfo,
                    underlying,
                    contentMetaInfo.getOffsets(),
                    contentMetaInfo.getSize());
        }
    }

    /**
     * 处理失败了
     * @param e
     */
    public void fail(Throwable e) {
        result.fail(e);
    }

    public boolean isDone() {
        return this.result.isDone();
    }

    private interface HandleFactory<I, H extends AbstractChannelStateHandle<I>> {
        H create(
                int subtaskIndex,
                I info,
                StreamStateHandle underlying,
                List<Long> offsets,
                long size);

        HandleFactory<InputChannelInfo, InputChannelStateHandle> INPUT_CHANNEL =
                InputChannelStateHandle::new;

        HandleFactory<ResultSubpartitionInfo, ResultSubpartitionStateHandle> RESULT_SUBPARTITION =
                ResultSubpartitionStateHandle::new;
    }
}
