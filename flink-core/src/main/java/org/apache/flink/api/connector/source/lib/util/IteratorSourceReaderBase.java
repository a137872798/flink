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

package org.apache.flink.api.connector.source.lib.util;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link SourceReader} that returns the values of an iterator, supplied via an {@link
 * IteratorSourceSplit}.
 *
 * <p>The {@code IteratorSourceSplit} is also responsible for taking the current iterator and
 * turning it back into a split for checkpointing.
 *
 * @param <E> The type of events returned by the reader.
 * @param <IterT> The type of the iterator that produces the events. This type exists to make the
 *     conversion between iterator and {@code IteratorSourceSplit} type safe.
 * @param <SplitT> The concrete type of the {@code IteratorSourceSplit} that creates and converts
 *     the iterator that produces this reader's elements.
 */
@Public
public abstract class IteratorSourceReaderBase<
                E, O, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>>
        implements SourceReader<O, SplitT> {

    /** The context for this reader, to communicate with the enumerator.
     * 这个是reader相关的上下文对象 可以获取配置也可以申请增加split
     * */
    private final SourceReaderContext context;

    /** The availability future. This reader is available as soon as a split is assigned.
     * 表示流处于可用状态
     * */
    private CompletableFuture<Void> availability;

    /**
     * The iterator producing data. Non-null after a split has been assigned. This field is null or
     * non-null always together with the {@link #currentSplit} field.
     * 当产生一个split后 会转换成迭代器  当split读取完毕后 会切换到下个split  转换成新的迭代器
     */
    @Nullable private IterT iterator;

    /**
     * The split whose data we return. Non-null after a split has been assigned. This field is null
     * or non-null always together with the {@link #iterator} field.
     *
     * 当前被使用的split
     */
    @Nullable private SplitT currentSplit;

    /** The remaining splits that were assigned but not yet processed.
     * 表示被产生 但还未发送数据的split
     * */
    private final Queue<SplitT> remainingSplits;

    /**
     * 代表没有split了
     */
    private boolean noMoreSplits;

    public IteratorSourceReaderBase(SourceReaderContext context) {
        this.context = checkNotNull(context);
        this.availability = new CompletableFuture<>();
        this.remainingSplits = new ArrayDeque<>();
    }

    // ------------------------------------------------------------------------

    @Override
    public void start() {
        // request a split if we don't have one
        // 启动后 此时split为空 需要上下文发送产生split的请求
        if (remainingSplits.isEmpty()) {
            context.sendSplitRequest();
        }
        start(context);
    }

    protected void start(SourceReaderContext context) {}

    /**
     *
     * @param output
     * @return
     */
    @Override
    public InputStatus pollNext(ReaderOutput<O> output) {
        if (iterator != null) {
            if (iterator.hasNext()) {
                // 通过迭代器拿到元素 并通过output发往下游
                output.collect(convert(iterator.next()));
                return InputStatus.MORE_AVAILABLE;
            } else {
                finishSplit();
            }
        }
        // 获取下一个split
        final InputStatus inputStatus = tryMoveToNextSplit();
        // 此时元素已经得到补充 重新从迭代器获取
        if (inputStatus == InputStatus.MORE_AVAILABLE) {
            output.collect(convert(iterator.next()));
        }
        return inputStatus;
    }

    protected abstract O convert(E value);

    /**
     * 表示当前split已经被读完了
     */
    private void finishSplit() {
        iterator = null;
        currentSplit = null;

        // request another split if no other is left
        // we do this only here in the finishSplit part to avoid requesting a split
        // whenever the reader is polled and doesn't currently have a split
        // 需要请求协调者 申请一个新的split
        if (remainingSplits.isEmpty() && !noMoreSplits) {
            context.sendSplitRequest();
        }
    }

    /**
     * 获取下个split
     * @return
     */
    private InputStatus tryMoveToNextSplit() {
        // 先尝试从队列申请  单个节点可能会一次性申请多个split  不过如果该节点宕机了 任务会如何移动呢?
        currentSplit = remainingSplits.poll();
        if (currentSplit != null) {
            iterator = currentSplit.getIterator();
            return InputStatus.MORE_AVAILABLE;
        } else if (noMoreSplits) {
            return InputStatus.END_OF_INPUT;
        } else {
            // ensure we are not called in a loop by resetting the availability future
            if (availability.isDone()) {
                availability = new CompletableFuture<>();
            }

            // 代表 split还没有补充 此时处于不可用状态
            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return availability;
    }

    /**
     * 可能调用tryMoveToNextSplit时 split补充的没有这么快
     * @param splits The splits assigned by the split enumerator.
     */
    @Override
    public void addSplits(List<SplitT> splits) {
        remainingSplits.addAll(splits);
        // set availability so that pollNext is actually called
        availability.complete(null);
    }

    /**
     * 有协调者发起通知  告知该reader已经没有数据了
     */
    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
        // set availability so that pollNext is actually called
        availability.complete(null);
    }

    /**
     * 持久化一些关键信息 便于重启后继续任务  不过如果该节点始终不恢复 任务会丢失吗
     * @param checkpointId
     * @return
     */
    @Override
    public List<SplitT> snapshotState(long checkpointId) {
        if (currentSplit == null && remainingSplits.isEmpty()) {
            return Collections.emptyList();
        }

        final ArrayList<SplitT> allSplits = new ArrayList<>(1 + remainingSplits.size());

        // 当前迭代器内的数据 要包装成split
        if (iterator != null && iterator.hasNext()) {
            assert currentSplit != null;

            @SuppressWarnings("unchecked")
            final SplitT inProgressSplit =
                    (SplitT) currentSplit.getUpdatedSplitForIterator(iterator);
            allSplits.add(inProgressSplit);
        }
        allSplits.addAll(remainingSplits);
        return allSplits;
    }

    @Override
    public void close() throws Exception {}
}
