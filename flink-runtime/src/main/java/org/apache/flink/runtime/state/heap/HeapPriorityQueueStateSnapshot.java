/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupPartitioner;
import org.apache.flink.runtime.state.KeyGroupPartitioner.PartitioningResult;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.lang.reflect.Array;
import java.util.Iterator;

/**
 * This class represents the snapshot of an {@link HeapPriorityQueueSet}.
 *
 * @param <T> type of the state elements.
 *           使用优先队列的结构存储状态时 对应的快照
 */
public class HeapPriorityQueueStateSnapshot<T> implements StateSnapshot {

    /** Function that extracts keys from elements. */
    @Nonnull private final KeyExtractorFunction<T> keyExtractor;

    /** Copy of the heap array containing all the (immutable or deeply copied) elements.
     * 这个就是优先队列的二叉堆
     * */
    @Nonnull private final T[] heapArrayCopy;

    /** The meta info of the state.
     * 元数据对象 一般就是存储写序列化对象
     * */
    @Nonnull private final RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo;

    /** The key-group range covered by this snapshot.
     * 表示 key的范围
     * */
    @Nonnull private final KeyGroupRange keyGroupRange;

    /** The total number of key-groups in the job. */
    @Nonnegative private final int totalKeyGroups;

    /** Result of partitioning the snapshot by key-group.
     * 可以使用keyGroup对该对象进行分区   进而得到部分数据
     * */
    @Nullable private PartitioningResult<T> partitioningResult;

    HeapPriorityQueueStateSnapshot(
            @Nonnull T[] heapArrayCopy,
            @Nonnull KeyExtractorFunction<T> keyExtractor,
            @Nonnull RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int totalKeyGroups) {

        this.keyExtractor = keyExtractor;
        this.heapArrayCopy = heapArrayCopy;
        this.metaInfo = metaInfo;
        this.keyGroupRange = keyGroupRange;
        this.totalKeyGroups = totalKeyGroups;
    }

    @Nonnull
    @Override
    public StateKeyGroupWriter getKeyGroupWriter() {
        return getPartitioningResult();
    }

    public Iterator<T> getIteratorForKeyGroup(int keyGroupId) {
        return getPartitioningResult().iterator(keyGroupId);
    }

    /**
     * 产生一个基于keyGroup分区的对象
     * @return
     */
    @SuppressWarnings("unchecked")
    private PartitioningResult<T> getPartitioningResult() {
        if (partitioningResult == null) {

            // 生成一个新数组
            T[] partitioningOutput =
                    (T[])
                            Array.newInstance(
                                    heapArrayCopy.getClass().getComponentType(),
                                    heapArrayCopy.length);

            final TypeSerializer<T> elementSerializer = metaInfo.getElementSerializer();

            KeyGroupPartitioner<T> keyGroupPartitioner =
                    new KeyGroupPartitioner<>(
                            heapArrayCopy,
                            heapArrayCopy.length,
                            partitioningOutput,
                            keyGroupRange,
                            totalKeyGroups,
                            keyExtractor,
                            elementSerializer::serialize);

            partitioningResult = keyGroupPartitioner.partitionByKeyGroup();
        }

        return partitioningResult;
    }

    @Nonnull
    @Override
    public StateMetaInfoSnapshot getMetaInfoSnapshot() {
        return metaInfo.snapshot();
    }

    @Nonnull
    public RegisteredPriorityQueueStateBackendMetaInfo<T> getMetaInfo() {
        return metaInfo;
    }

    @Override
    public void release() {}
}
