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

package org.apache.flink.runtime.state;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Class that contains the base algorithm for partitioning data into key-groups. This algorithm
 * currently works with two array (input, output) for optimal algorithmic complexity. Notice that
 * this could also be implemented over a single array, using some cuckoo-hashing-style element
 * replacement. This would have worse algorithmic complexity but better space efficiency. We
 * currently prefer the trade-off in favor of better algorithmic complexity.
 *
 * @param <T> type of the partitioned elements.
 *
 *           可以将数据按照keyGroup进行划分
 *           产生的PartitioningResult 对象 可以根据传入的keyGroupId 返回部分数据的迭代器
 *           优先队列会使用到该对象  因为到二叉堆的顺序跟keyGroup无关  所以要提取出keyGroup并重新排序
 */
public class KeyGroupPartitioner<T> {

    /**
     * The input data for the partitioning. All elements to consider must be densely in the index
     * interval [0, {@link #numberOfElements}[, without null values.
     */
    @Nonnull private final T[] partitioningSource;

    /**
     * The output array for the partitioning. The size must be {@link #numberOfElements} (or
     * bigger).
     */
    @Nonnull private final T[] partitioningDestination;

    /** Total number of input elements. */
    @Nonnegative private final int numberOfElements;

    /** The total number of key-groups in the job. */
    @Nonnegative private final int totalKeyGroups;

    /**
     * This bookkeeping array is used to count the elements in each key-group. In a second step, it
     * is transformed into a histogram by accumulation.
     * 每个元素是当前keyGroup与之前所有keyGroup出现的元素总数
     */
    @Nonnull private final int[] counterHistogram;

    /**
     * This is a helper array that caches the key-group for each element, so we do not have to
     * compute them twice.
     * 记录数组中每个元素对应的 keyGroup下标
     */
    @Nonnull private final int[] elementKeyGroups;

    /** Cached value of keyGroupRange#firstKeyGroup. */
    @Nonnegative private final int firstKeyGroup;

    /** Function to extract the key from a given element. */
    @Nonnull private final KeyExtractorFunction<T> keyExtractorFunction;

    /** Function to write an element to a {@link DataOutputView}. */
    @Nonnull private final ElementWriterFunction<T> elementWriterFunction;

    /** Cached result.
     * 缓存分区结果
     * */
    @Nullable private PartitioningResult<T> computedResult;

    /**
     * Creates a new {@link KeyGroupPartitioner}.
     *
     * @param partitioningSource the input for the partitioning. All elements must be densely packed
     *     in the index interval [0, {@link #numberOfElements}[, without null values.
     * @param numberOfElements the number of elements to consider from the input, starting at input
     *     index 0.
     * @param partitioningDestination the output of the partitioning. Must have capacity of at least
     *     numberOfElements.
     * @param keyGroupRange the key-group range of the data that will be partitioned by this
     *     instance.
     * @param totalKeyGroups the total number of key groups in the job.
     * @param keyExtractorFunction this function extracts the partition key from an element.
     */
    public KeyGroupPartitioner(
            @Nonnull T[] partitioningSource,  // 包含原始数据的数组
            @Nonnegative int numberOfElements,
            @Nonnull T[] partitioningDestination,  // 用于存储结果的数组
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int totalKeyGroups,
            @Nonnull KeyExtractorFunction<T> keyExtractorFunction,  // 该函数用于提取key
            @Nonnull ElementWriterFunction<T> elementWriterFunction) {  // 该对象用于将单个元素写入输出流 其实就是序列化

        Preconditions.checkState(partitioningSource != partitioningDestination);
        Preconditions.checkState(partitioningSource.length >= numberOfElements);
        Preconditions.checkState(partitioningDestination.length >= numberOfElements);

        this.partitioningSource = partitioningSource;
        this.partitioningDestination = partitioningDestination;
        this.numberOfElements = numberOfElements;
        this.totalKeyGroups = totalKeyGroups;
        this.keyExtractorFunction = keyExtractorFunction;
        this.elementWriterFunction = elementWriterFunction;
        this.firstKeyGroup = keyGroupRange.getStartKeyGroup();
        this.elementKeyGroups = new int[numberOfElements];
        this.counterHistogram = new int[keyGroupRange.getNumberOfKeyGroups()];
        this.computedResult = null;
    }

    /**
     * Partitions the data into key-groups and returns the result as a {@link PartitioningResult}.
     * 生成基于keyGroup分区的对象
     */
    public PartitioningResult<T> partitionByKeyGroup() {
        if (computedResult == null) {
            reportAllElementKeyGroups();
            // 得到总数
            int outputNumberOfElements = buildHistogramByAccumulatingCounts();
            executePartitioning(outputNumberOfElements);
        }
        return computedResult;
    }

    /** This method iterates over the input data and reports the key-group for each element. */
    protected void reportAllElementKeyGroups() {

        Preconditions.checkState(partitioningSource.length >= numberOfElements);

        // 遍历所有元素
        for (int i = 0; i < numberOfElements; ++i) {
            // 计算key.hashCode 并转换成 下标
            int keyGroup =
                    KeyGroupRangeAssignment.assignToKeyGroup(
                            keyExtractorFunction.extractKeyFromElement(partitioningSource[i]),
                            totalKeyGroups);
            reportKeyGroupOfElementAtIndex(i, keyGroup);
        }
    }

    /**
     * This method reports in the bookkeeping data that the element at the given index belongs to
     * the given key-group.
     */
    protected void reportKeyGroupOfElementAtIndex(int index, int keyGroup) {
        final int keyGroupIndex = keyGroup - firstKeyGroup;
        elementKeyGroups[index] = keyGroupIndex;
        ++counterHistogram[keyGroupIndex];
    }

    /**
     * This method creates a histogram from the counts per key-group in {@link #counterHistogram}.
     */
    private int buildHistogramByAccumulatingCounts() {
        int sum = 0;
        for (int i = 0; i < counterHistogram.length; ++i) {
            int currentSlotValue = counterHistogram[i];
            counterHistogram[i] = sum;
            sum += currentSlotValue;
        }
        return sum;
    }

    /**
     * 进行分区
     * @param outputNumberOfElements   表示总计多少元素
     */
    private void executePartitioning(int outputNumberOfElements) {

        // We repartition the entries by their pre-computed key-groups, using the histogram values
        // as write indexes
        for (int inIdx = 0; inIdx < outputNumberOfElements; ++inIdx) {
            // 兑换成该元素的keyGroup
            int effectiveKgIdx = elementKeyGroups[inIdx];
            // 这样操作后  相当于源数组的元素按照keyGroup的顺序重新排序
            int outIdx = counterHistogram[effectiveKgIdx]++;
            partitioningDestination[outIdx] = partitioningSource[inIdx];
        }

        this.computedResult =
                new PartitioningResultImpl<>(
                        elementWriterFunction,
                        firstKeyGroup,
                        counterHistogram,  // 此时每个元素存储的都是之前累加的结果
                        partitioningDestination);
    }

    /** This represents the result of key-group partitioning.
     * 可以指定keyGroup后 获取相关元素的迭代器
     * */
    public interface PartitioningResult<T> extends StateSnapshot.StateKeyGroupWriter {
        Iterator<T> iterator(int keyGroupId);
    }

    /** The data in {@link * #partitionedElements} is partitioned w.r.t. key group range. */
    private static class PartitioningResultImpl<T> implements PartitioningResult<T> {

        /** Function to write one element to a {@link DataOutputView}.
         * 该函数用于实现 StateKeyGroupWriter
         * */
        @Nonnull private final ElementWriterFunction<T> elementWriterFunction;

        /**
         * The exclusive-end-offsets for all key-groups of the covered range for the partitioning.
         * Exclusive-end-offset for key-group n is under keyGroupOffsets[n - firstKeyGroup].
         * 里面记录的是表示同一个keyGroup的元素终止的下标
         */
        @Nonnull private final int[] keyGroupOffsets;

        /**
         * Array with elements that are partitioned w.r.t. the covered key-group range. The start
         * offset for each key-group is in {@link #keyGroupOffsets}.
         * 存储已经被分好区的元素
         */
        @Nonnull private final T[] partitionedElements;

        /** The first key-group of the range covered in the partitioning.
         * 表示第一个keyGroup值
         * */
        @Nonnegative private final int firstKeyGroup;

        PartitioningResultImpl(
                @Nonnull ElementWriterFunction<T> elementWriterFunction,
                @Nonnegative int firstKeyGroup,
                @Nonnull int[] keyGroupEndOffsets,
                @Nonnull T[] partitionedElements) {
            this.elementWriterFunction = elementWriterFunction;
            this.firstKeyGroup = firstKeyGroup;
            this.keyGroupOffsets = keyGroupEndOffsets;
            this.partitionedElements = partitionedElements;
        }

        @Nonnegative
        private int getKeyGroupStartOffsetInclusive(int keyGroup) {
            int idx = keyGroup - firstKeyGroup - 1;
            return idx < 0 ? 0 : keyGroupOffsets[idx];
        }

        @Nonnegative
        private int getKeyGroupEndOffsetExclusive(int keyGroup) {
            return keyGroupOffsets[keyGroup - firstKeyGroup];
        }

        @Override
        public void writeStateInKeyGroup(@Nonnull DataOutputView dov, int keyGroupId)
                throws IOException {

            // 得到该keyGroup的范围
            int startOffset = getKeyGroupStartOffsetInclusive(keyGroupId);
            int endOffset = getKeyGroupEndOffsetExclusive(keyGroupId);

            // write number of mappings in key-group
            dov.writeInt(endOffset - startOffset);

            // write mappings  挨个写入
            for (int i = startOffset; i < endOffset; ++i) {
                elementWriterFunction.writeElement(partitionedElements[i], dov);
            }
        }

        /**
         * 通过下标锁定范围  并生成迭代器
         * @param keyGroupId
         * @return
         */
        @Override
        public Iterator<T> iterator(int keyGroupId) {
            int startOffset = getKeyGroupStartOffsetInclusive(keyGroupId);
            int endOffset = getKeyGroupEndOffsetExclusive(keyGroupId);

            return Arrays.stream(partitionedElements, startOffset, endOffset).iterator();
        }
    }

    /**
     * 加载快照数据时需要
     * @param readerFunction
     * @param elementConsumer
     * @param <T>
     * @return
     */
    public static <T> StateSnapshotKeyGroupReader createKeyGroupPartitionReader(
            @Nonnull ElementReaderFunction<T> readerFunction,
            @Nonnull KeyGroupElementsConsumer<T> elementConsumer) {
        return new PartitioningResultKeyGroupReader<>(readerFunction, elementConsumer);
    }

    /**
     * General algorithm to read key-grouped state that was written from a {@link
     * PartitioningResultImpl}.
     *
     * @param <T> type of the elements to read.
     *           上面的是writer对象  与之对应的是reader对象
     */
    private static class PartitioningResultKeyGroupReader<T>
            implements StateSnapshotKeyGroupReader {

        @Nonnull private final ElementReaderFunction<T> readerFunction;

        @Nonnull private final KeyGroupElementsConsumer<T> elementConsumer;

        public PartitioningResultKeyGroupReader(
                @Nonnull ElementReaderFunction<T> readerFunction,
                @Nonnull KeyGroupElementsConsumer<T> elementConsumer) {

            this.readerFunction = readerFunction;
            this.elementConsumer = elementConsumer;
        }

        @Override
        public void readMappingsInKeyGroup(@Nonnull DataInputView in, @Nonnegative int keyGroupId)
                throws IOException {
            int numElements = in.readInt();
            for (int i = 0; i < numElements; i++) {
                T element = readerFunction.readElement(in);
                elementConsumer.consume(element, keyGroupId);
            }
        }
    }

    /**
     * This functional interface defines how one element is written to a {@link DataOutputView}.
     *
     * @param <T> type of the written elements.
     */
    @FunctionalInterface
    public interface ElementWriterFunction<T> {

        /**
         * This method defines how to write a single element to the output.
         *
         * @param element the element to be written.
         * @param dov the output view to write the element.
         * @throws IOException on write-related problems.
         */
        void writeElement(@Nonnull T element, @Nonnull DataOutputView dov) throws IOException;
    }

    /**
     * This functional interface defines how one element is read from a {@link DataInputView}.
     *
     * @param <T> type of the read elements.
     */
    @FunctionalInterface
    public interface ElementReaderFunction<T> {

        @Nonnull
        T readElement(@Nonnull DataInputView div) throws IOException;
    }

    /**
     * Functional interface to consume elements from a key group.
     *
     * @param <T> type of the consumed elements.
     */
    @FunctionalInterface
    public interface KeyGroupElementsConsumer<T> {
        void consume(@Nonnull T element, @Nonnegative int keyGroupId) throws IOException;
    }
}
