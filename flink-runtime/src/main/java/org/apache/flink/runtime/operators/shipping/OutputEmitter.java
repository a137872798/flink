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

package org.apache.flink.runtime.operators.shipping;

import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.MathUtils;

/**
 * The output emitter decides to which of the possibly multiple output channels a record is sent. It
 * implement routing based on hash-partitioning, broadcasting, round-robin, custom partition
 * functions, etc.
 *
 * @param <T> The type of the element handled by the emitter.
 *           在发送数据前 根据不同的策略选择不同的channel
 */
public class OutputEmitter<T> implements ChannelSelector<SerializationDelegate<T>> {

    /** the shipping strategy used by this output emitter
     * 搬运策略
     * */
    private final ShipStrategyType strategy;

    /** counter to go over channels round robin
     * 当使用 round robin时 推测会使用的channel */
    private int nextChannelToSendTo;

    /** the total number of output channels
     * 总计有多少条输出通道
     * */
    private int numberOfChannels;

    /** the comparator for hashing / sorting */
    private final TypeComparator<T> comparator;

    private Object[][] partitionBoundaries; // the partition boundaries for range partitioning

    private DataDistribution
            distribution; // the data distribution to create the partition boundaries for range
    // partitioning

    private final Partitioner<Object> partitioner;

    private TypeComparator[] flatComparators;

    /**
     * 参与分区计算的几个字段
     */
    private Object[] keys;

    private Object[] extractedKeys;

    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /**
     * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...)
     * and uses the supplied task index perform a round robin distribution.
     *
     * @param strategy The distribution strategy to be used.
     */
    public OutputEmitter(ShipStrategyType strategy, int indexInSubtaskGroup) {
        this(strategy, indexInSubtaskGroup, null, null, null);
    }

    /**
     * Creates a new channel selector that uses the given strategy (broadcasting, partitioning, ...)
     * and uses the supplied comparator to hash / compare records for partitioning them
     * deterministically.
     *
     * @param strategy The distribution strategy to be used.
     * @param comparator The comparator used to hash / compare the records.
     */
    public OutputEmitter(ShipStrategyType strategy, TypeComparator<T> comparator) {
        this(strategy, 0, comparator, null, null);
    }

    @SuppressWarnings("unchecked")
    public OutputEmitter(
            ShipStrategyType strategy,
            int indexInSubtaskGroup,
            TypeComparator<T> comparator,
            Partitioner<?> partitioner,
            DataDistribution distribution) {

        // 策略不能为空
        if (strategy == null) {
            throw new NullPointerException();
        }

        this.strategy = strategy;
        this.nextChannelToSendTo = indexInSubtaskGroup;
        this.comparator = comparator;
        this.partitioner = (Partitioner<Object>) partitioner;
        this.distribution = distribution;

        switch (strategy) {
            case PARTITION_CUSTOM:
                extractedKeys = new Object[1];
            case FORWARD:
            case PARTITION_HASH:
            case PARTITION_RANDOM:
            case PARTITION_FORCED_REBALANCE:
                break;
            case PARTITION_RANGE:
                if (comparator != null) {
                    this.flatComparators = comparator.getFlatComparators();
                    // 参与比较的几个字段都变成了key
                    this.keys = new Object[flatComparators.length];
                }
                break;
            case BROADCAST:
                break;
            default:
                throw new IllegalArgumentException(
                        "Invalid shipping strategy for OutputEmitter: " + strategy.name());
        }

        // 当采用自定义分区器时   partitioner不能为空
        if (strategy == ShipStrategyType.PARTITION_CUSTOM && partitioner == null) {
            throw new NullPointerException(
                    "Partitioner must not be null when the ship strategy is set to custom partitioning.");
        }
    }

    // ------------------------------------------------------------------------
    // Channel Selection
    // ------------------------------------------------------------------------

    /**
     * 设置总通道数
     * @param numberOfChannels the total number of output channels which are attached to respective
     *     output gate.
     *
     */
    @Override
    public void setup(int numberOfChannels) {
        this.numberOfChannels = numberOfChannels;
    }

    @Override
    public final int selectChannel(SerializationDelegate<T> record) {
        switch (strategy) {
            case FORWARD:
                // 总是返回第一个channel
                return forward();
            case PARTITION_RANDOM:
            case PARTITION_FORCED_REBALANCE:
                // 采用轮询策略
                return robin(numberOfChannels);
            case PARTITION_HASH:
                // 计算record hash值后 进行选择
                return hashPartitionDefault(record.getInstance(), numberOfChannels);
            case PARTITION_CUSTOM:
                // 使用自定义分区器 判断目标分区
                return customPartition(record.getInstance(), numberOfChannels);
            case PARTITION_RANGE:
                return rangePartition(record.getInstance(), numberOfChannels);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported distribution strategy: " + strategy.name());
        }
    }

    /**
     * 判断是否是 广播类型
     * @return
     */
    @Override
    public boolean isBroadcast() {
        if (strategy == ShipStrategyType.BROADCAST) {
            return true;
        } else {
            return false;
        }
    }

    // --------------------------------------------------------------------------------------------

    private int forward() {
        return 0;
    }

    /**
     * 采用轮询策略
     * @param numberOfChannels
     * @return
     */
    private int robin(int numberOfChannels) {
        int nextChannel = nextChannelToSendTo;
        if (nextChannel >= numberOfChannels) {
            if (nextChannel == numberOfChannels) {
                nextChannel = 0;
            } else {
                nextChannel %= numberOfChannels;
            }
        }
        nextChannelToSendTo = nextChannel + 1;

        return nextChannel;
    }

    /**
     * 计算hash值  根据hash值来选择
     * @param record
     * @param numberOfChannels
     * @return
     */
    private int hashPartitionDefault(T record, int numberOfChannels) {
        int hash = this.comparator.hash(record);

        return MathUtils.murmurHash(hash) % numberOfChannels;
    }

    /**
     * 在一定范围内 对应一个channel
     * @param record
     * @param numberOfChannels
     * @return
     */
    private int rangePartition(final T record, int numberOfChannels) {
        if (this.partitionBoundaries == null) {
            this.partitionBoundaries = new Object[numberOfChannels - 1][];
            for (int i = 0; i < numberOfChannels - 1; i++) {
                // 表示每个channel的范围
                this.partitionBoundaries[i] =
                        this.distribution.getBucketBoundary(i, numberOfChannels);
            }
        }

        if (numberOfChannels == this.partitionBoundaries.length + 1) {
            final Object[][] boundaries = this.partitionBoundaries;

            // bin search the bucket
            int low = 0;
            int high = this.partitionBoundaries.length - 1;

            while (low <= high) {
                final int mid = (low + high) >>> 1;
                // 二分查找
                final int result = compareRecordAndBoundary(record, boundaries[mid]);

                if (result > 0) {
                    low = mid + 1;
                } else if (result < 0) {
                    high = mid - 1;
                } else {
                    return mid;
                }
            }
            // key not found, but the low index is the target bucket, since the boundaries are the
            // upper bound
            return low;
        } else {
            throw new IllegalStateException(
                    "The number of channels to partition among is inconsistent with the partitioners state.");
        }
    }

    /**
     * 使用自定义分区对象
     * @param record
     * @param numberOfChannels
     * @return
     */
    private int customPartition(T record, int numberOfChannels) {
        if (extractedKeys == null) {
            extractedKeys = new Object[1];
        }

        try {
            // 只抽取第一个关键字
            if (comparator.extractKeys(record, extractedKeys, 0) == 1) {
                final Object key = extractedKeys[0];
                // 通过分区器来判断
                return partitioner.partition(key, numberOfChannels);
            } else {
                throw new RuntimeException(
                        "Inconsistency in the key comparator - comparator extracted more than one field.");
            }
        } catch (Throwable t) {
            throw new RuntimeException("Error while calling custom partitioner.", t);
        }
    }

    /**
     *
     * @param record
     * @param boundary  该数组中所有元素构成了一条分界线  根据与该分界线的关系 通过二分查找确定channel
     * @return
     */
    private final int compareRecordAndBoundary(T record, Object[] boundary) {
        // 从0开始抽取关键字
        this.comparator.extractKeys(record, keys, 0);

        if (flatComparators.length != keys.length || flatComparators.length > boundary.length) {
            throw new RuntimeException(
                    "Can not compare keys with boundary due to mismatched length.");
        }

        for (int i = 0; i < flatComparators.length; i++) {
            int result = flatComparators[i].compare(keys[i], boundary[i]);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }
}
