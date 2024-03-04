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

import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.util.CollectionUtil;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A heap-based priority queue with set semantics, based on {@link HeapPriorityQueue}. The heap is
 * supported by hash set for fast contains (de-duplication) and deletes. Object identification
 * happens based on {@link #equals(Object)}.
 *
 * <p>Possible future improvements:
 *
 * <ul>
 *   <li>We could also implement shrinking for the heap and the deduplication set.
 *   <li>We could replace the deduplication maps with more efficient custom implementations. In
 *       particular, a hash set would be enough if it could return existing elements on unsuccessful
 *       adding, etc..
 * </ul>
 *
 * @param <T> type of the contained elements.
 *
 *           整个优先队列中包含了各种key 然后每个key对应一组state
 */
public class HeapPriorityQueueSet<T extends HeapPriorityQueueElement> extends HeapPriorityQueue<T>
        implements KeyGroupedInternalPriorityQueue<T> {

    /** Function to extract the key from contained elements.
     * 告知如何从T中抽取出 key
     * */
    private final KeyExtractorFunction<T> keyExtractor;

    /**
     * This array contains one hash set per key-group. The sets are used for fast de-duplication and
     * deletes of elements.
     * 每个map 对应一个key
     */
    private final HashMap<T, T>[] deduplicationMapsByKeyGroup;

    /** The key-group range of elements that are managed by this queue. */
    private final KeyGroupRange keyGroupRange;

    /** The total number of key-groups of the job. */
    private final int totalNumberOfKeyGroups;

    /**
     * Creates an empty {@link HeapPriorityQueueSet} with the requested initial capacity.
     *
     * @param elementPriorityComparator comparator for the priority of contained elements.
     * @param keyExtractor function to extract a key from the contained elements.
     * @param minimumCapacity the minimum and initial capacity of this priority queue.
     * @param keyGroupRange the key-group range of the elements in this set.
     * @param totalNumberOfKeyGroups the total number of key-groups of the job.
     */
    @SuppressWarnings("unchecked")
    public HeapPriorityQueueSet(
            @Nonnull PriorityComparator<T> elementPriorityComparator,
            @Nonnull KeyExtractorFunction<T> keyExtractor,
            @Nonnegative int minimumCapacity,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int totalNumberOfKeyGroups) {

        super(elementPriorityComparator, minimumCapacity);

        this.keyExtractor = keyExtractor;

        this.totalNumberOfKeyGroups = totalNumberOfKeyGroups;
        this.keyGroupRange = keyGroupRange;

        final int keyGroupsInLocalRange = keyGroupRange.getNumberOfKeyGroups();

        // 按照key分组
        final int deduplicationSetSize = 1 + minimumCapacity / keyGroupsInLocalRange;

        // 每个key 对应一个map
        this.deduplicationMapsByKeyGroup = new HashMap[keyGroupsInLocalRange];

        // 期望每个key 存储一定的数据 每个key分到的应该是不一样的 那么存在map中也合理
        for (int i = 0; i < keyGroupsInLocalRange; ++i) {
            deduplicationMapsByKeyGroup[i] =
                    CollectionUtil.newHashMapWithExpectedSize(deduplicationSetSize);
        }
    }

    @Override
    @Nullable
    public T poll() {
        // 弹出第一个元素
        final T toRemove = super.poll();
        return toRemove != null ? getDedupMapForElement(toRemove).remove(toRemove) : null;
    }

    /**
     * Adds the element to the queue. In contrast to the superclass and to maintain set semantics,
     * this happens only if no such element is already contained (determined by {@link
     * #equals(Object)}).
     *
     * @return <code>true</code> if the operation changed the head element or if is it unclear if
     *     the head element changed. Only returns <code>false</code> iff the head element was not
     *     changed by this operation.
     */
    @Override
    public boolean add(@Nonnull T element) {
        // 元素先加入map
        return getDedupMapForElement(element).putIfAbsent(element, element) == null
                && super.add(element);
    }

    /**
     * In contrast to the superclass and to maintain set semantics, removal here is based on
     * comparing the given element via {@link #equals(Object)}.
     *
     * @return <code>true</code> if the operation changed the head element or if is it unclear if
     *     the head element changed. Only returns <code>false</code> iff the head element was not
     *     changed by this operation.
     *
     */
    @Override
    public boolean remove(@Nonnull T toRemove) {
        // 移除也是一样的  先定位到map  然后移除
        T storedElement = getDedupMapForElement(toRemove).remove(toRemove);
        return storedElement != null && super.remove(storedElement);
    }

    @Override
    public void clear() {
        super.clear();
        // 清除每个map
        for (HashMap<?, ?> elementHashMap : deduplicationMapsByKeyGroup) {
            elementHashMap.clear();
        }
    }

    /**
     * 这里直接是兑换成下标检索  不需要计算hash了
     * @param keyGroupId
     * @return
     */
    private HashMap<T, T> getDedupMapForKeyGroup(@Nonnegative int keyGroupId) {
        return deduplicationMapsByKeyGroup[globalKeyGroupToLocalIndex(keyGroupId)];
    }

    /**
     * 找到元素的key对应的map
     * @param element
     * @return
     */
    private HashMap<T, T> getDedupMapForElement(T element) {
        // 如何判断该元素的key值  不是直接从元素上获得的  而是取到一个可以计算key的字段 计算hash 再/totalNumberOfKeyGroups
        // 剩下的才是key 因为hash totalNumberOfKeyGroups 都是不变的 或者说只要totalNumberOfKeyGroups这个不变 那么相关的元素计算的key就是不变的
        int keyGroup =
                KeyGroupRangeAssignment.assignToKeyGroup(
                        keyExtractor.extractKeyFromElement(element), totalNumberOfKeyGroups);
        // 通过key检索到map
        return getDedupMapForKeyGroup(keyGroup);
    }

    private int globalKeyGroupToLocalIndex(int keyGroup) {
        checkArgument(
                keyGroupRange.contains(keyGroup),
                "%s does not contain key group %s",
                keyGroupRange,
                keyGroup);
        return keyGroup - keyGroupRange.getStartKeyGroup();
    }

    /**
     * 这里就可以通过某个key找到部分状态了
     * @param keyGroupId
     * @return
     */
    @Nonnull
    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroupId) {
        return getDedupMapForKeyGroup(keyGroupId).keySet();
    }
}
