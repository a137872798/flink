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

package org.apache.flink.api.common.operators.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.TraversableOnceException;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * The KeyValueIterator returns a key and all values that belong to the key (share the same key).
 * 该迭代器返回一个key  以及与该key相关的所有value
 */
@Internal
public final class ListKeyGroupedIterator<E> {

    /**
     * 所有元素都存放在该list中
     */
    private final List<E> input;

    private final TypeSerializer<E> serializer; // != null if the elements should be copied

    private final TypeComparator<E> comparator;

    private ValuesIterator valuesIterator;

    private int currentPosition = 0;

    /**
     * 当切换到一个新的key时 会设置该对象
     */
    private E lookahead;

    private boolean done;

    /**
     * Initializes the ListKeyGroupedIterator..
     *
     * @param input The list with the input elements.
     * @param comparator The comparator for the data type iterated over.
     */
    public ListKeyGroupedIterator(
            List<E> input, TypeSerializer<E> serializer, TypeComparator<E> comparator) {
        if (input == null || comparator == null) {
            throw new NullPointerException();
        }

        this.input = input;
        this.serializer = serializer;
        this.comparator = comparator;

        this.done = input.isEmpty() ? true : false;
    }

    /**
     * Moves the iterator to the next key. This method may skip any values that have not yet been
     * returned by the iterator created by the {@link #getValues()} method. Hence, if called
     * multiple times it "removes" key groups.
     *
     * @return true, if the input iterator has an other group of records with the same key.
     * 当迭代器遍历完同一个key的所有value后 需要通过该方法切换到下个key
     */
    public boolean nextKey() throws IOException {

        // 之前已经扫描到一个新的key了
        if (lookahead != null) {
            // common case: whole value-iterator was consumed and a new key group is available.
            // 直接设置
            this.comparator.setReference(this.lookahead);
            // 设置迭代器的 next 元素 这样下次调用next 就会返回新key对应的对象
            this.valuesIterator.next = this.lookahead;
            this.lookahead = null;
            this.valuesIterator.iteratorAvailable = true;
            return true;
        }

        // first element, empty/done, or the values iterator was not entirely consumed
        // 所有元素都已经消耗完  返回false
        if (this.done) {
            return false;
        }

        if (this.valuesIterator != null) {
            // values was not entirely consumed. move to the next key
            // Required if user code / reduce() method did not read the whole value iterator.
            E next;
            while (true) {
                if (currentPosition < input.size()
                        && (next = this.input.get(currentPosition++)) != null) {
                    // 如果认定相同 进入下次循环
                    if (!this.comparator.equalToReference(next)) {
                        // the keys do not match, so we have a new group. store the current key
                        this.comparator.setReference(next);
                        this.valuesIterator.next = next;
                        this.valuesIterator.iteratorAvailable = true;
                        return true;
                    }
                } else {
                    // input exhausted  所有元素都读完了
                    this.valuesIterator.next = null;
                    this.valuesIterator = null;
                    this.done = true;
                    return false;
                }
            }
        } else {
            // first element
            // get the next element
            // 首次调用进入该分支
            E first = input.get(currentPosition++);
            if (first != null) {
                this.comparator.setReference(first);
                // 首次初始化 valuesIterator
                this.valuesIterator = new ValuesIterator(first, serializer);
                return true;
            } else {
                // empty input, set everything null
                this.done = true;
                return false;
            }
        }
    }

    /**
     * ValuesIterator 每次返回新元素前 需要调用该方法
     * @return
     */
    private E advanceToNext() {

        // 还未遍历完所有元素
        if (currentPosition < input.size()) {
            E next = input.get(currentPosition++);
            // comparator 可以提取出对象的关键字段进行比较  相同则代表key 一致 可以被返回
            if (comparator.equalToReference(next)) {
                // same key
                return next;
            } else {
                // moved to the next key, no more values here
                // 切换到下个key 同时本次返回null 代表下面没有value了
                lookahead = next;
                return null;
            }
        } else {
            // backing iterator is consumed  所有元素都已经消耗完
            this.done = true;
            return null;
        }
    }

    /**
     * Returns an iterator over all values that belong to the current key. The iterator is initially
     * <code>null</code> (before the first call to {@link #nextKey()} and after all keys are
     * consumed. In general, this method returns always a non-null value, if a previous call to
     * {@link #nextKey()} return <code>true</code>.
     *
     * @return Iterator over all values that belong to the current key.
     */
    public ValuesIterator getValues() {
        return this.valuesIterator;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 代表一个值迭代器
     */
    public final class ValuesIterator implements Iterator<E>, Iterable<E> {

        /**
         * 下个被返回的元素
         */
        private E next;

        /**
         * 每当切换key的时候 要重新调用 iterator() (该值会被反复重置成true)
         */
        private boolean iteratorAvailable = true;

        /**
         * 该对象可以序列化/反序列化元素
         */
        private final TypeSerializer<E> serializer;

        private ValuesIterator(E first, TypeSerializer<E> serializer) {
            this.next = first;
            this.serializer = serializer;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public E next() {
            if (this.next != null) {
                E current = this.next;
                // 取下个元素
                this.next = ListKeyGroupedIterator.this.advanceToNext();
                // 返回副本 确保原对象不受影响
                return serializer.copy(current);
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<E> iterator() {
            if (iteratorAvailable) {
                iteratorAvailable = false;
                return this;
            } else {
                throw new TraversableOnceException();
            }
        }

        public E getCurrent() {
            return next;
        }
    }
}
