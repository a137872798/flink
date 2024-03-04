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

package org.apache.flink.runtime.util;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.TraversableOnceException;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The KeyValueIterator returns a key and all values that belong to the key (share the same key).
 */
public final class ReusingKeyGroupedIterator<E> implements KeyGroupedIterator<E> {

    /**
     * 通过该对象来迭代数据  并且有个前提条件 也就是内部数据应当已经完成排序了
     */
    private final MutableObjectIterator<E> iterator;

    private final TypeSerializer<E> serializer;

    private final TypeComparator<E> comparator;

    private E reuse;

    private E current;

    private E lookahead;

    private ValuesIterator valuesIterator;

    private boolean lookAheadHasNext;

    private boolean done;

    /**
     * Initializes the KeyGroupedIterator. It requires an iterator which returns its result sorted
     * by the key fields.
     *
     * @param iterator An iterator over records, which are sorted by the key fields, in any order.
     * @param serializer The serializer for the data type iterated over.
     * @param comparator The comparator for the data type iterated over.
     */
    public ReusingKeyGroupedIterator(
            MutableObjectIterator<E> iterator,
            TypeSerializer<E> serializer,
            TypeComparator<E> comparator) {
        if (iterator == null || serializer == null || comparator == null) {
            throw new NullPointerException();
        }

        this.iterator = iterator;
        this.serializer = serializer;
        this.comparator = comparator;
        this.reuse = this.serializer.createInstance();
    }

    /**
     * Moves the iterator to the next key. This method may skip any values that have not yet been
     * returned by the iterator created by the {@link #getValues()} method. Hence, if called
     * multiple times it "removes" pairs.
     *
     * @return true if the input iterator has an other group of key-value pairs that share the same
     *     key.
     *     获取下个不同的key
     */
    @Override
    public boolean nextKey() throws IOException {
        // first element (or empty)
        if (this.current == null) {
            // 表示没有数据了 返回false
            if (this.done) {
                this.valuesIterator = null;
                return false;
            }
            this.current = this.reuse;
            // 读取数据
            if ((this.current = this.iterator.next(this.current)) != null) {
                this.comparator.setReference(this.current);
                this.lookAheadHasNext = false;
                // 准备好相同key的  value迭代器
                this.valuesIterator = new ValuesIterator();
                this.valuesIterator.currentIsUnconsumed = true;
                return true;
            } else {
                // empty input, set everything null
                // 表示没数据了
                this.valuesIterator = null;
                this.current = null;
                this.done = true;
                return false;
            }
        }

        // 表示ValuesIterator 可以迭代器
        this.valuesIterator.iteratorAvailable = true;

        // Whole value-iterator was read and a new key is available.
        // 表示已经窥探过下个key不同的值了
        if (this.lookAheadHasNext) {
            this.lookAheadHasNext = false;
            // 这是下个被窥探的值
            this.current = this.lookahead;
            this.lookahead = null;
            this.comparator.setReference(this.current);
            this.valuesIterator.currentIsUnconsumed = true;
            return true;
        }

        // try to move to next key.
        // Required if user code / reduce() method did not read the whole value iterator.
        while (true) {
            if (!this.done && ((this.current = this.iterator.next(this.current)) != null)) {
                // 这个是直到不相等为止
                if (!this.comparator.equalToReference(this.current)) {
                    // the keys do not match, so we have a new group. store the current keys
                    this.comparator.setReference(this.current);
                    this.lookAheadHasNext = false;
                    this.valuesIterator.currentIsUnconsumed = true;
                    return true;
                }
            } else {
                // 此时数据已经遍历完了
                this.valuesIterator = null;
                this.current = null;
                this.done = true;
                return false;
            }
        }
    }

    public TypeComparator<E> getComparatorWithCurrentReference() {
        return this.comparator;
    }

    @Override
    public E getCurrent() {
        return this.current;
    }

    /**
     * Returns an iterator over all values that belong to the current key. The iterator is initially
     * <code>null</code> (before the first call to {@link #nextKey()} and after all keys are
     * consumed. In general, this method returns always a non-null value, if a previous call to
     * {@link #nextKey()} return <code>true</code>.
     *
     * @return Iterator over all values that belong to the current key.
     */
    @Override
    public ValuesIterator getValues() {
        return this.valuesIterator;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 遍历相同key的一组value
     */
    public final class ValuesIterator implements Iterator<E>, Iterable<E> {

        private final TypeSerializer<E> serializer = ReusingKeyGroupedIterator.this.serializer;
        private final TypeComparator<E> comparator = ReusingKeyGroupedIterator.this.comparator;

        /**
         * 暂存对象  复用对象减少内存开销
         */
        private E staging = this.serializer.createInstance();
        private boolean currentIsUnconsumed = false;

        private boolean iteratorAvailable = true;

        private ValuesIterator() {}

        @Override
        public boolean hasNext() {
            // 必须先设置current
            // lookAheadHasNext 表示之前已经读取到下个key了
            if (ReusingKeyGroupedIterator.this.current == null
                    || ReusingKeyGroupedIterator.this.lookAheadHasNext) {
                return false;
            }
            if (this.currentIsUnconsumed) {
                return true;
            }

            try {
                // read the next value into the staging record to make sure we keep the
                // current as it is in case the key changed

                // 先读取下个元素
                E stagingStaging = ReusingKeyGroupedIterator.this.iterator.next(this.staging);
                if (stagingStaging != null) {
                    this.staging = stagingStaging;
                    // 表示发现了相同的值
                    if (this.comparator.equalToReference(this.staging)) {
                        // same key, next value is in staging, so exchange staging with current
                        final E tmp = this.staging;
                        this.staging = ReusingKeyGroupedIterator.this.current;
                        ReusingKeyGroupedIterator.this.current = tmp;
                        this.currentIsUnconsumed = true;
                        return true;
                    } else {
                        // moved to the next key, no more values here
                        // 下个值与当前值不想等  就表示上个key对应的values已经读完了
                        ReusingKeyGroupedIterator.this.lookAheadHasNext = true;
                        ReusingKeyGroupedIterator.this.lookahead = this.staging;
                        this.staging = ReusingKeyGroupedIterator.this.current;
                        return false;
                    }
                } else {
                    // backing iterator is consumed
                    // 迭代器已经被读完了 没数据了
                    ReusingKeyGroupedIterator.this.done = true;
                    return false;
                }
            } catch (IOException ioex) {
                throw new RuntimeException(
                        "An error occurred while reading the next record: " + ioex.getMessage(),
                        ioex);
            }
        }

        /** Prior to call this method, call hasNext() once! */
        @Override
        public E next() {
            if (this.currentIsUnconsumed || hasNext()) {
                this.currentIsUnconsumed = false;
                return ReusingKeyGroupedIterator.this.current;
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
    }
}
