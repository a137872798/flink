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

package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.hash.InPlaceMutableHashTable;
import org.apache.flink.runtime.operators.sort.FixedLengthRecordSorter;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/** Chained version of ReduceCombineDriver. */
public class ChainedReduceCombineDriver<T> extends ChainedDriver<T, T> {

    private static final Logger LOG = LoggerFactory.getLogger(ChainedReduceCombineDriver.class);

    /**
     * Fix length records with a length below this threshold will be in-place sorted, if possible.
     */
    private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;

    private AbstractInvokable parent;

    private TypeSerializer<T> serializer;

    private TypeComparator<T> comparator;

    /**
     * 主要就是reduce函数
     */
    private ReduceFunction<T> reducer;

    private DriverStrategy strategy;

    /**
     * 存储数据的容器 提供了排序的api
     */
    private InMemorySorter<T> sorter;

    /**
     * 提供快速排序算法
     */
    private QuickSort sortAlgo = new QuickSort();

    /**
     * 简单理解就是hash表 通过线性探测法解决冲突
     */
    private InPlaceMutableHashTable<T> table;

    /**
     * 该对象可以遍历hash桶的数据
     */
    private InPlaceMutableHashTable<T>.ReduceFacade reduceFacade;

    private List<MemorySegment> memory;

    private volatile boolean running;

    // ------------------------------------------------------------------------

    @Override
    public Function getStub() {
        return reducer;
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    @Override
    public void setup(AbstractInvokable parent) {
        this.parent = parent;
        running = true;

        strategy = config.getDriverStrategy();

        reducer = BatchTask.instantiateUserCode(config, userCodeClassLoader, ReduceFunction.class);
        FunctionUtils.setFunctionRuntimeContext(reducer, getUdfRuntimeContext());
    }

    /**
     * 开启任务
     * @throws Exception
     */
    @Override
    public void openTask() throws Exception {
        // open the stub first
        final Configuration stubConfig = config.getStubParameters();
        BatchTask.openUserCode(reducer, stubConfig);

        // instantiate the serializer / comparator
        serializer = config.<T>getInputSerializer(0, userCodeClassLoader).getSerializer();
        comparator = config.<T>getDriverComparator(0, userCodeClassLoader).createComparator();

        MemoryManager memManager = parent.getEnvironment().getMemoryManager();
        final int numMemoryPages =
                memManager.computeNumberOfPages(config.getRelativeMemoryDriver());

        // 申请一定数量的内存块
        memory = memManager.allocatePages(parent, numMemoryPages);

        LOG.debug(
                "ChainedReduceCombineDriver object reuse: "
                        + (objectReuseEnabled ? "ENABLED" : "DISABLED")
                        + ".");

        // 根据不同策略 初始化不同对象
        switch (strategy) {
            case SORTED_PARTIAL_REDUCE:
                // instantiate a fix-length in-place sorter, if possible, otherwise the out-of-place
                // sorter
                if (comparator.supportsSerializationWithKeyNormalization()
                        && serializer.getLength() > 0
                        && serializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING) {
                    // 使用固定长度的对象
                    sorter =
                            new FixedLengthRecordSorter<T>(
                                    serializer, comparator.duplicate(), memory);
                } else {
                    sorter = new NormalizedKeySorter<T>(serializer, comparator.duplicate(), memory);
                }
                break;
            case HASHED_PARTIAL_REDUCE:
                table = new InPlaceMutableHashTable<T>(serializer, comparator, memory);
                table.open();
                reduceFacade = table.new ReduceFacade(reducer, outputCollector, objectReuseEnabled);
                break;
        }
    }

    @Override
    public void collect(T record) {
        try {
            switch (strategy) {
                case SORTED_PARTIAL_REDUCE:
                    collectSorted(record);
                    break;
                case HASHED_PARTIAL_REDUCE:
                    collectHashed(record);
                    break;
            }
        } catch (Exception ex) {
            throw new ExceptionInChainedStubException(taskName, ex);
        }
    }

    /**
     * 基于排序对象采集数据
     * @param record
     * @throws Exception
     */
    private void collectSorted(T record) throws Exception {
        // try writing to the sorter first
        if (!sorter.write(record)) {
            // it didn't succeed; sorter is full

            // do the actual sorting, combining, and data writing

            // 表示此时sorter已经写满了   排序后将equals为true的记录合并并下发
            sortAndCombine();
            // 清空内部数据
            sorter.reset();

            // write the value again
            if (!sorter.write(record)) {
                throw new IOException(
                        "Cannot write record to fresh sort buffer. Record too large.");
            }
        }
    }

    /**
     * 基于hash的形式合并数据
     * @param record
     * @throws Exception
     */
    private void collectHashed(T record) throws Exception {
        try {
            // insert or update    update时 相同key会进行合并    sorter就是在满了时触发合并 hash是每次插入都可能合并
            // sorter的内存开销会更大
            reduceFacade.updateTableEntryWithReduce(record);
        } catch (EOFException ex) {
            // the table has run out of memory
            // 将hash数据发往下游
            reduceFacade.emitAndReset();
            // try again
            reduceFacade.updateTableEntryWithReduce(record);
        }
    }

    /**
     * 当sorter内部的数据已经写满了后 触发该方法
     * @throws Exception
     */
    private void sortAndCombine() throws Exception {
        final InMemorySorter<T> sorter = this.sorter;

        if (!sorter.isEmpty()) {
            // 使用快排算法 将内部数据排序
            sortAlgo.sort(sorter);

            final TypeSerializer<T> serializer = this.serializer;
            final TypeComparator<T> comparator = this.comparator;
            final ReduceFunction<T> function = this.reducer;
            final Collector<T> output = this.outputCollector;
            final MutableObjectIterator<T> input = sorter.getIterator();

            if (objectReuseEnabled) {
                // We only need two objects. The first reference stores results and is
                // eventually collected. New values are read into the second.
                //
                // The output value must have the same key fields as the input values.

                T reuse1 = input.next();
                T reuse2 = serializer.createInstance();

                T value = reuse1;

                // iterate over key groups
                while (running && value != null) {
                    comparator.setReference(value);

                    // iterate within a key group
                    while ((reuse2 = input.next(reuse2)) != null) {
                        // 从sorter中顺序取出元素 当发现与下个元素equals相同时 触发聚合函数
                        if (comparator.equalToReference(reuse2)) {
                            // same group, reduce
                            value = function.reduce(value, reuse2);

                            // we must never read into the object returned
                            // by the user, so swap the reuse objects
                            if (value == reuse2) {
                                T tmp = reuse1;
                                reuse1 = reuse2;
                                reuse2 = tmp;
                            }
                        } else {
                            // new key group
                            break;
                        }
                    }

                    // 相同key的值聚合后 发送到下游
                    output.collect(value);

                    // swap the value from the new key group into the first object
                    // 发现了不同的值 将其作为value 进入下轮循环
                    T tmp = reuse1;
                    reuse1 = reuse2;
                    reuse2 = tmp;

                    value = reuse1;
                }
            } else {
                // 对象不可复用  每次创建新对象
                T value = input.next();

                // iterate over key groups
                while (running && value != null) {
                    comparator.setReference(value);
                    T res = value;

                    // iterate within a key group
                    while ((value = input.next()) != null) {
                        if (comparator.equalToReference(value)) {
                            // same group, reduce
                            res = function.reduce(res, value);
                        } else {
                            // new key group
                            break;
                        }
                    }

                    output.collect(res);
                }
            }
        }
    }

    @Override
    public void close() {
        // send the final batch
        try {
            switch (strategy) {
                case SORTED_PARTIAL_REDUCE:
                    sortAndCombine();
                    break;
                case HASHED_PARTIAL_REDUCE:
                    reduceFacade.emit();
                    break;
            }
        } catch (Exception ex2) {
            throw new ExceptionInChainedStubException(taskName, ex2);
        }

        outputCollector.close();
        dispose(false);
    }

    @Override
    public void closeTask() throws Exception {
        if (running) {
            BatchTask.closeUserCode(reducer);
        }
    }

    @Override
    public void cancelTask() {
        running = false;
        dispose(true);
    }

    private void dispose(boolean ignoreException) {
        try {
            if (sorter != null) {
                sorter.dispose();
            }
            if (table != null) {
                table.close();
            }
        } catch (Exception e) {
            // May happen during concurrent modification.
            if (!ignoreException) {
                throw e;
            }
        } finally {
            parent.getEnvironment().getMemoryManager().release(memory);
        }
    }
}
