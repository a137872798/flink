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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.hash.NonReusingBuildFirstHashJoinIterator;
import org.apache.flink.runtime.operators.hash.NonReusingBuildSecondHashJoinIterator;
import org.apache.flink.runtime.operators.hash.ReusingBuildFirstHashJoinIterator;
import org.apache.flink.runtime.operators.hash.ReusingBuildSecondHashJoinIterator;
import org.apache.flink.runtime.operators.sort.NonReusingMergeInnerJoinIterator;
import org.apache.flink.runtime.operators.sort.ReusingMergeInnerJoinIterator;
import org.apache.flink.runtime.operators.util.JoinTaskIterator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.operators.util.metrics.CountingCollector;
import org.apache.flink.runtime.operators.util.metrics.CountingMutableObjectIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The join driver implements the logic of a join operator at runtime. It instantiates either hash
 * or sort-merge based strategies to find joining pairs of records.
 *
 * @see org.apache.flink.api.common.functions.FlatJoinFunction
 * 该驱动对应的function是 FlatJoinFunction    将2个数据join后发往下游
 */
public class JoinDriver<IT1, IT2, OT> implements Driver<FlatJoinFunction<IT1, IT2, OT>, OT> {

    protected static final Logger LOG = LoggerFactory.getLogger(JoinDriver.class);

    /**
     * 读取在setup阶段 会关联一个上下文
     */
    protected TaskContext<FlatJoinFunction<IT1, IT2, OT>, OT> taskContext;

    /**
     * 这个迭代器针对的是2个stream  record是可以抽取出keys的 并且相同keys的一组数据被称为一个keyGroup
     * 该迭代器在读取数据时 就会将2个stream中keys相同的keyGroup读出来 并在交叉聚合后发往下游
     */
    private volatile JoinTaskIterator<IT1, IT2, OT>
            joinIterator; // the iterator that does the actual join

    /**
     * 表示当前驱动还在运行中
     */
    protected volatile boolean running;

    // ------------------------------------------------------------------------

    @Override
    public void setup(TaskContext<FlatJoinFunction<IT1, IT2, OT>, OT> context) {
        this.taskContext = context;
        this.running = true;
    }

    /**
     * 因为是2个stream的聚合 所以inputs数量为2
     * @return
     */
    @Override
    public int getNumberOfInputs() {
        return 2;
    }

    @Override
    public Class<FlatJoinFunction<IT1, IT2, OT>> getStubType() {
        @SuppressWarnings("unchecked")
        final Class<FlatJoinFunction<IT1, IT2, OT>> clazz =
                (Class<FlatJoinFunction<IT1, IT2, OT>>) (Class<?>) FlatJoinFunction.class;
        return clazz;
    }

    @Override
    public int getNumberOfDriverComparators() {
        return 2;
    }

    /**
     * 进行准备工作
     * @throws Exception
     */
    @Override
    public void prepare() throws Exception {
        final TaskConfig config = this.taskContext.getTaskConfig();

        final Counter numRecordsIn =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();

        // obtain task manager's memory manager and I/O manager
        final MemoryManager memoryManager = this.taskContext.getMemoryManager();
        final IOManager ioManager = this.taskContext.getIOManager();

        // set up memory and I/O parameters
        final double fractionAvailableMemory = config.getRelativeMemoryDriver();
        final int numPages = memoryManager.computeNumberOfPages(fractionAvailableMemory);

        // test minimum memory requirements
        final DriverStrategy ls = config.getDriverStrategy();

        // 包装2个数据流   数据流是从context中获取的
        final MutableObjectIterator<IT1> in1 =
                new CountingMutableObjectIterator<>(
                        this.taskContext.<IT1>getInput(0), numRecordsIn);
        final MutableObjectIterator<IT2> in2 =
                new CountingMutableObjectIterator<>(
                        this.taskContext.<IT2>getInput(1), numRecordsIn);

        // get the key positions and types
        // 获取对应的序列化对象和比较器
        final TypeSerializer<IT1> serializer1 =
                this.taskContext.<IT1>getInputSerializer(0).getSerializer();
        final TypeSerializer<IT2> serializer2 =
                this.taskContext.<IT2>getInputSerializer(1).getSerializer();
        final TypeComparator<IT1> comparator1 = this.taskContext.getDriverComparator(0);
        final TypeComparator<IT2> comparator2 = this.taskContext.getDriverComparator(1);

        // 获取针对这2种类型的比较器
        final TypePairComparatorFactory<IT1, IT2> pairComparatorFactory =
                config.getPairComparatorFactory(this.taskContext.getUserCodeClassLoader());
        if (pairComparatorFactory == null) {
            throw new Exception("Missing pair comparator factory for join driver");
        }

        ExecutionConfig executionConfig = taskContext.getExecutionConfig();
        boolean objectReuseEnabled = executionConfig.isObjectReuseEnabled();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Join Driver object reuse: "
                            + (objectReuseEnabled ? "ENABLED" : "DISABLED")
                            + ".");
        }

        boolean hashJoinUseBitMaps =
                taskContext
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .getBoolean(AlgorithmOptions.HASH_JOIN_BLOOM_FILTERS);

        // create and return joining iterator according to provided local strategy.
        // 获取参数后 根据不同情况创建不同的迭代器
        if (objectReuseEnabled) {
            switch (ls) {
                case INNER_MERGE:
                    this.joinIterator =
                            new ReusingMergeInnerJoinIterator<>(
                                    in1,
                                    in2,
                                    serializer1,
                                    comparator1,
                                    serializer2,
                                    comparator2,
                                    pairComparatorFactory.createComparator12(
                                            comparator1, comparator2),
                                    memoryManager,
                                    ioManager,
                                    numPages,
                                    this.taskContext.getContainingTask());
                    break;
                    // first/second 表示以哪个为主
                case HYBRIDHASH_BUILD_FIRST:
                    this.joinIterator =
                            new ReusingBuildFirstHashJoinIterator<>(
                                    in1,
                                    in2,
                                    serializer1,
                                    comparator1,
                                    serializer2,
                                    comparator2,
                                    pairComparatorFactory.createComparator21(
                                            comparator1, comparator2),
                                    memoryManager,
                                    ioManager,
                                    this.taskContext.getContainingTask(),
                                    fractionAvailableMemory,
                                    false,
                                    false,
                                    hashJoinUseBitMaps);
                    break;
                case HYBRIDHASH_BUILD_SECOND:
                    this.joinIterator =
                            new ReusingBuildSecondHashJoinIterator<>(
                                    in1,
                                    in2,
                                    serializer1,
                                    comparator1,
                                    serializer2,
                                    comparator2,
                                    pairComparatorFactory.createComparator12(
                                            comparator1, comparator2),
                                    memoryManager,
                                    ioManager,
                                    this.taskContext.getContainingTask(),
                                    fractionAvailableMemory,
                                    false,
                                    false,
                                    hashJoinUseBitMaps);
                    break;
                default:
                    throw new Exception(
                            "Unsupported driver strategy for join driver: " + ls.name());
            }
        } else {
            switch (ls) {
                case INNER_MERGE:
                    this.joinIterator =
                            new NonReusingMergeInnerJoinIterator<>(
                                    in1,
                                    in2,
                                    serializer1,
                                    comparator1,
                                    serializer2,
                                    comparator2,
                                    pairComparatorFactory.createComparator12(
                                            comparator1, comparator2),
                                    memoryManager,
                                    ioManager,
                                    numPages,
                                    this.taskContext.getContainingTask());

                    break;
                case HYBRIDHASH_BUILD_FIRST:
                    this.joinIterator =
                            new NonReusingBuildFirstHashJoinIterator<>(
                                    in1,
                                    in2,
                                    serializer1,
                                    comparator1,
                                    serializer2,
                                    comparator2,
                                    pairComparatorFactory.createComparator21(
                                            comparator1, comparator2),
                                    memoryManager,
                                    ioManager,
                                    this.taskContext.getContainingTask(),
                                    fractionAvailableMemory,
                                    false,
                                    false,
                                    hashJoinUseBitMaps);
                    break;
                case HYBRIDHASH_BUILD_SECOND:
                    this.joinIterator =
                            new NonReusingBuildSecondHashJoinIterator<>(
                                    in1,
                                    in2,
                                    serializer1,
                                    comparator1,
                                    serializer2,
                                    comparator2,
                                    pairComparatorFactory.createComparator12(
                                            comparator1, comparator2),
                                    memoryManager,
                                    ioManager,
                                    this.taskContext.getContainingTask(),
                                    fractionAvailableMemory,
                                    false,
                                    false,
                                    hashJoinUseBitMaps);
                    break;
                default:
                    throw new Exception(
                            "Unsupported driver strategy for join driver: " + ls.name());
            }
        }

        // open the iterator - this triggers the sorting or hash-table building
        // and blocks until the iterator is ready
        // 进行初始化操作 1
        this.joinIterator.open();

        if (LOG.isDebugEnabled()) {
            LOG.debug(this.taskContext.formatLogString("join task iterator ready."));
        }
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void run() throws Exception {
        final Counter numRecordsOut =
                this.taskContext.getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();
        final FlatJoinFunction<IT1, IT2, OT> joinStub = this.taskContext.getStub();
        // 包装收集器 并追加计数能力
        final Collector<OT> collector =
                new CountingCollector<>(this.taskContext.getOutputCollector(), numRecordsOut);

        // 该对象会将一个流与另一个流的数据做hashJoin
        final JoinTaskIterator<IT1, IT2, OT> joinIterator = this.joinIterator;

        // 通过不断调用callWithNextKey 处理完所有数据
        while (this.running && joinIterator.callWithNextKey(joinStub, collector)) {}
    }

    @Override
    public void cleanup() throws Exception {
        if (this.joinIterator != null) {
            this.joinIterator.close();
            this.joinIterator = null;
        }
    }

    @Override
    public void cancel() {
        this.running = false;
        if (this.joinIterator != null) {
            this.joinIterator.abort();
        }
    }
}
