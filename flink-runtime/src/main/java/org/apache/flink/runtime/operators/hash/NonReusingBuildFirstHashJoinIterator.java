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

package org.apache.flink.runtime.operators.hash;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.util.JoinTaskIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.List;

/**
 * An implementation of the {@link org.apache.flink.runtime.operators.util.JoinTaskIterator} that
 * uses a hybrid-hash-join internally to match the records with equal key. The build side of the
 * hash is the first input of the match. This implementation DOES NOT reuse objects.
 * NonReusing 表示对象不会被重用
 */
public class NonReusingBuildFirstHashJoinIterator<V1, V2, O> extends HashJoinIteratorBase
        implements JoinTaskIterator<V1, V2, O> {

    /**
     * hash桶  分为build阶段和probe阶段
     */
    protected final MutableHashTable<V1, V2> hashJoin;

    protected final TypeSerializer<V2> probeSideSerializer;

    private final MemoryManager memManager;

    private final MutableObjectIterator<V1> firstInput;

    private final MutableObjectIterator<V2> secondInput;

    /**
     * 表示join的方式   如果外连接中probe为主  那么匹配不到数据时  使用null进行join
     */
    private final boolean probeSideOuterJoin;

    private final boolean buildSideOuterJoin;

    private volatile boolean running = true;

    // --------------------------------------------------------------------------------------------

    public NonReusingBuildFirstHashJoinIterator(
            MutableObjectIterator<V1> firstInput,
            MutableObjectIterator<V2> secondInput,
            TypeSerializer<V1> serializer1,
            TypeComparator<V1> comparator1,
            TypeSerializer<V2> serializer2,
            TypeComparator<V2> comparator2,
            TypePairComparator<V2, V1> pairComparator,
            MemoryManager memManager,
            IOManager ioManager,
            AbstractInvokable ownerTask,
            double memoryFraction,
            boolean probeSideOuterJoin,
            boolean buildSideOuterJoin,
            boolean useBitmapFilters)
            throws MemoryAllocationException {

        this.memManager = memManager;
        this.firstInput = firstInput;
        this.secondInput = secondInput;
        this.probeSideSerializer = serializer2;

        if (useBitmapFilters && probeSideOuterJoin) {
            throw new IllegalArgumentException(
                    "Bitmap filter may not be activated for joining with empty build side");
        }
        this.probeSideOuterJoin = probeSideOuterJoin;
        this.buildSideOuterJoin = buildSideOuterJoin;

        // 就是产生 MutableHashTable
        this.hashJoin =
                getHashJoin(
                        serializer1,
                        comparator1,
                        serializer2,
                        comparator2,
                        pairComparator,
                        memManager,
                        ioManager,
                        ownerTask,
                        memoryFraction,
                        useBitmapFilters);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void open() throws IOException, MemoryAllocationException, InterruptedException {
        // open 会填充数据 完成build阶段   同时生成探测数据迭代器
        this.hashJoin.open(this.firstInput, this.secondInput, this.buildSideOuterJoin);
    }

    /**
     * 关闭本对象
     */
    @Override
    public void close() {
        // close the join  会释放各种数据
        this.hashJoin.close();

        // free the memory
        final List<MemorySegment> segments = this.hashJoin.getFreedMemory();
        this.memManager.release(segments);
    }

    /**
     * 使用 flatJoin函数处理数据 并交给collector
     * @param matchFunction The match stub containing the match function which is called with the
     *     keys.
     * @param collector The collector to pass the match function.
     * @return
     * @throws Exception
     */
    @Override
    public final boolean callWithNextKey(
            FlatJoinFunction<V1, V2, O> matchFunction, Collector<O> collector) throws Exception {

        // 为探测做准备
        if (this.hashJoin.nextRecord()) {
            // we have a next record, get the iterators to the probe and build side values
            // 这个是利用bucket加速查找  并匹配数据用的  需要比较的数据在nextRecord时已经设置
            final MutableObjectIterator<V1> buildSideIterator =
                    this.hashJoin.getBuildSideIterator();

            // 获取此时使用的探测数据
            final V2 probeRecord = this.hashJoin.getCurrentProbeRecord();
            // 尝试为探测数据进行匹配
            V1 nextBuildSideRecord = buildSideIterator.next();

            // get the first build side value
            // 表示匹配成功
            if (probeRecord != null && nextBuildSideRecord != null) {
                V1 tmpRec;

                // check if there is another build-side value
                // 继续检索 查看是否有 hash和equals相同的数据
                if ((tmpRec = buildSideIterator.next()) != null) {
                    // more than one build-side value --> copy the probe side
                    V2 probeCopy;
                    // 这里采用数据拷贝的方式  也就是nonReusing
                    probeCopy = this.probeSideSerializer.copy(probeRecord);

                    // call match on the first pair
                    // 将2个匹配的数据进行合并 然后发送到下游
                    matchFunction.join(nextBuildSideRecord, probeCopy, collector);

                    // call match on the second pair
                    probeCopy = this.probeSideSerializer.copy(probeRecord);
                    matchFunction.join(tmpRec, probeCopy, collector);

                    // 直到所有与探测数据匹配的记录 都已经处理完
                    while (this.running
                            && ((nextBuildSideRecord = buildSideIterator.next()) != null)) {
                        // call match on the next pair
                        // make sure we restore the value of the probe side record
                        probeCopy = this.probeSideSerializer.copy(probeRecord);
                        matchFunction.join(nextBuildSideRecord, probeCopy, collector);
                    }
                } else {
                    // only single pair matches
                    matchFunction.join(nextBuildSideRecord, probeRecord, collector);
                }
            } else {
                // 表示匹配失败   这时就要根据左右连接来决定如何触发join了

                // while probe side outer join, join current probe record with null.
                if (probeSideOuterJoin && probeRecord != null && nextBuildSideRecord == null) {
                    matchFunction.join(null, probeRecord, collector);
                }

                // while build side outer join, iterate all build records which have not been probed
                // before,
                // and join with null.
                if (buildSideOuterJoin && probeRecord == null && nextBuildSideRecord != null) {
                    matchFunction.join(nextBuildSideRecord, null, collector);

                    while (this.running
                            && ((nextBuildSideRecord = buildSideIterator.next()) != null)) {
                        matchFunction.join(nextBuildSideRecord, null, collector);
                    }
                }
            }

            return true;
        } else {
            return false;
        }
    }

    @Override
    public void abort() {
        this.running = false;
        this.hashJoin.abort();
    }
}
