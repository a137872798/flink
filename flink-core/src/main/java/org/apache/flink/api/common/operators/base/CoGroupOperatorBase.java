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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.ListKeyGroupedIterator;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** @see org.apache.flink.api.common.functions.CoGroupFunction */
@Internal
public class CoGroupOperatorBase<IN1, IN2, OUT, FT extends CoGroupFunction<IN1, IN2, OUT>>
        extends DualInputOperator<IN1, IN2, OUT, FT> {

    // 2个分组排序对象

    /** The ordering for the order inside a group from input one.
     * */
    private Ordering groupOrder1;

    /** The ordering for the order inside a group from input two.
     * */
    private Ordering groupOrder2;

    /**
     * 分区对象    根据传入的key 和总分区数 可以计算出一个预期的分区值
     */
    private Partitioner<?> customPartitioner;

    private boolean combinableFirst;

    private boolean combinableSecond;

    // --------------------------------------------------------------------------------------------

    public CoGroupOperatorBase(
            UserCodeWrapper<FT> udf,   // 该operator使用的函数
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,  // 包含参数类型信息
            int[] keyPositions1,
            int[] keyPositions2,
            String name) {
        super(udf, operatorInfo, keyPositions1, keyPositions2, name);
        this.combinableFirst = false;
        this.combinableSecond = false;
    }

    public CoGroupOperatorBase(
            FT udf,
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,
            int[] keyPositions1,
            int[] keyPositions2,
            String name) {
        this(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
    }

    public CoGroupOperatorBase(
            Class<? extends FT> udf,
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,
            int[] keyPositions1,
            int[] keyPositions2,
            String name) {
        this(new UserCodeClassWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Sets the order of the elements within a group for the given input.
     *
     * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
     * @param order The order for the elements in a group.
     *              表示给第几个分区 设置排序方式
     */
    public void setGroupOrder(int inputNum, Ordering order) {
        if (inputNum == 0) {
            this.groupOrder1 = order;
        } else if (inputNum == 1) {
            this.groupOrder2 = order;
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * Sets the order of the elements within a group for the first input.
     *
     * @param order The order for the elements in a group.
     */
    public void setGroupOrderForInputOne(Ordering order) {
        setGroupOrder(0, order);
    }

    /**
     * Sets the order of the elements within a group for the second input.
     *
     * @param order The order for the elements in a group.
     */
    public void setGroupOrderForInputTwo(Ordering order) {
        setGroupOrder(1, order);
    }

    /**
     * Gets the value order for an input, i.e. the order of elements within a group. If no such
     * order has been set, this method returns null.
     *
     * @param inputNum The number of the input (here either <i>0</i> or <i>1</i>).
     * @return The group order.
     * 获取对应分区的排序方式
     */
    public Ordering getGroupOrder(int inputNum) {
        if (inputNum == 0) {
            return this.groupOrder1;
        } else if (inputNum == 1) {
            return this.groupOrder2;
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * Gets the order of elements within a group for the first input. If no such order has been set,
     * this method returns null.
     *
     * @return The group order for the first input.
     */
    public Ordering getGroupOrderForInputOne() {
        return getGroupOrder(0);
    }

    /**
     * Gets the order of elements within a group for the second input. If no such order has been
     * set, this method returns null.
     *
     * @return The group order for the second input.
     */
    public Ordering getGroupOrderForInputTwo() {
        return getGroupOrder(1);
    }

    // --------------------------------------------------------------------------------------------

    // 判断/设置  1，2输入流能否合并

    public boolean isCombinableFirst() {
        return this.combinableFirst;
    }

    public void setCombinableFirst(boolean combinableFirst) {
        this.combinableFirst = combinableFirst;
    }

    public boolean isCombinableSecond() {
        return this.combinableSecond;
    }

    public void setCombinableSecond(boolean combinableSecond) {
        this.combinableSecond = combinableSecond;
    }

    /**
     * 设置分区对象
     * @param customPartitioner
     */
    public void setCustomPartitioner(Partitioner<?> customPartitioner) {
        this.customPartitioner = customPartitioner;
    }

    public Partitioner<?> getCustomPartitioner() {
        return customPartitioner;
    }

    // ------------------------------------------------------------------------

    /**
     * 接收2个输入 并产生一个输出
     * @param input1
     * @param input2
     * @param ctx
     * @param executionConfig
     * @return
     * @throws Exception
     */
    @Override
    protected List<OUT> executeOnCollections(
            List<IN1> input1, List<IN2> input2, RuntimeContext ctx, ExecutionConfig executionConfig)
            throws Exception {
        // --------------------------------------------------------------------
        // Setup
        // --------------------------------------------------------------------
        TypeInformation<IN1> inputType1 = getOperatorInfo().getFirstInputType();
        TypeInformation<IN2> inputType2 = getOperatorInfo().getSecondInputType();

        // for the grouping / merging comparator
        int[] inputKeys1 = getKeyColumns(0);
        int[] inputKeys2 = getKeyColumns(1);

        boolean[] inputDirections1 = new boolean[inputKeys1.length];
        boolean[] inputDirections2 = new boolean[inputKeys2.length];
        // 这个代表按照这些keys排序的顺序
        Arrays.fill(inputDirections1, true);
        Arrays.fill(inputDirections2, true);

        // 根据输入类型创建对应的序列化对象
        final TypeSerializer<IN1> inputSerializer1 = inputType1.createSerializer(executionConfig);
        final TypeSerializer<IN2> inputSerializer2 = inputType2.createSerializer(executionConfig);

        // 生成排序对象
        final TypeComparator<IN1> inputComparator1 =
                getTypeComparator(executionConfig, inputType1, inputKeys1, inputDirections1);
        final TypeComparator<IN2> inputComparator2 =
                getTypeComparator(executionConfig, inputType2, inputKeys2, inputDirections2);

        final TypeComparator<IN1> inputSortComparator1;
        final TypeComparator<IN2> inputSortComparator2;

        // 不需要分组排序
        if (groupOrder1 == null || groupOrder1.getNumberOfFields() == 0) {
            // no group sorting
            inputSortComparator1 = inputComparator1;
        } else {
            // group sorting   尝试生成分组排序对象
            int[] groupSortKeys = groupOrder1.getFieldPositions();

            // 这是所有参与排序的字段
            int[] allSortKeys = new int[inputKeys1.length + groupOrder1.getNumberOfFields()];
            System.arraycopy(inputKeys1, 0, allSortKeys, 0, inputKeys1.length);
            System.arraycopy(
                    groupSortKeys, 0, allSortKeys, inputKeys1.length, groupSortKeys.length);

            // 获得分组排序的方向
            boolean[] groupSortDirections = groupOrder1.getFieldSortDirections();
            // 全正序
            boolean[] allSortDirections = new boolean[inputKeys1.length + groupSortKeys.length];
            Arrays.fill(allSortDirections, 0, inputKeys1.length, true);

            // 设置分组排序的方向
            System.arraycopy(
                    groupSortDirections,
                    0,
                    allSortDirections,
                    inputKeys1.length,
                    groupSortDirections.length);

            // 加入分组信息后 产生一个新的 comparator对象
            inputSortComparator1 =
                    getTypeComparator(executionConfig, inputType1, allSortKeys, allSortDirections);
        }

        if (groupOrder2 == null || groupOrder2.getNumberOfFields() == 0) {
            // no group sorting
            inputSortComparator2 = inputComparator2;
        } else {
            // group sorting
            int[] groupSortKeys = groupOrder2.getFieldPositions();
            int[] allSortKeys = new int[inputKeys2.length + groupOrder2.getNumberOfFields()];
            System.arraycopy(inputKeys2, 0, allSortKeys, 0, inputKeys2.length);
            System.arraycopy(
                    groupSortKeys, 0, allSortKeys, inputKeys2.length, groupSortKeys.length);

            boolean[] groupSortDirections = groupOrder2.getFieldSortDirections();
            boolean[] allSortDirections = new boolean[inputKeys2.length + groupSortKeys.length];
            Arrays.fill(allSortDirections, 0, inputKeys2.length, true);
            System.arraycopy(
                    groupSortDirections,
                    0,
                    allSortDirections,
                    inputKeys2.length,
                    groupSortDirections.length);

            inputSortComparator2 =
                    getTypeComparator(executionConfig, inputType2, allSortKeys, allSortDirections);
        }

        // 把相关信息整合起来 变成一个迭代器
        CoGroupSortListIterator<IN1, IN2> coGroupIterator =
                new CoGroupSortListIterator<IN1, IN2>(
                        input1,
                        inputSortComparator1,
                        inputComparator1,
                        inputSerializer1,
                        input2,
                        inputSortComparator2,
                        inputComparator2,
                        inputSerializer2);

        // --------------------------------------------------------------------
        // Run UDF
        // --------------------------------------------------------------------
        CoGroupFunction<IN1, IN2, OUT> function = userFunction.getUserCodeObject();

        FunctionUtils.setFunctionRuntimeContext(function, ctx);
        // 在执行函数前 允许一些初始化操作
        FunctionUtils.openFunction(function, parameters);

        List<OUT> result = new ArrayList<OUT>();

        // 该对象用于存储结果
        Collector<OUT> resultCollector =
                new CopyingListCollector<OUT>(
                        result,
                        getOperatorInfo().getOutputType().createSerializer(executionConfig));

        while (coGroupIterator.next()) {
            // 通过迭代器返回元素 并将函数生成的结果存储到 resultCollector中
            function.coGroup(
                    coGroupIterator.getValues1(), coGroupIterator.getValues2(), resultCollector);
        }

        FunctionUtils.closeFunction(function);

        return result;
    }

    @SuppressWarnings("unchecked")
    private <T> TypeComparator<T> getTypeComparator(
            ExecutionConfig executionConfig,
            TypeInformation<T> inputType,
            int[] inputKeys,
            boolean[] inputSortDirections) {
        if (inputType instanceof CompositeType) {
            return ((CompositeType<T>) inputType)
                    .createComparator(inputKeys, inputSortDirections, 0, executionConfig);
        } else if (inputType instanceof AtomicType) {
            return ((AtomicType<T>) inputType)
                    .createComparator(inputSortDirections[0], executionConfig);
        }

        throw new InvalidProgramException(
                "Input type of coGroup must be one of composite types or atomic types.");
    }

    /**
     * 两两元素 分组排序
     * @param <IN1>
     * @param <IN2>
     */
    private static class CoGroupSortListIterator<IN1, IN2> {

        private static enum MatchStatus {
            NONE_REMAINED,  // 2个input都还有剩余
            FIRST_REMAINED,   // 第一个还有剩余
            SECOND_REMAINED,
            FIRST_EMPTY,
            SECOND_EMPTY
        }

        private final ListKeyGroupedIterator<IN1> iterator1;

        private final ListKeyGroupedIterator<IN2> iterator2;

        private final TypePairComparator<IN1, IN2> pairComparator;

        private MatchStatus matchStatus;

        private Iterable<IN1> firstReturn;

        private Iterable<IN2> secondReturn;

        private CoGroupSortListIterator(
                List<IN1> input1,
                final TypeComparator<IN1> inputSortComparator1,
                TypeComparator<IN1> inputComparator1,
                TypeSerializer<IN1> serializer1,
                List<IN2> input2,
                final TypeComparator<IN2> inputSortComparator2,
                TypeComparator<IN2> inputComparator2,
                TypeSerializer<IN2> serializer2) {
            // 合成一个比较器
            this.pairComparator =
                    new GenericPairComparator<IN1, IN2>(inputComparator1, inputComparator2);

            // 将2个输入分别变成 迭代器对象  这种迭代器每个key 关联一组value
            this.iterator1 = new ListKeyGroupedIterator<IN1>(input1, serializer1, inputComparator1);
            this.iterator2 = new ListKeyGroupedIterator<IN2>(input2, serializer2, inputComparator2);

            // ----------------------------------------------------------------
            // Sort
            // ----------------------------------------------------------------

            // 将2个输入 按照comparator进行排序
            Collections.sort(
                    input1,
                    new Comparator<IN1>() {
                        @Override
                        public int compare(IN1 o1, IN1 o2) {
                            return inputSortComparator1.compare(o1, o2);
                        }
                    });

            Collections.sort(
                    input2,
                    new Comparator<IN2>() {
                        @Override
                        public int compare(IN2 o1, IN2 o2) {
                            return inputSortComparator2.compare(o1, o2);
                        }
                    });
        }

        /**
         * 比较2个input 返回下个对象
         * @return
         * @throws IOException
         */
        private boolean next() throws IOException {
            boolean firstEmpty = true;
            boolean secondEmpty = true;

            if (this.matchStatus != MatchStatus.FIRST_EMPTY) {
                // 都有 或者只有第一个有
                if (this.matchStatus == MatchStatus.FIRST_REMAINED) {
                    // comparator is still set correctly
                    firstEmpty = false;
                } else {
                    // 切换key
                    if (this.iterator1.nextKey()) {
                        // 取出value中的字段
                        this.pairComparator.setReference(iterator1.getValues().getCurrent());
                        firstEmpty = false;
                    }
                }
            }

            if (this.matchStatus != MatchStatus.SECOND_EMPTY) {
                if (this.matchStatus == MatchStatus.SECOND_REMAINED) {
                    secondEmpty = false;
                } else {
                    if (iterator2.nextKey()) {
                        secondEmpty = false;
                    }
                }
            }

            // 已经没有数据了
            if (firstEmpty && secondEmpty) {
                // both inputs are empty
                return false;
            } else if (firstEmpty && !secondEmpty) {
                // input1 is empty, input2 not
                this.firstReturn = Collections.emptySet();
                // 此时key 对应的一组value
                this.secondReturn = this.iterator2.getValues();
                this.matchStatus = MatchStatus.FIRST_EMPTY;
                return true;
            } else if (!firstEmpty && secondEmpty) {
                // input1 is not empty, input 2 is empty     一组有值 另一组返回空
                this.firstReturn = this.iterator1.getValues();
                this.secondReturn = Collections.emptySet();
                this.matchStatus = MatchStatus.SECOND_EMPTY;
                return true;
            } else {
                // both inputs are not empty   都不为空 要通过比较
                final int comp =
                        this.pairComparator.compareToReference(iterator2.getValues().getCurrent());

                // 二者相等
                if (0 == comp) {
                    // keys match
                    this.firstReturn = this.iterator1.getValues();
                    this.secondReturn = this.iterator2.getValues();
                    this.matchStatus = MatchStatus.NONE_REMAINED;
                } else if (0 < comp) {
                    // key1 goes first  先返回1
                    this.firstReturn = this.iterator1.getValues();
                    this.secondReturn = Collections.emptySet();
                    this.matchStatus = MatchStatus.SECOND_REMAINED;
                } else {
                    // key 2 goes first  先返回2
                    this.firstReturn = Collections.emptySet();
                    this.secondReturn = this.iterator2.getValues();
                    this.matchStatus = MatchStatus.FIRST_REMAINED;
                }
                return true;
            }
        }

        private Iterable<IN1> getValues1() {
            return firstReturn;
        }

        private Iterable<IN2> getValues2() {
            return secondReturn;
        }
    }
}
