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
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.util.ListKeyGroupedIterator;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.GenericPairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.Collector;

import org.apache.commons.collections.ResettableIterator;
import org.apache.commons.collections.iterators.ListIteratorWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 基于外连接的函数
 * @param <IN1>
 * @param <IN2>
 * @param <OUT>
 * @param <FT>
 */
@Internal
public class OuterJoinOperatorBase<IN1, IN2, OUT, FT extends FlatJoinFunction<IN1, IN2, OUT>>
        extends JoinOperatorBase<IN1, IN2, OUT, FT> {

    public static enum OuterJoinType {
        LEFT,
        RIGHT,
        FULL
    }

    /**
     * 表示外连接的方式
     */
    private OuterJoinType outerJoinType;

    public OuterJoinOperatorBase(
            UserCodeWrapper<FT> udf,
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,
            int[] keyPositions1,
            int[] keyPositions2,
            String name,
            OuterJoinType outerJoinType) {
        super(udf, operatorInfo, keyPositions1, keyPositions2, name);
        this.outerJoinType = outerJoinType;
    }

    public OuterJoinOperatorBase(
            FT udf,
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,
            int[] keyPositions1,
            int[] keyPositions2,
            String name,
            OuterJoinType outerJoinType) {
        super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
        this.outerJoinType = outerJoinType;
    }

    public OuterJoinOperatorBase(
            Class<? extends FT> udf,
            BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo,
            int[] keyPositions1,
            int[] keyPositions2,
            String name,
            OuterJoinType outerJoinType) {
        super(new UserCodeClassWrapper<FT>(udf), operatorInfo, keyPositions1, keyPositions2, name);
        this.outerJoinType = outerJoinType;
    }

    public void setOuterJoinType(OuterJoinType outerJoinType) {
        this.outerJoinType = outerJoinType;
    }

    public OuterJoinType getOuterJoinType() {
        return outerJoinType;
    }


    /**
     * 使用外连接处理数据 并收集到output中
     * @param leftInput
     * @param rightInput
     * @param runtimeContext
     * @param executionConfig
     * @return
     * @throws Exception
     */
    @Override
    protected List<OUT> executeOnCollections(
            List<IN1> leftInput,
            List<IN2> rightInput,
            RuntimeContext runtimeContext,
            ExecutionConfig executionConfig)
            throws Exception {
        TypeInformation<IN1> leftInformation = getOperatorInfo().getFirstInputType();
        TypeInformation<IN2> rightInformation = getOperatorInfo().getSecondInputType();
        TypeInformation<OUT> outInformation = getOperatorInfo().getOutputType();

        // 生成2个比较器对象
        TypeComparator<IN1> leftComparator =
                buildComparatorFor(0, executionConfig, leftInformation);
        TypeComparator<IN2> rightComparator =
                buildComparatorFor(1, executionConfig, rightInformation);

        TypeSerializer<IN1> leftSerializer = leftInformation.createSerializer(executionConfig);
        TypeSerializer<IN2> rightSerializer = rightInformation.createSerializer(executionConfig);

        // 将相关信息拼接起来变成迭代器
        OuterJoinListIterator<IN1, IN2> outerJoinIterator =
                new OuterJoinListIterator<>(
                        leftInput,
                        leftSerializer,
                        leftComparator,
                        rightInput,
                        rightSerializer,
                        rightComparator,
                        outerJoinType);

        // --------------------------------------------------------------------
        // Run UDF    获取join函数   外连接相当于是选择哪些元素两两配对 而join函数则是包含处理逻辑
        // --------------------------------------------------------------------
        FlatJoinFunction<IN1, IN2, OUT> function = userFunction.getUserCodeObject();

        FunctionUtils.setFunctionRuntimeContext(function, runtimeContext);
        FunctionUtils.openFunction(function, this.parameters);

        List<OUT> result = new ArrayList<>();
        Collector<OUT> collector =
                new CopyingListCollector<>(
                        result, outInformation.createSerializer(executionConfig));

        // 这个迭代器的使用方式是  先next()  然后分别调用 left/right
        while (outerJoinIterator.next()) {
            IN1 left = outerJoinIterator.getLeft();
            IN2 right = outerJoinIterator.getRight();
            function.join(
                    left == null ? null : leftSerializer.copy(left),
                    right == null ? null : rightSerializer.copy(right),
                    collector);
        }

        FunctionUtils.closeFunction(function);

        return result;
    }


    /**
     * 生成比较器对象
     * @param input
     * @param executionConfig
     * @param typeInformation
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    private <T> TypeComparator<T> buildComparatorFor(
            int input, ExecutionConfig executionConfig, TypeInformation<T> typeInformation) {
        TypeComparator<T> comparator;
        if (typeInformation instanceof AtomicType) {
            comparator = ((AtomicType<T>) typeInformation).createComparator(true, executionConfig);
        } else if (typeInformation instanceof CompositeType) {
            int[] keyPositions = getKeyColumns(input);
            boolean[] orders = new boolean[keyPositions.length];
            Arrays.fill(orders, true);

            comparator =
                    ((CompositeType<T>) typeInformation)
                            .createComparator(keyPositions, orders, 0, executionConfig);
        } else {
            throw new RuntimeException(
                    "Type information for input of type "
                            + typeInformation.getClass().getCanonicalName()
                            + " is not supported. Could not generate a comparator.");
        }
        return comparator;
    }

    /**
     * 用于外连接的迭代器
     * @param <IN1>
     * @param <IN2>
     */
    private static class OuterJoinListIterator<IN1, IN2> {

        private static enum MatchStatus {
            NONE_REMAINED,
            FIRST_REMAINED,
            SECOND_REMAINED,
            FIRST_EMPTY,
            SECOND_EMPTY
        }

        private OuterJoinType outerJoinType;

        private ListKeyGroupedIterator<IN1> leftGroupedIterator;
        private ListKeyGroupedIterator<IN2> rightGroupedIterator;
        private Iterable<IN1> currLeftSubset;

        /**
         * 代表可重置的迭代器
         */
        private ResettableIterator currLeftIterator;
        private Iterable<IN2> currRightSubset;
        private ResettableIterator currRightIterator;

        private MatchStatus matchStatus;
        private GenericPairComparator<IN1, IN2> pairComparator;

        private IN1 leftReturn;
        private IN2 rightReturn;

        public OuterJoinListIterator(
                List<IN1> leftInput,
                TypeSerializer<IN1> leftSerializer,
                final TypeComparator<IN1> leftComparator,
                List<IN2> rightInput,
                TypeSerializer<IN2> rightSerializer,
                final TypeComparator<IN2> rightComparator,
                OuterJoinType outerJoinType) {
            this.outerJoinType = outerJoinType;

            // 用于比较2个不同类型对象抽出来的某几个字段是否相等
            pairComparator = new GenericPairComparator<>(leftComparator, rightComparator);

            // 将2个输入分别包装成迭代器
            leftGroupedIterator =
                    new ListKeyGroupedIterator<>(leftInput, leftSerializer, leftComparator);
            rightGroupedIterator =
                    new ListKeyGroupedIterator<>(rightInput, rightSerializer, rightComparator);
            // ----------------------------------------------------------------
            // Sort   对2个输入进行排序
            // ----------------------------------------------------------------

            Collections.sort(
                    leftInput,
                    new Comparator<IN1>() {
                        @Override
                        public int compare(IN1 o1, IN1 o2) {
                            return leftComparator.compare(o1, o2);
                        }
                    });

            Collections.sort(
                    rightInput,
                    new Comparator<IN2>() {
                        @Override
                        public int compare(IN2 o1, IN2 o2) {
                            return rightComparator.compare(o1, o2);
                        }
                    });
        }

        /**
         * 迭代器的使用方式是先调用该方法
         * @return
         * @throws IOException
         */
        @SuppressWarnings("unchecked")
        private boolean next() throws IOException {
            boolean hasMoreElements;

            // 左右两侧迭代器都要加载
            if ((currLeftIterator == null || !currLeftIterator.hasNext())
                    && (currRightIterator == null || !currRightIterator.hasNext())) {
                hasMoreElements = nextGroups(outerJoinType);
                if (hasMoreElements) {

                    // 推进并为 leftReturn/rightReturn 赋值
                    if (outerJoinType != OuterJoinType.LEFT) {
                        currLeftIterator = new ListIteratorWrapper(currLeftSubset.iterator());
                    }
                    leftReturn = (IN1) currLeftIterator.next();
                    if (outerJoinType != OuterJoinType.RIGHT) {
                        currRightIterator = new ListIteratorWrapper(currRightSubset.iterator());
                    }
                    rightReturn = (IN2) currRightIterator.next();
                    return true;
                } else {
                    // no more elements
                    return false;
                }
            } else if (currLeftIterator.hasNext() && !currRightIterator.hasNext()) {
                leftReturn = (IN1) currLeftIterator.next();
                currRightIterator.reset();
                rightReturn = (IN2) currRightIterator.next();
                return true;
            } else {
                rightReturn = (IN2) currRightIterator.next();
                return true;
            }
        }

        /**
         * 返回下一组数据
         * @param outerJoinType
         * @return
         * @throws IOException
         */
        private boolean nextGroups(OuterJoinType outerJoinType) throws IOException {
            if (outerJoinType == OuterJoinType.FULL) {
                // 此时补充了 左右子集
                return nextGroups();

                // 根据左右情况 走不同逻辑
            } else if (outerJoinType == OuterJoinType.LEFT) {
                boolean leftContainsElements = false;
                while (!leftContainsElements && nextGroups()) {
                    currLeftIterator = new ListIteratorWrapper(currLeftSubset.iterator());
                    if (currLeftIterator.next() != null) {
                        leftContainsElements = true;
                    }
                    currLeftIterator.reset();
                }
                return leftContainsElements;
            } else if (outerJoinType == OuterJoinType.RIGHT) {
                boolean rightContainsElements = false;
                while (!rightContainsElements && nextGroups()) {
                    currRightIterator = new ListIteratorWrapper(currRightSubset.iterator());
                    if (currRightIterator.next() != null) {
                        rightContainsElements = true;
                    }
                    currRightIterator.reset();
                }
                return rightContainsElements;
            } else {
                throw new IllegalArgumentException(
                        "Outer join of type '" + outerJoinType + "' not supported.");
            }
        }

        /**
         * 切换到下个组
         * @return
         * @throws IOException
         */
        private boolean nextGroups() throws IOException {
            boolean firstEmpty = true;
            boolean secondEmpty = true;

            if (this.matchStatus != MatchStatus.FIRST_EMPTY) {
                // 代表还有数据未读完 不需要更新 pairComparator 内的数据
                if (this.matchStatus == MatchStatus.FIRST_REMAINED) {
                    // comparator is still set correctly
                    firstEmpty = false;
                } else {
                    // 更新到下一个key
                    if (this.leftGroupedIterator.nextKey()) {
                        this.pairComparator.setReference(
                                leftGroupedIterator.getValues().getCurrent());
                        firstEmpty = false;
                    }
                }
            }

            // 判断是否需要更新第二个迭代器
            if (this.matchStatus != MatchStatus.SECOND_EMPTY) {
                if (this.matchStatus == MatchStatus.SECOND_REMAINED) {
                    secondEmpty = false;
                } else {
                    if (rightGroupedIterator.nextKey()) {
                        secondEmpty = false;
                    }
                }
            }

            // 已经没有数据可以处理了
            if (firstEmpty && secondEmpty) {
                // both inputs are empty
                return false;
            } else if (firstEmpty && !secondEmpty) {
                // input1 is empty, input2 not    外连接就是 允许有值的一侧与null join
                this.currLeftSubset = Collections.singleton(null);
                // 获取当前key 相关的一组value
                this.currRightSubset = this.rightGroupedIterator.getValues();
                this.matchStatus = MatchStatus.FIRST_EMPTY;
                return true;
            } else if (!firstEmpty && secondEmpty) {
                // input1 is not empty, input 2 is empty
                this.currLeftSubset = this.leftGroupedIterator.getValues();
                this.currRightSubset = Collections.singleton(null);
                this.matchStatus = MatchStatus.SECOND_EMPTY;
                return true;
            } else {
                // both inputs are not empty    二者皆不为空
                final int comp =
                        this.pairComparator.compareToReference(
                                rightGroupedIterator.getValues().getCurrent());

                // 左右相同 都会被使用 所以都不会剩余 对应NONE_REMAINED
                if (0 == comp) {
                    // keys match
                    this.currLeftSubset = this.leftGroupedIterator.getValues();
                    this.currRightSubset = this.rightGroupedIterator.getValues();
                    this.matchStatus = MatchStatus.NONE_REMAINED;
                } else if (0 < comp) {
                    // key1 goes first  先返回左边的 同时右边为null
                    this.currLeftSubset = this.leftGroupedIterator.getValues();
                    this.currRightSubset = Collections.singleton(null);
                    this.matchStatus = MatchStatus.SECOND_REMAINED;
                } else {
                    // key 2 goes first
                    this.currLeftSubset = Collections.singleton(null);
                    this.currRightSubset = this.rightGroupedIterator.getValues();
                    this.matchStatus = MatchStatus.FIRST_REMAINED;
                }
                return true;
            }
        }

        private IN1 getLeft() {
            return leftReturn;
        }

        private IN2 getRight() {
            return rightReturn;
        }
    }
}
