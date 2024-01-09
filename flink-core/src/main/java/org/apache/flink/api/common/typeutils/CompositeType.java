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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base type information class for Tuple and Pojo types
 *
 * <p>The class is taking care of serialization and comparators for Tuples as well.
 * 代表一个组合类型 打散开会有很多field
 */
@Public
public abstract class CompositeType<T> extends TypeInformation<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> typeClass;

    @PublicEvolving
    public CompositeType(Class<T> typeClass) {
        this.typeClass = checkNotNull(typeClass);
    }

    /**
     * Returns the type class of the composite type
     *
     * @return Type class of the composite type
     */
    @PublicEvolving
    public Class<T> getTypeClass() {
        return typeClass;
    }

    /**
     * Returns the flat field descriptors for the given field expression.
     *
     * @param fieldExpression The field expression for which the flat field descriptors are
     *     computed.
     * @return The list of descriptors for the flat fields which are specified by the field
     *     expression.
     *     根据表达式信息 读取相关的field
     */
    @PublicEvolving
    public List<FlatFieldDescriptor> getFlatFields(String fieldExpression) {
        List<FlatFieldDescriptor> result = new ArrayList<FlatFieldDescriptor>();
        this.getFlatFields(fieldExpression, 0, result);
        return result;
    }

    /**
     * Computes the flat field descriptors for the given field expression with the given offset.
     *
     * @param fieldExpression The field expression for which the FlatFieldDescriptors are computed.
     * @param offset The offset to use when computing the positions of the flat fields.
     * @param result The list into which all flat field descriptors are inserted.
     */
    @PublicEvolving
    public abstract void getFlatFields(
            String fieldExpression, int offset, List<FlatFieldDescriptor> result);

    /**
     * Returns the type of the (nested) field at the given field expression position. Wildcards are
     * not allowed.
     *
     * @param fieldExpression The field expression for which the field of which the type is
     *     returned.
     * @return The type of the field at the given field expression.
     * 获取对应字段的类型
     */
    @PublicEvolving
    public abstract <X> TypeInformation<X> getTypeAt(String fieldExpression);

    /**
     * Returns the type of the (unnested) field at the given field position.
     *
     * @param pos The position of the (unnested) field in this composite type.
     * @return The type of the field at the given position.
     */
    @PublicEvolving
    public abstract <X> TypeInformation<X> getTypeAt(int pos);

    @PublicEvolving
    protected abstract TypeComparatorBuilder<T> createTypeComparatorBuilder();

    /**
     * Generic implementation of the comparator creation. Composite types are supplying the
     * infrastructure to create the actual comparators
     *
     * @return The comparator
     *
     * 生成比较器对象   因为最终的比较结果受多个字段影响  需要将他们组合在一起
     */
    @PublicEvolving
    public TypeComparator<T> createComparator(
            int[] logicalKeyFields,  // 只有key字段才会参与到compare的计算中
            boolean[] orders,    // 这些列的顺序
            int logicalFieldOffset,
            ExecutionConfig config) {

        TypeComparatorBuilder<T> builder = createTypeComparatorBuilder();

        // 根据参与的字段数量 初始化
        builder.initializeTypeComparatorBuilder(logicalKeyFields.length);

        for (int logicalKeyFieldIndex = 0;
                logicalKeyFieldIndex < logicalKeyFields.length;
                logicalKeyFieldIndex++) {

            // key字段的下标
            int logicalKeyField = logicalKeyFields[logicalKeyFieldIndex];
            int logicalField = logicalFieldOffset; // this is the global/logical field number
            boolean comparatorAdded = false;


            // 每次遍历从0到logicalKeyField
            // logicalKeyField 代表每个排序相关的key的位置
            for (int localFieldId = 0;
                    localFieldId < this.getArity()
                            && logicalField <= logicalKeyField
                            && !comparatorAdded;  // 一旦添加了比较器 就可以进入下次循环了
                    localFieldId++) {

                // 获取该字段的类型信息
                TypeInformation<?> localFieldType = this.getTypeAt(localFieldId);

                // 代表找到了一个比较用的key  创建比较器  并且注意是到logicalKeyField了 也就是本次期望的那个会影响到排序的key 这是正常情况
                if (localFieldType instanceof AtomicType && logicalField == logicalKeyField) {
                    // we found an atomic key --> create comparator
                    builder.addComparatorField(
                            localFieldId,
                            ((AtomicType<?>) localFieldType)
                                    // orders[logicalKeyFieldIndex] 获取该字段的排序方式
                                    .createComparator(orders[logicalKeyFieldIndex], config));

                    // 代表成功添加了比较器  提前退出内循环
                    comparatorAdded = true;
                }
                // must be composite type and check that the logicalKeyField is within the bounds
                // of the composite type's logical fields
                // 在发现参与排序的字段前 可能会发现一些普通字段  并且是复合类型
                else if (localFieldType instanceof CompositeType
                        && logicalField <= logicalKeyField   // 代表不会超过本次期望的key
                        && logicalKeyField   // 代表此时该type 包含了logicalKeyField 也就是参与排序的字段在type内部 要把它单独拎出来
                                <= logicalField + (localFieldType.getTotalFields() - 1)) {
                    // we found a compositeType that is containing the logicalKeyField we are
                    // looking for --> create comparator
                    builder.addComparatorField(
                            localFieldId,
                            ((CompositeType<?>) localFieldType)  // 开始嵌套创建了
                                    .createComparator(
                                            new int[] {logicalKeyField},  // 扫描到目标位置
                                            new boolean[] {orders[logicalKeyFieldIndex]},
                                            logicalField, // 这个是偏移量 代表从这里开始扫描
                                            config));

                    comparatorAdded = true;
                }

                // 当发现复合类型时 跳跃到最后一个字段的位置
                if (localFieldType instanceof CompositeType) {
                    // we need to subtract 1 because we are not accounting for the local field (not
                    // accessible for the user)
                    logicalField += localFieldType.getTotalFields() - 1;
                }

                logicalField++;
            }

            // 一轮下来没有任何比较器加入  是错误情况
            if (!comparatorAdded) {
                throw new IllegalArgumentException(
                        "Could not add a comparator for the logical"
                                + "key field index "
                                + logicalKeyFieldIndex
                                + ".");
            }
        }

        return builder.createTypeComparator(config);
    }

    // --------------------------------------------------------------------------------------------

    @PublicEvolving
    protected interface TypeComparatorBuilder<T> {
        void initializeTypeComparatorBuilder(int size);

        void addComparatorField(int fieldId, TypeComparator<?> comparator);

        TypeComparator<T> createTypeComparator(ExecutionConfig config);
    }

    /**
     * 简单来说就是非组合类型
     */
    @PublicEvolving
    public static class FlatFieldDescriptor {
        /**
         * 该字段的位置
         */
        private int keyPosition;
        private TypeInformation<?> type;

        public FlatFieldDescriptor(int keyPosition, TypeInformation<?> type) {
            if (type instanceof CompositeType) {
                throw new IllegalArgumentException("A flattened field can not be a composite type");
            }
            this.keyPosition = keyPosition;
            this.type = type;
        }

        public int getPosition() {
            return keyPosition;
        }

        public TypeInformation<?> getType() {
            return type;
        }

        @Override
        public String toString() {
            return "FlatFieldDescriptor [position=" + keyPosition + " typeInfo=" + type + "]";
        }
    }

    /** Returns true when this type has a composite field with the given name. */
    @PublicEvolving
    public boolean hasField(String fieldName) {
        return getFieldIndex(fieldName) >= 0;
    }

    /**
     * 只要有一个字段 不是key类型 就返回false
     * @return
     */
    @Override
    @PublicEvolving
    public boolean isKeyType() {
        for (int i = 0; i < this.getArity(); i++) {
            if (!this.getTypeAt(i).isKeyType()) {
                return false;
            }
        }
        return true;
    }

    @Override
    @PublicEvolving
    public boolean isSortKeyType() {
        for (int i = 0; i < this.getArity(); i++) {
            if (!this.getTypeAt(i).isSortKeyType()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the names of the composite fields of this type. The order of the returned array must
     * be consistent with the internal field index ordering.
     * 获取这些type对应的fieldName
     */
    @PublicEvolving
    public abstract String[] getFieldNames();

    /**
     * True if this type has an inherent ordering of the fields, such that a user can always be sure
     * in which order the fields will be in. This is true for Tuples and Case Classes. It is not
     * true for Regular Java Objects, since there, the ordering of the fields can be arbitrary.
     *
     * <p>This is used when translating a DataSet or DataStream to an Expression Table, when
     * initially renaming the fields of the underlying type.
     */
    @PublicEvolving
    public boolean hasDeterministicFieldOrder() {
        return false;
    }

    /**
     * Returns the field index of the composite field of the given name.
     *
     * @return The field index or -1 if this type does not have a field of the given name.
     */
    @PublicEvolving
    public abstract int getFieldIndex(String fieldName);

    @PublicEvolving
    public static class InvalidFieldReferenceException extends IllegalArgumentException {

        private static final long serialVersionUID = 1L;

        public InvalidFieldReferenceException(String s) {
            super(s);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CompositeType) {
            @SuppressWarnings("unchecked")
            CompositeType<T> compositeType = (CompositeType<T>) obj;

            return compositeType.canEqual(this) && typeClass == compositeType.typeClass;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeClass);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof CompositeType;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "<" + typeClass.getSimpleName() + ">";
    }
}
