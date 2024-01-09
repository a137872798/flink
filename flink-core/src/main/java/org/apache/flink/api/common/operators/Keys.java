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

package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 描述一组关键字段    某个对象可能会有一组关键字段
 * @param <T>
 */
@Internal
public abstract class Keys<T> {

    /**
     * 获取关键字段数量
     * @return
     */
    public abstract int getNumberOfKeyFields();

    /**
     * 这些字段的下标
     * @return
     */
    public abstract int[] computeLogicalKeyPositions();

    /**
     * 这些字段类型
     * @return
     */
    public abstract TypeInformation<?>[] getKeyFieldTypes();

    /**
     * 原始字段类型
     * @return
     */
    public abstract TypeInformation<?>[] getOriginalKeyFieldTypes();

    /**
     * 校验分区器 有效性
     * @param partitioner
     * @param typeInfo
     * @param <E>
     */
    public abstract <E> void validateCustomPartitioner(
            Partitioner<E> partitioner, TypeInformation<E> typeInfo);

    /**
     * 表示没有关键字段
     * @return
     */
    public boolean isEmpty() {
        return getNumberOfKeyFields() == 0;
    }

    /** Check if two sets of keys are compatible to each other (matching types, key counts)
     * 判断2个 Keys 是否兼容
     * */
    public boolean areCompatible(Keys<?> other) throws IncompatibleKeysException {

        TypeInformation<?>[] thisKeyFieldTypes = this.getKeyFieldTypes();
        TypeInformation<?>[] otherKeyFieldTypes = other.getKeyFieldTypes();

        // 首先长度要一致
        if (thisKeyFieldTypes.length != otherKeyFieldTypes.length) {
            throw new IncompatibleKeysException(IncompatibleKeysException.SIZE_MISMATCH_MESSAGE);
        } else {
            for (int i = 0; i < thisKeyFieldTypes.length; i++) {
                // type.equals 必须相同
                if (!thisKeyFieldTypes[i].equals(otherKeyFieldTypes[i])) {
                    throw new IncompatibleKeysException(
                            thisKeyFieldTypes[i], otherKeyFieldTypes[i]);
                }
            }
        }
        return true;
    }

    // --------------------------------------------------------------------------------------------
    //  Specializations for expression-based / extractor-based grouping
    // --------------------------------------------------------------------------------------------

    public static class SelectorFunctionKeys<T, K> extends Keys<T> {

        /**
         * 该函数仅能抽取出一个key
         */
        private final KeySelector<T, K> keyExtractor;
        // 表示输入的元素类型 以及抽取出来的key类型
        private final TypeInformation<T> inputType;
        private final TypeInformation<K> keyType;
        /**
         * 描述一组字段 以及字段的偏移量
         */
        private final List<FlatFieldDescriptor> keyFields;

        /**
         * 表示这些key的原始类型
         */
        private final TypeInformation<?>[] originalKeyTypes;

        /**
         * 初始化基于选择函数的 keys对象
         * @param keyExtractor
         * @param inputType
         * @param keyType
         */
        public SelectorFunctionKeys(
                KeySelector<T, K> keyExtractor,
                TypeInformation<T> inputType,
                TypeInformation<K> keyType) {

            if (keyExtractor == null) {
                throw new NullPointerException("Key extractor must not be null.");
            }
            if (keyType == null) {
                throw new NullPointerException("Key type must not be null.");
            }
            if (!keyType.isKeyType()) {
                throw new InvalidProgramException(
                        "Return type "
                                + keyType
                                + " of KeySelector "
                                + keyExtractor.getClass()
                                + " is not a valid key type");
            }

            this.keyExtractor = keyExtractor;
            this.inputType = inputType;
            this.keyType = keyType;

            this.originalKeyTypes = new TypeInformation[] {keyType};

            // 如果抽取出来的key是一个组合类型 打散得到多个field
            if (keyType instanceof CompositeType) {
                this.keyFields =
                        ((CompositeType<T>) keyType).getFlatFields(ExpressionKeys.SELECT_ALL_CHAR);
            } else {
                // 代表key是单字段的
                this.keyFields = new ArrayList<>(1);
                this.keyFields.add(new FlatFieldDescriptor(0, keyType));
            }
        }

        public TypeInformation<K> getKeyType() {
            return keyType;
        }

        public TypeInformation<T> getInputType() {
            return inputType;
        }

        public KeySelector<T, K> getKeyExtractor() {
            return keyExtractor;
        }

        @Override
        public int getNumberOfKeyFields() {
            return keyFields.size();
        }

        /**
         * 返回key fields的position
         * @return
         */
        @Override
        public int[] computeLogicalKeyPositions() {
            int[] logicalKeys = new int[keyFields.size()];
            for (int i = 0; i < keyFields.size(); i++) {
                logicalKeys[i] = keyFields.get(i).getPosition();
            }
            return logicalKeys;
        }

        /**
         * 获取 key 内部的各个field的类型
         * @return
         */
        @Override
        public TypeInformation<?>[] getKeyFieldTypes() {
            TypeInformation<?>[] fieldTypes = new TypeInformation[keyFields.size()];
            for (int i = 0; i < keyFields.size(); i++) {
                fieldTypes[i] = keyFields.get(i).getType();
            }
            return fieldTypes;
        }

        @Override
        public TypeInformation<?>[] getOriginalKeyFieldTypes() {
            return originalKeyTypes;
        }

        /**
         * 校验分区对象是否合法
         * @param partitioner
         * @param typeInfo
         * @param <E>
         */
        @Override
        public <E> void validateCustomPartitioner(
                Partitioner<E> partitioner, TypeInformation<E> typeInfo) {

            // 当使用自定义分区时  key只能有一个field  又是这种奇怪的限制
            if (keyFields.size() != 1) {
                throw new InvalidProgramException(
                        "Custom partitioners can only be used with keys that have one key field.");
            }

            // 未指定key类型时  从分区器推断
            if (typeInfo == null) {
                // try to extract key type from partitioner
                try {
                    typeInfo = TypeExtractor.getPartitionerTypes(partitioner);
                } catch (Throwable t) {
                    // best effort check, so we ignore exceptions
                }
            }

            // only check if type is known and not a generic type
            // 要求key类型与入参类型一致
            if (typeInfo != null && !(typeInfo instanceof GenericTypeInfo)) {
                // check equality of key and partitioner type
                if (!keyType.equals(typeInfo)) {
                    throw new InvalidProgramException(
                            "The partitioner is incompatible with the key type. "
                                    + "Partitioner type: "
                                    + typeInfo
                                    + " , key type: "
                                    + keyType);
                }
            }
        }

        @Override
        public String toString() {
            return "Key function (Type: " + keyType + ")";
        }
    }

    /** Represents (nested) field access through string and integer-based keys */
    public static class ExpressionKeys<T> extends Keys<T> {

        /**
         * 应该是表示返回所有的field
         */
        public static final String SELECT_ALL_CHAR = "*";
        public static final String SELECT_ALL_CHAR_SCALA = "_";
        private static final Pattern WILD_CARD_REGEX =
                Pattern.compile(
                        "[\\.]?("
                                + "\\"
                                + SELECT_ALL_CHAR
                                + "|"
                                + "\\"
                                + SELECT_ALL_CHAR_SCALA
                                + ")$");

        // Flattened fields representing keys fields   组成该key的一组field
        private List<FlatFieldDescriptor> keyFields;
        private TypeInformation<?>[] originalKeyTypes;

        /** ExpressionKeys that is defined by the full data type. */
        public ExpressionKeys(TypeInformation<T> type) {
            this(SELECT_ALL_CHAR, type);
        }

        /** Create int-based (non-nested) field position keys on a tuple type. */
        public ExpressionKeys(int keyPosition, TypeInformation<T> type) {
            this(new int[] {keyPosition}, type, false);
        }

        /** Create int-based (non-nested) field position keys on a tuple type. */
        public ExpressionKeys(int[] keyPositions, TypeInformation<T> type) {
            this(keyPositions, type, false);
        }

        /** Create int-based (non-nested) field position keys on a tuple type.
         * @param allowEmpty  是否允许空
         * 通过 position数组初始化
         * */
        public ExpressionKeys(int[] keyPositions, TypeInformation<T> type, boolean allowEmpty) {

            // 要求type必须是元组类型
            if (!type.isTupleType() || !(type instanceof CompositeType)) {
                throw new InvalidProgramException(
                        "Specifying keys via field positions is only valid "
                                + "for tuple data types. Type: "
                                + type);
            }

            // 身为元组类型 嵌套数量必然大于0
            if (type.getArity() == 0) {
                throw new InvalidProgramException(
                        "Tuple size must be greater than 0. Size: " + type.getArity());
            }

            // 不允许为空的情况下 keyPositions一定要有值
            if (!allowEmpty && (keyPositions == null || keyPositions.length == 0)) {
                throw new IllegalArgumentException("The grouping fields must not be empty.");
            }

            // 用于存储元组类型中用到的各种类型
            this.keyFields = new ArrayList<>();

            if (keyPositions == null || keyPositions.length == 0) {
                // use all tuple fields as key fields
                // 生成一个 从0开始单调递增的 int数组
                keyPositions = createIncrIntArray(type.getArity());
            } else {
                // 检查这些position 都在 arity之内
                rangeCheckFields(keyPositions, type.getArity() - 1);
            }

            checkArgument(
                    keyPositions.length > 0, "Grouping fields can not be empty at this point");

            // extract key field types
            CompositeType<T> cType = (CompositeType<T>) type;

            // 获取所有field
            this.keyFields = new ArrayList<>(type.getTotalFields());

            // for each key position, find all (nested) field types
            // 获取这些field的名字
            String[] fieldNames = cType.getFieldNames();
            this.originalKeyTypes = new TypeInformation<?>[keyPositions.length];
            ArrayList<FlatFieldDescriptor> tmpList = new ArrayList<>();
            for (int i = 0; i < keyPositions.length; i++) {
                int keyPos = keyPositions[i];
                tmpList.clear();
                // get all flat fields   取出对应field的type 并设置到 originalKeyTypes中
                this.originalKeyTypes[i] = cType.getTypeAt(keyPos);

                // 将展开的field 读取到 tmpList中
                cType.getFlatFields(fieldNames[keyPos], 0, tmpList);
                // check if fields are of key type
                for (FlatFieldDescriptor ffd : tmpList) {
                    // 要求这些field 都可以作为key
                    if (!ffd.getType().isKeyType()) {
                        throw new InvalidProgramException(
                                "This type (" + ffd.getType() + ") cannot be used as key.");
                    }
                }
                this.keyFields.addAll(tmpList);
            }
        }

        /** Create String-based (nested) field expression keys on a composite type. */
        public ExpressionKeys(String keyExpression, TypeInformation<T> type) {
            this(new String[] {keyExpression}, type);
        }

        /** Create String-based (nested) field expression keys on a composite type.
         * 上面通过 int数组初始化 这里通过string数组初始化
         * */
        public ExpressionKeys(String[] keyExpressions, TypeInformation<T> type) {
            checkNotNull(keyExpressions, "Field expression cannot be null.");

            this.keyFields = new ArrayList<>(keyExpressions.length);

            // 当type是复合类型时
            if (type instanceof CompositeType) {
                CompositeType<T> cType = (CompositeType<T>) type;
                this.originalKeyTypes = new TypeInformation<?>[keyExpressions.length];

                // extract the keys on their flat position
                for (int i = 0; i < keyExpressions.length; i++) {
                    // 取出表达式
                    String keyExpr = keyExpressions[i];

                    if (keyExpr == null) {
                        throw new InvalidProgramException("Expression key may not be null.");
                    }
                    // strip off whitespace
                    keyExpr = keyExpr.trim();

                    // 通过表达式 得到一组field
                    List<FlatFieldDescriptor> flatFields = cType.getFlatFields(keyExpr);

                    // 必须传入有效的表达式
                    if (flatFields.size() == 0) {
                        throw new InvalidProgramException(
                                "Unable to extract key from expression '"
                                        + keyExpr
                                        + "' on key "
                                        + cType);
                    }
                    // check if all nested fields can be used as keys
                    // 要求field 都可以作为key
                    for (FlatFieldDescriptor field : flatFields) {
                        if (!field.getType().isKeyType()) {
                            throw new InvalidProgramException(
                                    "This type (" + field.getType() + ") cannot be used as key.");
                        }
                    }
                    // add flat fields to key fields     这里是存储所有field
                    keyFields.addAll(flatFields);

                    // 这里是存储原始key类型
                    String strippedKeyExpr = WILD_CARD_REGEX.matcher(keyExpr).replaceAll("");
                    if (strippedKeyExpr.isEmpty()) {
                        this.originalKeyTypes[i] = type;
                    } else {
                        this.originalKeyTypes[i] = cType.getTypeAt(strippedKeyExpr);
                    }
                }
            } else {
                // 普通类型 要求必须可以作为key
                if (!type.isKeyType()) {
                    throw new InvalidProgramException(
                            "This type (" + type + ") cannot be used as key.");
                }

                // check that all key expressions are valid
                for (String keyExpr : keyExpressions) {
                    if (keyExpr == null) {
                        throw new InvalidProgramException("Expression key may not be null.");
                    }
                    // strip off whitespace
                    keyExpr = keyExpr.trim();
                    // check that full type is addressed
                    if (!(SELECT_ALL_CHAR.equals(keyExpr)
                            || SELECT_ALL_CHAR_SCALA.equals(keyExpr))) {
                        throw new InvalidProgramException(
                                "Field expression must be equal to '"
                                        + SELECT_ALL_CHAR
                                        + "' or '"
                                        + SELECT_ALL_CHAR_SCALA
                                        + "' for non-composite types.");
                    }
                    // add full type as key
                    keyFields.add(new FlatFieldDescriptor(0, type));
                }
                this.originalKeyTypes = new TypeInformation[] {type};
            }
        }

        /**
         * 获取 keyField数量
         * @return
         */
        @Override
        public int getNumberOfKeyFields() {
            if (keyFields == null) {
                return 0;
            }
            return keyFields.size();
        }

        /**
         * 返回key的 position信息
         * @return
         */
        @Override
        public int[] computeLogicalKeyPositions() {
            int[] logicalKeys = new int[keyFields.size()];
            for (int i = 0; i < keyFields.size(); i++) {
                logicalKeys[i] = keyFields.get(i).getPosition();
            }
            return logicalKeys;
        }

        /**
         * 返回每个keyField的类型
         * @return
         */
        @Override
        public TypeInformation<?>[] getKeyFieldTypes() {
            TypeInformation<?>[] fieldTypes = new TypeInformation[keyFields.size()];
            for (int i = 0; i < keyFields.size(); i++) {
                fieldTypes[i] = keyFields.get(i).getType();
            }
            return fieldTypes;
        }

        @Override
        public TypeInformation<?>[] getOriginalKeyFieldTypes() {
            return originalKeyTypes;
        }


        /**
         * 验证自定义分区器
         * @param partitioner
         * @param typeInfo
         * @param <E>
         */
        @Override
        public <E> void validateCustomPartitioner(
                Partitioner<E> partitioner, TypeInformation<E> typeInfo) {

            // 每当使用自定义分区器时 keyField 必须为1
            if (keyFields.size() != 1) {
                throw new InvalidProgramException(
                        "Custom partitioners can only be used with keys that have one key field.");
            }

            if (typeInfo == null) {
                // try to extract key type from partitioner
                try {
                    typeInfo = TypeExtractor.getPartitionerTypes(partitioner);
                } catch (Throwable t) {
                    // best effort check, so we ignore exceptions
                }
            }

            // 要求类型必须一致
            if (typeInfo != null && !(typeInfo instanceof GenericTypeInfo)) {
                // only check type compatibility if type is known and not a generic type

                TypeInformation<?> keyType = keyFields.get(0).getType();
                if (!keyType.equals(typeInfo)) {
                    throw new InvalidProgramException(
                            "The partitioner is incompatible with the key type. "
                                    + "Partitioner type: "
                                    + typeInfo
                                    + " , key type: "
                                    + keyType);
                }
            }
        }

        @Override
        public String toString() {
            return "ExpressionKeys: " + StringUtils.join(keyFields, '.');
        }

        /**
         * 判断是否是排序键
         * @param fieldPos
         * @param type
         * @return
         */
        public static boolean isSortKey(int fieldPos, TypeInformation<?> type) {

            // 要求必须是复合类型
            if (!type.isTupleType() || !(type instanceof CompositeType)) {
                throw new InvalidProgramException(
                        "Specifying keys via field positions is only valid "
                                + "for tuple data types. Type: "
                                + type);
            }
            // 要求 arity > 0  并且 不能超过pos
            if (type.getArity() == 0) {
                throw new InvalidProgramException(
                        "Tuple size must be greater than 0. Size: " + type.getArity());
            }

            if (fieldPos < 0 || fieldPos >= type.getArity()) {
                throw new IndexOutOfBoundsException("Tuple position is out of range: " + fieldPos);
            }

            // 返回判断是否是排序键
            TypeInformation<?> sortKeyType = ((CompositeType<?>) type).getTypeAt(fieldPos);
            return sortKeyType.isSortKeyType();
        }

        /**
         * 基于字符串表达式
         * @param fieldExpr
         * @param type
         * @return
         */
        public static boolean isSortKey(String fieldExpr, TypeInformation<?> type) {

            TypeInformation<?> sortKeyType;

            fieldExpr = fieldExpr.trim();
            // 如果是普通类型 直接使用普通类型
            if (SELECT_ALL_CHAR.equals(fieldExpr) || SELECT_ALL_CHAR_SCALA.equals(fieldExpr)) {
                sortKeyType = type;
            } else {
                // 如果是复合类型 通过表达式抽取某个字段
                if (type instanceof CompositeType) {
                    sortKeyType = ((CompositeType<?>) type).getTypeAt(fieldExpr);
                } else {
                    throw new InvalidProgramException(
                            "Field expression must be equal to '"
                                    + SELECT_ALL_CHAR
                                    + "' or '"
                                    + SELECT_ALL_CHAR_SCALA
                                    + "' for atomic types.");
                }
            }

            return sortKeyType.isSortKeyType();
        }
    }

    // --------------------------------------------------------------------------------------------

    // --------------------------------------------------------------------------------------------
    //  Utilities
    // --------------------------------------------------------------------------------------------

    private static int[] createIncrIntArray(int numKeys) {
        int[] keyFields = new int[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keyFields[i] = i;
        }
        return keyFields;
    }

    @VisibleForTesting
    static void rangeCheckFields(int[] fields, int maxAllowedField) {

        for (int f : fields) {
            if (f < 0 || f > maxAllowedField) {
                throw new IndexOutOfBoundsException("Tuple position is out of range: " + f);
            }
        }
    }

    public static class IncompatibleKeysException extends Exception {
        private static final long serialVersionUID = 1L;
        public static final String SIZE_MISMATCH_MESSAGE =
                "The number of specified keys is different.";

        public IncompatibleKeysException(String message) {
            super(message);
        }

        public IncompatibleKeysException(
                TypeInformation<?> typeInformation, TypeInformation<?> typeInformation2) {
            super(typeInformation + " and " + typeInformation2 + " are not compatible");
        }
    }
}
