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
import org.apache.flink.api.common.operators.util.FieldSet;

import java.util.HashMap;
import java.util.Map;

/** Container for the semantic properties associated to a single input operator.
 * 针对单输入operator的语义属性
 * */
@Internal
public class SingleInputSemanticProperties implements SemanticProperties {
    private static final long serialVersionUID = 1L;

    /** Mapping from fields in the source record(s) to fields in the destination record(s). */
    private Map<Integer, FieldSet> fieldMapping;

    /** Set of fields that are read in the source record(s). */
    private FieldSet readFields;

    public SingleInputSemanticProperties() {
        this.fieldMapping = new HashMap<Integer, FieldSet>();
        this.readFields = null;
    }

    // 下面这2个方法的 input参数 好像代表第几个input   然后因为是 singleInput 所以只能为0

    @Override
    public FieldSet getForwardingTargetFields(int input, int sourceField) {
        if (input != 0) {
            throw new IndexOutOfBoundsException();
        }

        // 传入 sourceField的id  会得到一组field
        return this.fieldMapping.containsKey(sourceField)
                ? this.fieldMapping.get(sourceField)
                : FieldSet.EMPTY_SET;
    }

    @Override
    public int getForwardingSourceField(int input, int targetField) {
        if (input != 0) {
            throw new IndexOutOfBoundsException();
        }

        // 这个是反向查询
        for (Map.Entry<Integer, FieldSet> e : fieldMapping.entrySet()) {
            if (e.getValue().contains(targetField)) {
                return e.getKey();
            }
        }
        return -1;
    }

    @Override
    public FieldSet getReadFields(int input) {
        if (input != 0) {
            throw new IndexOutOfBoundsException();
        }

        return this.readFields;
    }

    /**
     * Adds, to the existing information, a field that is forwarded directly from the source
     * record(s) to the destination record(s).
     *
     * @param sourceField the position in the source record(s)
     * @param targetField the position in the destination record(s)
     *                    添加正向的 field
     */
    public void addForwardedField(int sourceField, int targetField) {
        // 表示 targetField 已经加过了  拒绝重复添加
        if (isTargetFieldPresent(targetField)) {
            throw new InvalidSemanticAnnotationException(
                    "Target field " + targetField + " was added twice.");
        }

        FieldSet targetFields = fieldMapping.get(sourceField);
        // 将targetField 追加到FieldSet中
        if (targetFields != null) {
            fieldMapping.put(sourceField, targetFields.addField(targetField));
        } else {
            fieldMapping.put(sourceField, new FieldSet(targetField));
        }
    }

    /**
     * 检查 targetField 是否已经存在
     * @param targetField
     * @return
     */
    private boolean isTargetFieldPresent(int targetField) {
        for (FieldSet targetFields : fieldMapping.values()) {
            if (targetFields.contains(targetField)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds, to the existing information, field(s) that are read in the source record(s).
     *
     * @param readFields the position(s) in the source record(s)
     */
    public void addReadFields(FieldSet readFields) {
        if (this.readFields == null) {
            this.readFields = readFields;
        } else {
            this.readFields = this.readFields.addFields(readFields);
        }
    }

    @Override
    public String toString() {
        return "SISP(" + this.fieldMapping + ")";
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 这个对象 传什么返回什么
     */
    public static class AllFieldsForwardedProperties extends SingleInputSemanticProperties {

        private static final long serialVersionUID = 1L;

        @Override
        public FieldSet getForwardingTargetFields(int input, int sourceField) {
            if (input != 0) {
                throw new IndexOutOfBoundsException();
            }
            return new FieldSet(sourceField);
        }

        @Override
        public int getForwardingSourceField(int input, int targetField) {
            if (input != 0) {
                throw new IndexOutOfBoundsException();
            }
            return targetField;
        }

        @Override
        public void addForwardedField(int sourceField, int targetField) {
            throw new UnsupportedOperationException("Cannot modify forwarded fields");
        }
    }
}
