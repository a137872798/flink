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
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;

import java.util.ArrayList;

/**
 * This class represents an ordering on a set of fields. It specifies the fields and order direction
 * (ascending, descending).
 * 代表一个顺序对象
 */
@Internal
public class Ordering implements Cloneable {

    /**
     * FieldList的api都是顺序敏感的 内部存储了一组fieldId
     */
    protected FieldList indexes = new FieldList();

    protected final ArrayList<Class<? extends Comparable<?>>> types =
            new ArrayList<Class<? extends Comparable<?>>>();

    /**
     * 一组顺序对象
     */
    protected final ArrayList<Order> orders = new ArrayList<Order>();

    // --------------------------------------------------------------------------------------------

    /** Creates an empty ordering. */
    public Ordering() {}

    /**
     * @param index
     * @param type
     * @param order
     */
    public Ordering(int index, Class<? extends Comparable<?>> type, Order order) {
        appendOrdering(index, type, order);
    }

    /**
     * Extends this ordering by appending an additional order requirement. If the index has been
     * previously appended then the unmodified Ordering is returned.
     *
     * @param index Field index of the appended order requirement.     表示某个字段的编号
     * @param type Type of the appended order requirement.
     * @param order Order of the appended order requirement.
     * @return This ordering with an additional appended order requirement.
     *
     * 该对象 应该是多个顺序的组合
     */
    public Ordering appendOrdering(
            Integer index, Class<? extends Comparable<?>> type, Order order) {
        if (index < 0) {
            throw new IllegalArgumentException("The key index must not be negative.");
        }
        if (order == null) {
            throw new NullPointerException();
        }
        if (order == Order.NONE) {
            throw new IllegalArgumentException(
                    "An ordering must not be created with a NONE order.");
        }

        if (!this.indexes.contains(index)) {
            // 表示该field使用的comparable对象 以及 顺序对象
            this.indexes = this.indexes.addField(index);
            this.types.add(type);
            this.orders.add(order);
        }

        return this;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 此时参与排序的所有字段
     * @return
     */
    public int getNumberOfFields() {
        return this.indexes.size();
    }

    /**
     * 返回涉及到的字段
     * @return
     */
    public FieldList getInvolvedIndexes() {
        return this.indexes;
    }

    // 下面3个方法 根据index去匹配

    public Integer getFieldNumber(int index) {
        if (index < 0 || index >= this.indexes.size()) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        return this.indexes.get(index);
    }

    public Class<? extends Comparable<?>> getType(int index) {
        if (index < 0 || index >= this.types.size()) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        return this.types.get(index);
    }

    public Order getOrder(int index) {
        if (index < 0 || index >= this.types.size()) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        return orders.get(index);
    }

    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public Class<? extends Comparable<?>>[] getTypes() {
        return this.types.toArray(new Class[this.types.size()]);
    }

    /**
     * 获取每个field的  position
     * @return
     */
    public int[] getFieldPositions() {
        final int[] ia = new int[this.indexes.size()];
        for (int i = 0; i < ia.length; i++) {
            ia[i] = this.indexes.get(i);
        }
        return ia;
    }

    public Order[] getFieldOrders() {
        return this.orders.toArray(new Order[this.orders.size()]);
    }

    public boolean[] getFieldSortDirections() {
        final boolean[] directions = new boolean[this.orders.size()];
        for (int i = 0; i < directions.length; i++) {
            directions[i] = this.orders.get(i) != Order.DESCENDING;
        }
        return directions;
    }

    // --------------------------------------------------------------------------------------------

    public boolean isMetBy(Ordering otherOrdering) {
        // 另一个是否涵盖这个
        if (otherOrdering == null || this.indexes.size() > otherOrdering.indexes.size()) {
            return false;
        }

        for (int i = 0; i < this.indexes.size(); i++) {

            // 另一个要比这个大
            if (!this.indexes.get(i).equals(otherOrdering.indexes.get(i))) {
                return false;
            }

            // if this one request no order, everything is good
            if (this.orders.get(i) != Order.NONE) {
                if (this.orders.get(i) == Order.ANY) {
                    // if any order is requested, any not NONE order is good
                    // any时 other一定要有个有效值
                    if (otherOrdering.orders.get(i) == Order.NONE) {
                        return false;
                    }
                    // 否则就要相同
                } else if (otherOrdering.orders.get(i) != this.orders.get(i)) {
                    // the orders must be equal
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 前几个是否一致
     * @param other
     * @param n
     * @return
     */
    public boolean isOrderEqualOnFirstNFields(Ordering other, int n) {
        if (n > getNumberOfFields() || n > other.getNumberOfFields()) {
            throw new IndexOutOfBoundsException();
        }

        for (int i = 0; i < n; i++) {
            final Order o = this.orders.get(i);
            // 不包含 NONE/ANY
            if (o == Order.NONE || o == Order.ANY || o != other.orders.get(i)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Creates a new ordering the represents an ordering on a prefix of the fields. If the exclusive
     * index up to which to create the ordering is <code>0</code>, then there is no resulting
     * ordering and this method return <code>null</code>.
     *
     * @param exclusiveIndex The index (exclusive) up to which to create the ordering.
     * @return The new ordering on the prefix of the fields, or <code>null</code>, if the prefix is
     *     empty.
     *     取前面几个顺序
     */
    public Ordering createNewOrderingUpToIndex(int exclusiveIndex) {
        if (exclusiveIndex == 0) {
            return null;
        }
        final Ordering newOrdering = new Ordering();
        for (int i = 0; i < exclusiveIndex; i++) {
            newOrdering.appendOrdering(this.indexes.get(i), this.types.get(i), this.orders.get(i));
        }
        return newOrdering;
    }

    public boolean groupsFields(FieldSet fields) {
        // TODO 上下2个逻辑感觉是相悖的
        if (fields.size() > this.indexes.size()) {
            return false;
        }

        for (int i = 0; i < fields.size(); i++) {
            if (!fields.contains(this.indexes.get(i))) {
                return false;
            }
        }
        return true;
    }

    // --------------------------------------------------------------------------------------------

    public Ordering clone() {
        Ordering newOrdering = new Ordering();
        newOrdering.indexes = this.indexes;
        newOrdering.types.addAll(this.types);
        newOrdering.orders.addAll(this.orders);
        return newOrdering;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((indexes == null) ? 0 : indexes.hashCode());
        result = prime * result + ((orders == null) ? 0 : orders.hashCode());
        result = prime * result + ((types == null) ? 0 : types.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Ordering other = (Ordering) obj;
        if (indexes == null) {
            if (other.indexes != null) {
                return false;
            }
        } else if (!indexes.equals(other.indexes)) {
            return false;
        }
        if (orders == null) {
            if (other.orders != null) {
                return false;
            }
        } else if (!orders.equals(other.orders)) {
            return false;
        }
        if (types == null) {
            if (other.types != null) {
                return false;
            }
        } else if (!types.equals(other.types)) {
            return false;
        }
        return true;
    }

    public String toString() {
        final StringBuilder buf = new StringBuilder("[");
        for (int i = 0; i < indexes.size(); i++) {
            if (i != 0) {
                buf.append(",");
            }
            buf.append(this.indexes.get(i));
            if (this.types.get(i) != null) {
                buf.append(":");
                buf.append(this.types.get(i).getName());
            }
            buf.append(":");
            buf.append(this.orders.get(i).getShortName());
        }
        buf.append("]");
        return buf.toString();
    }
}
