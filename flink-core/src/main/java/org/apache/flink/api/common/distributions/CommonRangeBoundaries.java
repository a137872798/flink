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
package org.apache.flink.api.common.distributions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;

@Internal
public class CommonRangeBoundaries<T> implements RangeBoundaries<T> {
    /**
     * 用于比较给定类型 也可用来做hash
     */
    private final TypeComparator<T> typeComparator;

    /**
     * 外层代表多个对象  内层代表每个对象参与比较的多个field 与keys对应
     */
    private final Object[][] boundaries;

    /**
     * 由 typeComparator 平铺开得到  该数组中每个比较器针对某个field   看来一个类型比较器可能会涉及到对内部多个字段的比较 然后得到一个最终序号
     */
    private final TypeComparator[] flatComparators;

    /**
     * 对应flatComparators 的field
     */
    private final Object[] keys;

    public CommonRangeBoundaries(TypeComparator<T> typeComparators, Object[][] boundaries) {
        this.typeComparator = typeComparators;
        this.flatComparators = typeComparators.getFlatComparators();
        this.keys = new Object[flatComparators.length];
        this.boundaries = boundaries;
    }

    @Override
    public int getRangeIndex(T record) {
        return binarySearch(record);
    }

    // Search the range index of input record.
    private int binarySearch(T record) {
        int low = 0;
        int high = this.boundaries.length - 1;

        // 从record中抽取 之后需要对比的字段 并存入keys中
        typeComparator.extractKeys(record, keys, 0);

        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final int result = compareKeys(flatComparators, keys, this.boundaries[mid]);

            if (result > 0) {
                low = mid + 1;
            } else if (result < 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        // key not found, but the low index is the target
        // bucket, since the boundaries are the upper bound
        return low;
    }

    // 将keys 与boundary的值进行比较 得到一个位置
    private int compareKeys(TypeComparator[] flatComparators, Object[] keys, Object[] boundary) {
        if (flatComparators.length != keys.length || flatComparators.length != boundary.length) {
            throw new RuntimeException(
                    "Can not compare keys with boundary due to mismatched length.");
        }

        for (int i = 0; i < flatComparators.length; i++) {
            int result = flatComparators[i].compare(keys[i], boundary[i]);
            if (result != 0) {
                return result;
            }
        }

        // 全部都为0 代表完全相等
        return 0;
    }
}
