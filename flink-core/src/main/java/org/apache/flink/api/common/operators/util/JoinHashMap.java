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

package org.apache.flink.api.common.operators.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.apache.commons.collections.map.AbstractHashedMap;

@Internal
public class JoinHashMap<BT> extends AbstractHashedMap {

    /**
     * 该对象可以序列化/反序列化对象
     */
    private final TypeSerializer<BT> buildSerializer;

    /**
     * 提供一些比较BT类型的api
     */
    private final TypeComparator<BT> buildComparator;

    public JoinHashMap(TypeSerializer<BT> buildSerializer, TypeComparator<BT> buildComparator) {
        super(64);
        this.buildSerializer = buildSerializer;
        this.buildComparator = buildComparator;
    }

    public TypeSerializer<BT> getBuildSerializer() {
        return buildSerializer;
    }

    public TypeComparator<BT> getBuildComparator() {
        return buildComparator;
    }

    /**
     * 创建一个探测对象
     * @param probeComparator
     * @param pairComparator
     * @param <PT>
     * @return
     */
    public <PT> Prober<PT> createProber(
            TypeComparator<PT> probeComparator, TypePairComparator<PT, BT> pairComparator) {
        return new Prober<PT>(probeComparator, pairComparator);
    }

    @SuppressWarnings("unchecked")
    public void insertOrReplace(BT record) {
        // 二次计算才能得到hash值
        int hashCode = hash(buildComparator.hash(record));
        int index = hashIndex(hashCode, data.length);
        buildComparator.setReference(record);
        HashEntry entry = data[index];
        while (entry != null) {
            // 2个record 判定是相同的 才能覆盖   这个判定相同 可能只判断了某些关键field
            if (entryHashCode(entry) == hashCode
                    && buildComparator.equalToReference((BT) entry.getValue())) {
                entry.setValue(record);
                return;
            }
            // 顺着链表往下
            entry = entryNext(entry);
        }

        // 首次插入到map中
        addMapping(index, hashCode, null, record);
    }

    /**
     * 探测对象
     * @param <PT>
     */
    public class Prober<PT> {

        /**
         * 创建探测对象
         * @param probeComparator  用于相同类型比较
         * @param pairComparator  用于比较2个不同的类型
         */
        public Prober(
                TypeComparator<PT> probeComparator, TypePairComparator<PT, BT> pairComparator) {
            this.probeComparator = probeComparator;
            this.pairComparator = pairComparator;
        }

        private final TypeComparator<PT> probeComparator;

        private final TypePairComparator<PT, BT> pairComparator;

        @SuppressWarnings("unchecked")
        public BT lookupMatch(PT record) {
            int hashCode = hash(probeComparator.hash(record));
            int index = hashIndex(hashCode, data.length);
            pairComparator.setReference(record);
            HashEntry entry = data[index];
            while (entry != null) {
                if (entryHashCode(entry) == hashCode
                        && pairComparator.equalToReference((BT) entry.getValue())) {
                    // 发现与record 判定为相同的值    注意他们类型是不一样的  跟insertOrReplace 不同
                    return (BT) entry.getValue();
                }
                entry = entryNext(entry);
            }
            return null;
        }
    }
}
