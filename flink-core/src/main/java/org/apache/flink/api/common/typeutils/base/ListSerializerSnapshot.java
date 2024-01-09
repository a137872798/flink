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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.List;

/** Snapshot class for the {@link ListSerializer}.
 * 从代码上观察 CompositeTypeSerializerSnapshot 分为一个外层序列化对象 和多个嵌套的内层序列化对象
 * */
public class ListSerializerSnapshot<T>
        extends CompositeTypeSerializerSnapshot<List<T>, ListSerializer<T>> {

    private static final int CURRENT_VERSION = 1;

    /** Constructor for read instantiation. */
    public ListSerializerSnapshot() {
        super(ListSerializer.class);
    }

    /** Constructor to create the snapshot for writing. */
    public ListSerializerSnapshot(ListSerializer<T> listSerializer) {
        super(listSerializer);
    }

    @Override
    public int getCurrentOuterSnapshotVersion() {
        return CURRENT_VERSION;
    }

    /**
     * 卧槽 这也太简单了吧 这里已经认定嵌套的序列化对象都是一样的 直接取一个 生成ListSerializer
     * 不过这也是正确的   CompositeTypeSerializerSnapshot 主要针对的对象应该是 Pojo tuple row 之类的组合对象 其中的各字段类型一般是不一样的
     * @param nestedSerializers array of nested serializers to create the outer serializer with.
     * @return
     */
    @Override
    protected ListSerializer<T> createOuterSerializerWithNestedSerializers(
            TypeSerializer<?>[] nestedSerializers) {
        @SuppressWarnings("unchecked")
        TypeSerializer<T> elementSerializer = (TypeSerializer<T>) nestedSerializers[0];
        return new ListSerializer<>(elementSerializer);
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(ListSerializer<T> outerSerializer) {
        return new TypeSerializer<?>[] {outerSerializer.getElementSerializer()};
    }
}
