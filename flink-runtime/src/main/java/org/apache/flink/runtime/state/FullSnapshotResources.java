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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.io.IOException;
import java.util.List;

/**
 * A {@link SnapshotResources} to be used with the backend-independent {@link
 * FullSnapshotAsyncWriter}.
 *
 * @param <K> type of the backend keys.
 *           产生快照时依赖的各种资源
 *
 */
@Internal
public interface FullSnapshotResources<K> extends SnapshotResources {

    /**
     * Returns the list of {@link StateMetaInfoSnapshot meta info snapshots} for this state
     * snapshot.
     * 有关该状态元数据的快照
     */
    List<StateMetaInfoSnapshot> getMetaInfoSnapshots();

    /**
     * Returns a {@link KeyValueStateIterator} for iterating over all key-value states for this
     * snapshot resources.
     * 获取迭代器 遍历state entry
     */
    KeyValueStateIterator createKVStateIterator() throws IOException;

    /** Returns the {@link KeyGroupRange} of this snapshot.
     * 获取快照 keyGroup
     * */
    KeyGroupRange getKeyGroupRange();

    /** Returns key {@link TypeSerializer}.
     * 包含key 序列化/反序列化逻辑的对象
     * */
    TypeSerializer<K> getKeySerializer();

    /** Returns the {@link StreamCompressionDecorator} that should be used for writing.
     * 返回压缩流
     * */
    StreamCompressionDecorator getStreamCompressionDecorator();
}
