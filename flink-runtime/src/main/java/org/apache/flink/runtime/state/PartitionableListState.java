/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of operator list state.
 *
 * @param <S> the type of an operator state partition.
 *
 *           表示一个可以分区的  ListState
 */
public final class PartitionableListState<S> implements ListState<S> {

    /** Meta information of the state, including state name, assignment mode, and typeSerializer
     * 这种状态是  OperatorState      包含相关的元数据
     * */
    private RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo;

    /** The internal list the holds the elements of the state
     * 使用list存储 state中的元素
     * */
    private final ArrayList<S> internalList;

    /** A typeSerializer that allows to perform deep copies of internalList */
    private ArrayListSerializer<S> internalListCopySerializer;

    PartitionableListState(RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo) {
        this(stateMetaInfo, new ArrayList<S>());
    }

    private PartitionableListState(
            RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo, ArrayList<S> internalList) {

        this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
        this.internalList = Preconditions.checkNotNull(internalList);

        // 设置内部element的序列化对象
        this.internalListCopySerializer =
                new ArrayListSerializer<>(stateMetaInfo.getPartitionStateSerializer());
    }

    private PartitionableListState(PartitionableListState<S> toCopy) {

        this(
                toCopy.stateMetaInfo.deepCopy(),
                toCopy.internalListCopySerializer.copy(toCopy.internalList));
    }

    public void setStateMetaInfo(RegisteredOperatorStateBackendMetaInfo<S> stateMetaInfo) {
        this.internalListCopySerializer =
                new ArrayListSerializer<>(stateMetaInfo.getPartitionStateSerializer());
        this.stateMetaInfo = stateMetaInfo;
    }

    public RegisteredOperatorStateBackendMetaInfo<S> getStateMetaInfo() {
        return stateMetaInfo;
    }

    public PartitionableListState<S> deepCopy() {
        return new PartitionableListState<>(this);
    }

    @Override
    public void clear() {
        internalList.clear();
    }

    /**
     * 获取内部的状态
     * @return
     */
    @Override
    public Iterable<S> get() {
        return internalList;
    }

    /**
     * 追加一个值
     * @param value The new value for the state.
     */
    @Override
    public void add(S value) {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
        internalList.add(value);
    }

    @Override
    public String toString() {
        return "PartitionableListState{"
                + "stateMetaInfo="
                + stateMetaInfo
                + ", internalList="
                + internalList
                + '}';
    }

    public long[] write(FSDataOutputStream out) throws IOException {

        long[] partitionOffsets = new long[internalList.size()];

        DataOutputView dov = new DataOutputViewStreamWrapper(out);

        // 写位置 再写序列化数据
        for (int i = 0; i < internalList.size(); ++i) {
            S element = internalList.get(i);
            partitionOffsets[i] = out.getPos();
            getStateMetaInfo().getPartitionStateSerializer().serialize(element, dov);
        }

        return partitionOffsets;
    }

    /**
     * 更新ListState内的数据
     * @param values The new values for the state.
     */
    @Override
    public void update(List<S> values) {
        internalList.clear();

        addAll(values);
    }

    @Override
    public void addAll(List<S> values) {
        if (values != null && !values.isEmpty()) {
            internalList.addAll(values);
        }
    }

    @VisibleForTesting
    public ArrayListSerializer<S> getInternalListCopySerializer() {
        return internalListCopySerializer;
    }
}
