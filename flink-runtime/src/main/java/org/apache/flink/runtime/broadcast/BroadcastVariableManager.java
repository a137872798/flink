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

package org.apache.flink.runtime.broadcast;

import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.runtime.io.network.api.reader.MutableReader;
import org.apache.flink.runtime.operators.BatchTask;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The BroadcastVariableManager is used to manage the materialization of broadcast variables.
 * References to materialized broadcast variables are cached and shared between parallel subtasks. A
 * reference count is maintained to track whether the materialization may be cleaned up.
 *
 * 广播变量管理器
 */
public class BroadcastVariableManager {

    /**
     * 维护广播变量的kv关系
     */
    private final ConcurrentHashMap<BroadcastVariableKey, BroadcastVariableMaterialization<?, ?>>
            variables =
                    new ConcurrentHashMap<
                            BroadcastVariableKey, BroadcastVariableMaterialization<?, ?>>(16);

    // --------------------------------------------------------------------------------------------

    /**
     * Materializes the broadcast variable for the given name, scoped to the given task and its
     * iteration superstep. An existing materialization created by another parallel subtask may be
     * returned, if it hasn't expired yet.
     *
     * 每个task都尝试用reader对象内的数据来物化广播变量 只会采用第一个task的数据 其他task则变成广播变量的引用者
     * 当引用数量归0时  清除广播变量
     */
    public <T> BroadcastVariableMaterialization<T, ?> materializeBroadcastVariable(
            String name,
            int superstep,
            BatchTask<?, ?> holder,
            MutableReader<?> reader,  // 通过它读取广播变量
            TypeSerializerFactory<T> serializerFactory)
            throws IOException {
        final BroadcastVariableKey key =
                new BroadcastVariableKey(holder.getEnvironment().getJobVertexId(), name, superstep);

        while (true) {
            final BroadcastVariableMaterialization<T, Object> newMat =
                    new BroadcastVariableMaterialization<T, Object>(key);

            final BroadcastVariableMaterialization<?, ?> previous =
                    // putIfAbsent 并不会覆盖
                    variables.putIfAbsent(key, newMat);

            @SuppressWarnings("unchecked")
            final BroadcastVariableMaterialization<T, ?> materialization =
                    (previous == null) ? newMat : (BroadcastVariableMaterialization<T, ?>) previous;

            try {
                // 记录信息
                materialization.materializeVariable(reader, serializerFactory, holder);
                return materialization;

                // 表示广播变量已经被关闭了  重新创建并插入map
            } catch (MaterializationExpiredException e) {
                // concurrent release. as an optimization, try to replace the previous one with our
                // version. otherwise we might spin for a while
                // until the releaser removes the variable
                // NOTE: This would also catch a bug prevented an expired materialization from ever
                // being removed, so it acts as a future safeguard

                boolean replaceSuccessful = false;
                try {
                    replaceSuccessful = variables.replace(key, materialization, newMat);
                } catch (Throwable t) {
                }

                if (replaceSuccessful) {
                    try {
                        newMat.materializeVariable(reader, serializerFactory, holder);
                        return newMat;
                    } catch (MaterializationExpiredException ee) {
                        // can still happen in cases of extreme races and fast tasks
                        // fall through the loop;
                    }
                }
                // else fall through the loop
            }
        }
    }

    /**
     * 该task 不再引用广播变量
     * @param name
     * @param superstep
     * @param referenceHolder
     */
    public void releaseReference(String name, int superstep, BatchTask<?, ?> referenceHolder) {
        BroadcastVariableKey key =
                new BroadcastVariableKey(
                        referenceHolder.getEnvironment().getJobVertexId(), name, superstep);
        releaseReference(key, referenceHolder);
    }

    public void releaseReference(BroadcastVariableKey key, BatchTask<?, ?> referenceHolder) {
        BroadcastVariableMaterialization<?, ?> mat = variables.get(key);

        // release this reference
        // 返回true 代表该广播变量不再被引用了
        if (mat.decrementReference(referenceHolder)) {
            // remove if no one holds a reference and no one concurrently replaced the entry
            variables.remove(key, mat);
        }
    }

    public void releaseAllReferencesFromTask(BatchTask<?, ?> referenceHolder) {
        // go through all registered variables
        for (Map.Entry<BroadcastVariableKey, BroadcastVariableMaterialization<?, ?>> entry :
                variables.entrySet()) {
            BroadcastVariableMaterialization<?, ?> mat = entry.getValue();

            // release the reference
            if (mat.decrementReferenceIfHeld(referenceHolder)) {
                // remove if no one holds a reference and no one concurrently replaced the entry
                variables.remove(entry.getKey(), mat);
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    public int getNumberOfVariablesWithReferences() {
        return this.variables.size();
    }
}
