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

/** A keyed state backend interface for internal testing purpose.
 * 表示可用于内部测试
 * */
@VisibleForTesting
public interface TestableKeyedStateBackend<K> extends KeyedStateBackend<K> {
    /** Returns the total number of state entries across all keys/namespaces.
     * 返回状态总数   囊括所有keys和namespaces
     * */
    int numKeyValueStateEntries();
    /**
     * @return delegated {@link KeyedStateBackend} if this backends delegates its
     *     responisibilities..
     * @param recursive true if the call should be recursive
     *                  返回最内层的状态后端
     */
    default KeyedStateBackend<K> getDelegatedKeyedStateBackend(boolean recursive) {
        return this;
    }
}
