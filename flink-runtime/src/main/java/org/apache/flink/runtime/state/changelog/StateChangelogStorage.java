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

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.state.KeyGroupRange;

/**
 * A storage for changelog. Could produce {@link StateChangelogHandleReader} and {@link
 * StateChangelogWriter} for read and write. Please use {@link StateChangelogStorageLoader} to
 * obtain an instance.
 */
@Internal
public interface StateChangelogStorage<Handle extends ChangelogStateHandle>
        extends StateChangelogStorageView<Handle> {

    /**
     * view 提供产生reader的api  作为storage就还要提供产生writer的api 用于写入stateChange
     * @param operatorID  表示哪次操作导致的state变化
     * @param keyGroupRange   该状态关联的key范围
     * @param mailboxExecutor
     * @return
     */
    StateChangelogWriter<Handle> createWriter(
            String operatorID, KeyGroupRange keyGroupRange, MailboxExecutor mailboxExecutor);

    default AvailabilityProvider getAvailabilityProvider() {
        return () -> AvailabilityProvider.AVAILABLE;
    }
}
