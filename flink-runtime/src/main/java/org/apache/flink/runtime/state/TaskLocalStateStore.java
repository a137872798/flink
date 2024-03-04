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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.function.LongPredicate;

/**
 * Classes that implement this interface serve as a task-manager-level local storage for local
 * checkpointed state. The purpose is to provide access to a state that is stored locally for a
 * faster recovery compared to the state that is stored remotely in a stable store DFS. For now,
 * this storage is only complementary to the stable storage and local state is typically lost in
 * case of machine failures. In such cases (and others), client code of this class must fall back to
 * using the slower but highly available store.
 *
 * 表示任务状态的存储
 */
@Internal
public interface TaskLocalStateStore {
    /**
     * Stores the local state for the given checkpoint id.
     *
     * @param checkpointId id for the checkpoint that created the local state that will be stored.
     * @param localState the local state to store.
     *                   存储某个检查点数据  传入检查点id 和快照数据
     */
    void storeLocalState(@Nonnegative long checkpointId, @Nullable TaskStateSnapshot localState);

    /**
     * Returns the local state that is stored under the given checkpoint id or null if nothing was
     * stored under the id.
     *
     * @param checkpointID the checkpoint id by which we search for local state.
     * @return the local state found for the given checkpoint id. Can be null
     * 从仓库再拿回任务快照
     */
    @Nullable
    TaskStateSnapshot retrieveLocalState(long checkpointID);

    /** Returns the {@link LocalRecoveryConfig} for this task local state store.
     * 获取有关存储目录的对象
     * */
    @Nonnull
    LocalRecoveryConfig getLocalRecoveryConfig();

    /**
     * Notifies that the checkpoint with the given id was confirmed as complete. This prunes the
     * checkpoint history and removes all local states with a checkpoint id that is smaller than the
     * newly confirmed checkpoint id.
     * 表示该检查点已经完成    会删除本地小于该检查点id的历史数据
     */
    void confirmCheckpoint(long confirmedCheckpointId);

    /**
     * Notifies that the checkpoint with the given id was confirmed as aborted. This prunes the
     * checkpoint history and removes states with a checkpoint id that is equal to the newly aborted
     * checkpoint id.
     * 终止某次检查点工作
     */
    void abortCheckpoint(long abortedCheckpointId);

    /**
     * Remove all checkpoints from the store that match the given predicate.
     *
     * @param matcher the predicate that selects the checkpoints for pruning.
     *                删除所有满足条件的检查点数据
     */
    void pruneMatchingCheckpoints(LongPredicate matcher);
}
