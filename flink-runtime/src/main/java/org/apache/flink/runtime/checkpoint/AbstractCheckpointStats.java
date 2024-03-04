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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Base class for checkpoint statistics.
 * 检查点描述的统计信息
 * */
public abstract class AbstractCheckpointStats implements Serializable {

    private static final long serialVersionUID = 1041218202028265151L;

    /** ID of this checkpoint. */
    final long checkpointId;

    /** Timestamp when the checkpoint was triggered at the coordinator.
     * 触发检查点的时间
     * */
    final long triggerTimestamp;

    /** {@link TaskStateStats} accessible by their ID.
     * 此时有哪些job  以及他们的任务统计数据   每个task下有多个subtask
     * */
    final Map<JobVertexID, TaskStateStats> taskStats;

    /** Total number of subtasks over all tasks.
     * 相关的子任务数量
     * */
    final int numberOfSubtasks;

    /** Properties of the checkpoint.
     * 检查点的一些属性
     * */
    final CheckpointProperties props;

    AbstractCheckpointStats(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            int numberOfSubtasks,
            Map<JobVertexID, TaskStateStats> taskStats) {

        this.checkpointId = checkpointId;
        this.triggerTimestamp = triggerTimestamp;
        this.taskStats = checkNotNull(taskStats);
        checkArgument(taskStats.size() > 0, "Empty task stats");
        checkArgument(numberOfSubtasks > 0, "Non-positive number of subtasks");
        this.numberOfSubtasks = numberOfSubtasks;
        this.props = checkNotNull(props);
    }

    /**
     * Returns the status of this checkpoint.
     *
     * @return Status of this checkpoint
     *
     * 获取当前检查点处理状态
     */
    public abstract CheckpointStatsStatus getStatus();

    /**
     * Returns the number of acknowledged subtasks.
     *
     * @return The number of acknowledged subtasks.
     * 返回此时已经确认ack的subtask数量
     */
    public abstract int getNumberOfAcknowledgedSubtasks();

    /**
     * Returns the total checkpoint state size over all subtasks.
     *
     * @return Total checkpoint state size over all subtasks.
     * 获取state的总大小  (包含所有subtask)
     */
    public abstract long getStateSize();

    /**
     * Returns the checkpointed size during that checkpoint.
     *
     * @return The checkpointed size during that checkpoint.
     *
     * 此时的检查点大小
     */
    public abstract long getCheckpointedSize();

    /** @return the total number of processed bytes during the checkpoint.
     * 返回处理中的数据大小
     * */
    public abstract long getProcessedData();

    /** @return the total number of persisted bytes during the checkpoint.
     * 已经持久化的数据大小
     * */
    public abstract long getPersistedData();

    /** @return whether the checkpoint is unaligned.
     * 检查点是否非对齐
     * */
    public abstract boolean isUnalignedCheckpoint();

    /**
     * Returns the latest acknowledged subtask stats or <code>null</code> if none was acknowledged
     * yet.
     *
     * @return Latest acknowledged subtask stats or <code>null</code>
     *
     * 获取最近的子任务状态统计信息
     */
    @Nullable
    public abstract SubtaskStateStats getLatestAcknowledgedSubtaskStats();

    /**
     * Returns the ID of this checkpoint.
     *
     * @return ID of this checkpoint.
     */
    public long getCheckpointId() {
        return checkpointId;
    }

    /**
     * Returns the timestamp when the checkpoint was triggered.
     *
     * @return Timestamp when the checkpoint was triggered.
     *
     * 触发检查点的时间
     */
    public long getTriggerTimestamp() {
        return triggerTimestamp;
    }

    /**
     * Returns the properties of this checkpoint.
     *
     * @return Properties of this checkpoint.
     */
    public CheckpointProperties getProperties() {
        return props;
    }

    /**
     * Returns the total number of subtasks involved in this checkpoint.
     *
     * @return Total number of subtasks involved in this checkpoint.
     */
    public int getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    /**
     * Returns the task state stats for the given job vertex ID or <code>null</code> if no task with
     * such an ID is available.
     *
     * @param jobVertexId Job vertex ID of the task stats to look up.
     * @return The task state stats instance for the given ID or <code>null</code>.
     *
     * 获取某个job相关的任务统计信息
     */
    public TaskStateStats getTaskStateStats(JobVertexID jobVertexId) {
        return taskStats.get(jobVertexId);
    }

    /**
     * Returns all task state stats instances.
     *
     * @return All task state stats instances.
     */
    public Collection<TaskStateStats> getAllTaskStateStats() {
        return taskStats.values();
    }

    /**
     * Returns the ack timestamp of the latest acknowledged subtask or <code>-1</code> if none was
     * acknowledged yet.
     *
     * @return Ack timestamp of the latest acknowledged subtask or <code>-1</code>.
     */
    public long getLatestAckTimestamp() {
        SubtaskStateStats subtask = getLatestAcknowledgedSubtaskStats();
        if (subtask != null) {
            return subtask.getAckTimestamp();
        } else {
            return -1;
        }
    }

    /**
     * Returns the duration of this checkpoint calculated as the time since triggering until the
     * latest acknowledged subtask or <code>-1</code> if no subtask was acknowledged yet.
     *
     * @return Duration of this checkpoint or <code>-1</code> if no subtask was acknowledged yet.
     *
     * 返回自触发检查点到收到最近一个subtask的ack时间差
     */
    public long getEndToEndDuration() {
        SubtaskStateStats subtask = getLatestAcknowledgedSubtaskStats();
        if (subtask != null) {
            return Math.max(0, subtask.getAckTimestamp() - triggerTimestamp);
        } else {
            return -1;
        }
    }
}
