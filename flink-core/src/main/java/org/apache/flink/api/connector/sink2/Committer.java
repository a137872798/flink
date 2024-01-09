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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.PublicEvolving;

import java.io.IOException;
import java.util.Collection;

/**
 * The {@code Committer} is responsible for committing the data staged by the {@link
 * TwoPhaseCommittingSink.PrecommittingSinkWriter} in the second step of a two-phase commit
 * protocol.
 *
 * <p>A commit must be idempotent: If some failure occurs in Flink during commit phase, Flink will
 * restart from previous checkpoint and re-attempt to commit all committables. Thus, some or all
 * committables may have already been committed. These {@link CommitRequest}s must not change the
 * external system and implementers are asked to signal {@link
 * CommitRequest#signalAlreadyCommitted()}.
 *
 * @param <CommT> The type of information needed to commit the staged data
 *
 *               该对象用于提交请求
 */
@PublicEvolving
public interface Committer<CommT> extends AutoCloseable {
    /**
     * Commit the given list of {@link CommT}.
     *
     * @param committables A list of commit requests staged by the sink writer.
     * @throws IOException for reasons that may yield a complete restart of the job.
     * 提交一组Req对象
     */
    void commit(Collection<CommitRequest<CommT>> committables)
            throws IOException, InterruptedException;

    /**
     * A request to commit a specific committable.
     *
     * @param <CommT>
     *     该请求包装了一个可提交的对象
     */
    @PublicEvolving
    interface CommitRequest<CommT> {

        /** Returns the committable.
         * 获取内部的可提交对象
         * */
        CommT getCommittable();

        /**
         * Returns how many times this particular committable has been retried. Starts at 0 for the
         * first attempt.
         * 提交有一个重试次数
         */
        int getNumberOfRetries();

        /**
         * The commit failed for known reason and should not be retried.
         *
         * <p>Currently calling this method only logs the error, discards the comittable and
         * continues. In the future the behaviour might be configurable.
         * 标记提交失败  这样会避免重试
         */
        void signalFailedWithKnownReason(Throwable t);

        /**
         * The commit failed for unknown reason and should not be retried.
         *
         * <p>Currently calling this method fails the job. In the future the behaviour might be
         * configurable.
         */
        void signalFailedWithUnknownReason(Throwable t);

        /**
         * The commit failed for a retriable reason. If the sink supports a retry maximum, this may
         * permanently fail after reaching that maximum. Else the committable will be retried as
         * long as this method is invoked after each attempt.
         * 在提交失败后 调用该方法会在一定延时后重试
         */
        void retryLater();

        /**
         * Updates the underlying committable and retries later (see {@link #retryLater()} for a
         * description). This method can be used if a committable partially succeeded.
         * 更新可提交对象 并重试
         */
        void updateAndRetryLater(CommT committable);

        /**
         * Signals that a committable is skipped as it was committed already in a previous run.
         * Using this method is optional but eases bookkeeping and debugging. It also serves as a
         * code documentation for the branches dealing with recovery.
         * 标记已经提交成功
         */
        void signalAlreadyCommitted();
    }
}
