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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;

/** Interface for completion actions once a Flink job has reached a terminal state.
 * 表示当某个job结束时触发
 * */
public interface OnCompletionActions {

    /**
     * Job reached a globally terminal state.
     *
     * @param executionGraphInfo contains information about the terminated job
     *                           到达一个全局的终止状态时触发
     */
    void jobReachedGloballyTerminalState(ExecutionGraphInfo executionGraphInfo);

    /** The {@link JobMaster} failed while executing the job.
     * 以失败形式触发
     * */
    void jobMasterFailed(Throwable cause);
}
