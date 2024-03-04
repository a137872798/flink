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

package org.apache.flink.runtime.query;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/** A helper for KvState registrations of a single task.
 * 该对象是以task为单位进行管理的   而这些TaskKvStateRegistry背后是同一个 KvStateRegistry    使用它进行统一维护
 * */
public class TaskKvStateRegistry {

    /** KvStateRegistry for KvState instance registrations.
     * 简单理解就是维护状态的
     * */
    private final KvStateRegistry registry;

    /** JobID of the task.
     * 当前task所属的job
     * */
    private final JobID jobId;

    /** JobVertexID of the task.
     * 标注该task相关的job的顶级id
     * */
    private final JobVertexID jobVertexId;

    /** List of all registered KvState instances of this task.
     * 维护本task相关的所有state
     * */
    private final List<KvStateInfo> registeredKvStates = new ArrayList<>();

    TaskKvStateRegistry(KvStateRegistry registry, JobID jobId, JobVertexID jobVertexId) {
        this.registry = Preconditions.checkNotNull(registry, "KvStateRegistry");
        this.jobId = Preconditions.checkNotNull(jobId, "JobID");
        this.jobVertexId = Preconditions.checkNotNull(jobVertexId, "JobVertexID");
    }

    /**
     * Registers the KvState instance at the KvStateRegistry.
     *
     * @param keyGroupRange Key group range the KvState instance belongs to
     * @param registrationName The registration name (not necessarily the same as the KvState name
     *     defined in the state descriptor used to create the KvState instance)
     * @param kvState The
     *                其实相当于对外暴露了这个state 
     */
    public void registerKvState(
            KeyGroupRange keyGroupRange,
            String registrationName,
            InternalKvState<?, ?, ?> kvState,
            ClassLoader userClassLoader) {

        // 由大registry注册  返回id 并包装成KvStateInfo
        KvStateID kvStateId =
                registry.registerKvState(
                        jobId,
                        jobVertexId,
                        keyGroupRange,
                        registrationName,
                        kvState,
                        userClassLoader);
        registeredKvStates.add(new KvStateInfo(keyGroupRange, registrationName, kvStateId));
    }

    /** Unregisters all registered KvState instances from the KvStateRegistry. */
    public void unregisterAll() {
        for (KvStateInfo kvState : registeredKvStates) {
            // 将该task相关的所有task都注销
            registry.unregisterKvState(
                    jobId,
                    jobVertexId,
                    kvState.keyGroupRange,
                    kvState.registrationName,
                    kvState.kvStateId);
        }
    }

    /** 3-tuple holding registered KvState meta data.
     * 由该task产生的状态对象  只包含基础信息
     * */
    private static class KvStateInfo {

        private final KeyGroupRange keyGroupRange;

        private final String registrationName;

        /**
         * 唯一id
         */
        private final KvStateID kvStateId;

        KvStateInfo(KeyGroupRange keyGroupRange, String registrationName, KvStateID kvStateId) {
            this.keyGroupRange = keyGroupRange;
            this.registrationName = registrationName;
            this.kvStateId = kvStateId;
        }
    }
}
