/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.state.KeyGroupRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/** Handler for common queryable state logic.
 * 通过该对象可以操控 KvState
 * */
public class KvStateHandler {

    private static final Logger LOG = LoggerFactory.getLogger(KvStateHandler.class);

    /**
     * 执行图
     */
    private final ExecutionGraph executionGraph;

    public KvStateHandler(ExecutionGraph executionGraph) {
        this.executionGraph = executionGraph;
    }

    /**
     * 查询某个KvState的位置
     * @param jobId
     * @param registrationName
     * @return
     * @throws UnknownKvStateLocation
     * @throws FlinkJobNotFoundException
     */
    public KvStateLocation requestKvStateLocation(final JobID jobId, final String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException {

        // sanity check for the correct JobID
        // 执行图只对应一个job
        if (executionGraph.getJobID().equals(jobId)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Lookup key-value state for job {} with registration " + "name {}.",
                        executionGraph.getJobID(),
                        registrationName);
            }

            // 找到存储key位置的注册器
            final KvStateLocationRegistry registry = executionGraph.getKvStateLocationRegistry();
            // 通过名字查询位置
            final KvStateLocation location = registry.getKvStateLocation(registrationName);
            if (location != null) {
                return location;
            } else {
                throw new UnknownKvStateLocation(registrationName);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Request of key-value state location for unknown job {} received.", jobId);
            }
            throw new FlinkJobNotFoundException(jobId);
        }
    }

    /**
     * 当有新的KvState注册时触发
     * @param jobId
     * @param jobVertexId
     * @param keyGroupRange
     * @param registrationName
     * @param kvStateId
     * @param kvStateServerAddress
     * @throws FlinkJobNotFoundException
     */
    public void notifyKvStateRegistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName,
            final KvStateID kvStateId,
            final InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException {

        if (executionGraph.getJobID().equals(jobId)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Key value state registered for job {} under name {}.",
                        executionGraph.getJobID(),
                        registrationName);
            }

            try {
                // 转发完成注册
                executionGraph
                        .getKvStateLocationRegistry()
                        .notifyKvStateRegistered(
                                jobVertexId,
                                keyGroupRange,
                                registrationName,
                                kvStateId,
                                kvStateServerAddress);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new FlinkJobNotFoundException(jobId);
        }
    }

    public void notifyKvStateUnregistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName)
            throws FlinkJobNotFoundException {

        if (executionGraph.getJobID().equals(jobId)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Key value state unregistered for job {} under name {}.",
                        executionGraph.getJobID(),
                        registrationName);
            }

            try {
                // 转发完成注销
                executionGraph
                        .getKvStateLocationRegistry()
                        .notifyKvStateUnregistered(jobVertexId, keyGroupRange, registrationName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new FlinkJobNotFoundException(jobId);
        }
    }
}
