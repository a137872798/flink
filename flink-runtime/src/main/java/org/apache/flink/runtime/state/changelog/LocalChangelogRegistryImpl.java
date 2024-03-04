/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

/**
 * 本地changelog 默认实现
 */
@Internal
public class LocalChangelogRegistryImpl implements LocalChangelogRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(LocalChangelogRegistry.class);
    /**
     * All registered handles. (PhysicalStateHandleID, (handle, checkpointID)) represents a handle
     * and the latest checkpoint that refer to this handle.
     * 也是维护 StreamStateHandle 和 checkpointID 的映射关系
     */
    private final Map<PhysicalStateHandleID, Tuple2<StreamStateHandle, Long>>
            handleToLastUsedCheckpointID = new ConcurrentHashMap<>();

    /** Executor for async state deletion.
     * 通过执行器进行删除
     * */
    private final Executor asyncDisposalExecutor;

    public LocalChangelogRegistryImpl(Executor ioExecutor) {
        this.asyncDisposalExecutor = ioExecutor;
    }

    public void register(StreamStateHandle handle, long checkpointID) {
        handleToLastUsedCheckpointID.compute(
                handle.getStreamStateHandleID(),
                (k, v) -> {
                    if (v == null) {
                        return Tuple2.of(handle, checkpointID);
                    } else {
                        Preconditions.checkState(handle.equals(v.f0));
                        return Tuple2.of(handle, Math.max(v.f1, checkpointID));
                    }
                });
    }

    public void discardUpToCheckpoint(long upTo) {
        List<StreamStateHandle> handles = new ArrayList<>();
        synchronized (handleToLastUsedCheckpointID) {
            Iterator<Tuple2<StreamStateHandle, Long>> iterator =
                    handleToLastUsedCheckpointID.values().iterator();
            while (iterator.hasNext()) {
                Tuple2<StreamStateHandle, Long> entry = iterator.next();
                if (entry.f1 < upTo) {
                    handles.add(entry.f0);
                    iterator.remove();
                }
            }
        }
        for (StreamStateHandle handle : handles) {
            scheduleAsyncDelete(handle);
        }
    }

    /**
     * 看来多个stateHandle 可能会对应同一个检查点
     * @param checkpointID to abort
     */
    public void prune(long checkpointID) {
        Set<StreamStateHandle> handles =
                handleToLastUsedCheckpointID.values().stream()
                        .filter(tuple -> tuple.f1 == checkpointID)
                        .map(tuple -> tuple.f0)
                        .collect(Collectors.toSet());
        for (StreamStateHandle handle : handles) {
            scheduleAsyncDelete(handle);
        }
    }

    private void scheduleAsyncDelete(StreamStateHandle streamStateHandle) {
        if (streamStateHandle != null) {
            LOG.trace("Scheduled delete of state handle {}.", streamStateHandle);
            Runnable discardRunner =
                    () -> {
                        try {
                            streamStateHandle.discardState();
                        } catch (Exception exception) {
                            LOG.warn(
                                    "A problem occurred during asynchronous disposal of a stream handle {}.",
                                    streamStateHandle);
                        }
                    };
            try {
                asyncDisposalExecutor.execute(discardRunner);
            } catch (RejectedExecutionException ex) {
                discardRunner.run();
            }
        }
    }
}
