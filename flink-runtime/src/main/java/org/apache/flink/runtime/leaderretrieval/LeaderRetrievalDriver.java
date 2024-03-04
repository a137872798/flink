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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.runtime.leaderelection.LeaderElectionDriver;

/**
 * A {@link LeaderRetrievalDriver} is responsible for retrieves the current leader which has been
 * elected by the {@link LeaderElectionDriver}.
 *
 * <p><strong>Important</strong>: The {@link LeaderRetrievalDriver} could not guarantee that there
 * is no {@link LeaderRetrievalEventHandler} callbacks happen after {@link #close()}.
 * leader检索驱动  用于发现leader节点的
 */
public interface LeaderRetrievalDriver extends AutoCloseable {}
