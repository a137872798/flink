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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.concurrent.CompletableFuture;

/**
 * Client interface which sends out a {@link CoordinationRequest} and expects for a {@link
 * CoordinationResponse} from a {@link OperatorCoordinator}.
 * 协调者请求网关  用于将请求发给协调者
 */
public interface CoordinationRequestGateway {

    /**
     * Send out a request to a specified coordinator and return the response.
     *
     * @param operatorId specifies which coordinator to receive the request  请求要关联一个operatorId
     * @param request the request to send
     * @return the response from the coordinator
     */
    CompletableFuture<CoordinationResponse> sendCoordinationRequest(
            OperatorID operatorId, CoordinationRequest request);
}
