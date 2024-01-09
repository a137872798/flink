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

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.types.Value;

/**
 * A specialization of the {@link RuntimeContext} available in iterative computations of the DataSet
 * API.
 * 可迭代上下文 额外拥有获取聚合器的api
 */
@Public
public interface IterationRuntimeContext extends RuntimeContext {

    /**
     * Gets the number of the current superstep. Superstep numbers start at <i>1</i>.
     *
     * @return The number of the current superstep.
     * 获取当前的步骤
     */
    int getSuperstepNumber();

    @PublicEvolving
    <T extends Aggregator<?>> T getIterationAggregator(String name);

    /**
     * 获取之前聚合的结果
     * @param name
     * @param <T>
     * @return
     */
    <T extends Value> T getPreviousIterationAggregate(String name);
}
