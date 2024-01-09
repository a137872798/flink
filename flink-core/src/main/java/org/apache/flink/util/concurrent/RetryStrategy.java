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

package org.apache.flink.util.concurrent;

import java.time.Duration;

/** Interface that encapsulates retry logic. An instances should be immutable.
 * 表示重试策略
 * */
public interface RetryStrategy {
    /** @return the number of remaining retries
     * 剩余的重试次数
     * */
    int getNumRemainingRetries();

    /** @return the current delay if we need to retry
     * 每次重试的延迟
     * */
    Duration getRetryDelay();

    /** @return the next retry strategy to current delay if we need to retry
     * 相当于更新重试策略 比如次数会减少 延迟会增加
     * */
    RetryStrategy getNextRetryStrategy();
}
