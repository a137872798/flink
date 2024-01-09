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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

/**
 * A factory for selecting and instantiating the adequate {@link PipelineExecutor} based on a
 * provided {@link Configuration}.
 * 用于创建管道执行器
 */
@Internal
public interface PipelineExecutorFactory {

    /** Returns the name of the executor that this factory creates.
     * 返回创建的执行器名字
     * */
    String getName();

    /**
     * Returns {@code true} if this factory is compatible with the options in the provided
     * configuration, {@code false} otherwise.
     * 检查工厂与配置是否兼容
     */
    boolean isCompatibleWith(final Configuration configuration);

    /**
     * Instantiates an {@link PipelineExecutor} compatible with the provided configuration.
     *
     * @return the executor instance.
     * 基于相关配置产生执行器
     */
    PipelineExecutor getExecutor(final Configuration configuration);
}
