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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;

/**
 * 定义了一个产生比较器的工厂
 * */
@Internal
public interface TypeComparatorFactory<T> {

    /**
     * 将参数写入配置
     * @param config
     */
    void writeParametersToConfig(Configuration config);

    /**
     * 从配置中读取参数     (每个工厂按需从配置中读取参数)
     * @param config
     * @param cl
     * @throws ClassNotFoundException
     */
    void readParametersFromConfig(Configuration config, ClassLoader cl)
            throws ClassNotFoundException;

    /**
     * 核心方法 构建比较器
     * @return
     */
    TypeComparator<T> createComparator();
}
