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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The default implementation of the {@link PipelineExecutorServiceLoader}. This implementation uses
 * Java service discovery to find the available {@link PipelineExecutorFactory executor factories}.
 *
 * 默认实现
 */
@Internal
public class DefaultExecutorServiceLoader implements PipelineExecutorServiceLoader {

    // TODO: This code is almost identical to the ClusterClientServiceLoader and its default
    // implementation.
    // The reason of this duplication is the package structure which does not allow for the
    // ExecutorServiceLoader
    // to know about the ClusterClientServiceLoader. Remove duplication when package structure has
    // improved.

    private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutorServiceLoader.class);

    /**
     * 根据配置产生工厂
     * @param configuration
     * @return
     */
    @Override
    public PipelineExecutorFactory getExecutorFactory(final Configuration configuration) {
        checkNotNull(configuration);

        final ServiceLoader<PipelineExecutorFactory> loader =
                ServiceLoader.load(PipelineExecutorFactory.class);

        final List<PipelineExecutorFactory> compatibleFactories = new ArrayList<>();
        final Iterator<PipelineExecutorFactory> factories = loader.iterator();
        while (factories.hasNext()) {
            try {
                // 遍历通过spi加载的所有工厂
                final PipelineExecutorFactory factory = factories.next();
                // 找到兼容的
                if (factory != null && factory.isCompatibleWith(configuration)) {
                    compatibleFactories.add(factory);
                }
            } catch (Throwable e) {
                if (e.getCause() instanceof NoClassDefFoundError) {
                    LOG.info("Could not load factory due to missing dependencies.");
                } else {
                    throw e;
                }
            }
        }

        // 期望应当只有一个符合条件  猜测config中应该有执行器的名字
        if (compatibleFactories.size() > 1) {
            final String configStr =
                    configuration.toMap().entrySet().stream()
                            .map(e -> e.getKey() + "=" + e.getValue())
                            .collect(Collectors.joining("\n"));

            throw new IllegalStateException(
                    "Multiple compatible client factories found for:\n" + configStr + ".");
        }

        if (compatibleFactories.isEmpty()) {
            throw new IllegalStateException("No ExecutorFactory found to execute the application.");
        }

        return compatibleFactories.get(0);
    }

    /**
     * 返回所有可用的执行器名字
     * @return
     */
    @Override
    public Stream<String> getExecutorNames() {

        // 通过spi机制加载
        final ServiceLoader<PipelineExecutorFactory> loader =
                ServiceLoader.load(PipelineExecutorFactory.class);

        // 调用工厂api 获取执行器名字
        return StreamSupport.stream(loader.spliterator(), false)
                .map(PipelineExecutorFactory::getName);
    }
}
