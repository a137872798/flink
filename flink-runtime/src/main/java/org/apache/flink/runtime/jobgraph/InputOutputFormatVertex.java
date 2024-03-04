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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.FinalizeOnMaster.FinalizationContext;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.operators.util.TaskConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A task vertex that runs an initialization and a finalization on the master. If necessary, it
 * tries to deserialize input and output formats, and initialize and finalize them on master.
 *
 */
public class InputOutputFormatVertex extends JobVertex {

    private static final long serialVersionUID = 1L;

    private final Map<OperatorID, String> formatDescriptions = new HashMap<>();

    public InputOutputFormatVertex(String name) {
        super(name);
    }

    public InputOutputFormatVertex(
            String name, JobVertexID id, List<OperatorIDPair> operatorIDPairs) {
        super(name, id, operatorIDPairs);
    }

    /**
     * 当任务开始时触发
     * @param context Provides contextual information for the initialization
     * @throws Exception
     */
    @Override
    public void initializeOnMaster(InitializeOnMasterContext context) throws Exception {
        ClassLoader loader = context.getClassLoader();

        // 产生存储输入输出格式的容器
        final InputOutputFormatContainer formatContainer = initInputOutputformatContainer(loader);

        final ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            // set user classloader before calling user code
            Thread.currentThread().setContextClassLoader(loader);

            // configure the input format and setup input splits
            // 获取所有的inputFormat
            Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats =
                    formatContainer.getInputFormats();
            // 这里期望只有一个输入
            if (inputFormats.size() > 1) {
                throw new UnsupportedOperationException(
                        "Multiple input formats are not supported in a job vertex.");
            }
            for (Map.Entry<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> entry :
                    inputFormats.entrySet()) {
                final InputFormat<?, ?> inputFormat;

                try {
                    inputFormat = entry.getValue().getUserCodeObject();
                    // 使用operator相关的配置进行处理
                    inputFormat.configure(formatContainer.getParameters(entry.getKey()));
                } catch (Throwable t) {
                    throw new Exception(
                            "Configuring the input format ("
                                    + getFormatDescription(entry.getKey())
                                    + ") failed: "
                                    + t.getMessage(),
                            t);
                }

                // inputFormatter 作为 输出源  可以产生多个InputSplit
                setInputSplitSource(inputFormat);
            }

            // configure output formats and invoke initializeGlobal()
            // 对output数量没有限制
            Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats =
                    formatContainer.getOutputFormats();
            for (Map.Entry<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> entry :
                    outputFormats.entrySet()) {
                final OutputFormat<?> outputFormat;

                try {
                    outputFormat = entry.getValue().getUserCodeObject();
                    outputFormat.configure(formatContainer.getParameters(entry.getKey()));
                } catch (Throwable t) {
                    throw new Exception(
                            "Configuring the output format ("
                                    + getFormatDescription(entry.getKey())
                                    + ") failed: "
                                    + t.getMessage(),
                            t);
                }

                // 需要触发钩子
                if (outputFormat instanceof InitializeOnMaster) {
                    int executionParallelism = context.getExecutionParallelism();
                    ((InitializeOnMaster) outputFormat).initializeGlobal(executionParallelism);
                }
            }
        } finally {
            // restore original classloader
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    /**
     * 进行清理工作
     * @param context Provides contextual information for the initialization
     * @throws Exception
     */
    @Override
    public void finalizeOnMaster(FinalizeOnMasterContext context) throws Exception {

        // 一样是初始化容器  容器只是临时使用的
        final ClassLoader loader = context.getClassLoader();
        final InputOutputFormatContainer formatContainer = initInputOutputformatContainer(loader);

        final ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            // set user classloader before calling user code
            Thread.currentThread().setContextClassLoader(loader);

            // configure output formats and invoke finalizeGlobal()
            Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats =
                    formatContainer.getOutputFormats();
            for (Map.Entry<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> entry :
                    outputFormats.entrySet()) {
                final OutputFormat<?> outputFormat;

                try {
                    outputFormat = entry.getValue().getUserCodeObject();
                    outputFormat.configure(formatContainer.getParameters(entry.getKey()));
                } catch (Throwable t) {
                    throw new Exception(
                            "Configuring the output format ("
                                    + getFormatDescription(entry.getKey())
                                    + ") failed: "
                                    + t.getMessage(),
                            t);
                }

                // 主要是针对outputFormatter    触发相关钩子
                if (outputFormat instanceof FinalizeOnMaster) {
                    int executionParallelism = context.getExecutionParallelism();
                    ((FinalizeOnMaster) outputFormat)
                            .finalizeGlobal(
                                    // 相当于把上下文参数传递过去
                                    new FinalizationContext() {
                                        @Override
                                        public int getParallelism() {
                                            return executionParallelism;
                                        }

                                        @Override
                                        public int getFinishedAttempt(int subtaskIndex) {
                                            return context.getFinishedAttempt(subtaskIndex);
                                        }
                                    });
                }
            }
        } finally {
            // restore original classloader
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    public String getFormatDescription(OperatorID operatorID) {
        return formatDescriptions.get(operatorID);
    }

    public void setFormatDescription(OperatorID operatorID, String formatDescription) {
        formatDescriptions.put(checkNotNull(operatorID), formatDescription);
    }

    private InputOutputFormatContainer initInputOutputformatContainer(ClassLoader classLoader)
            throws Exception {
        try {
            return new InputOutputFormatContainer(new TaskConfig(getConfiguration()), classLoader);
        } catch (Throwable t) {
            throw new Exception(
                    "Loading the input/output formats failed: "
                            + String.join(",", formatDescriptions.values()),
                    t);
        }
    }
}
