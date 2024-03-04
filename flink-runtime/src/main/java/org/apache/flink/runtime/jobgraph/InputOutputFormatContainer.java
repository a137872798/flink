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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A container for {@link InputFormat InputFormats} and {@link OutputFormat OutputFormats}, along
 * with their {@link Configuration}.
 * 存储输入输出格式的容器
 */
public class InputOutputFormatContainer {

    /**
     * 该对象就体现了容器的职能 内部分别维护了 input/output format对象
     */
    private final FormatUserCodeTable formats;

    private final Configuration parameters;

    private final ClassLoader userCodeClassLoader;

    public InputOutputFormatContainer(ClassLoader classLoader) {
        this.formats = new FormatUserCodeTable();
        this.parameters = new Configuration();
        this.userCodeClassLoader = checkNotNull(classLoader);
    }

    /**
     *
     * @param config  里面包含了各种配置
     * @param classLoader
     */
    public InputOutputFormatContainer(TaskConfig config, ClassLoader classLoader) {
        checkNotNull(config);
        this.userCodeClassLoader = checkNotNull(classLoader);

        // 反序列化出某个对象
        final UserCodeWrapper<FormatUserCodeTable> wrapper;

        try {
            wrapper = config.getStubWrapper(classLoader);
        } catch (Throwable t) {
            throw new RuntimeException(
                    "Deserializing the input/output formats failed: " + t.getMessage(), t);
        }

        if (wrapper == null) {
            throw new RuntimeException(
                    "No InputFormat or OutputFormat present in task configuration.");
        }

        try {
            // 实例化 容器对象
            this.formats = wrapper.getUserCodeObject(FormatUserCodeTable.class, classLoader);
        } catch (Throwable t) {
            throw new RuntimeException(
                    "Instantiating the input/output formats failed: " + t.getMessage(), t);
        }

        this.parameters = new Configuration();
        Configuration stubParameters = config.getStubParameters();

        // 将具有指定前缀的配置读取出来
        for (String key :
                stubParameters.keySet()) { // copy only the parameters of input/output formats
            parameters.setString(key, stubParameters.getString(key, null));
        }
    }

    public Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> getInputFormats() {
        return formats.getInputFormats();
    }

    public Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> getOutputFormats() {
        return formats.getOutputFormats();
    }

    /**
     * 获取唯一的输出格式对象
     * @param <OT>
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public <OT, T extends InputSplit> Pair<OperatorID, InputFormat<OT, T>> getUniqueInputFormat() {
        Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats =
                formats.getInputFormats();
        // 确保长度为1
        Preconditions.checkState(inputFormats.size() == 1);

        Map.Entry<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> entry =
                inputFormats.entrySet().iterator().next();

        // 实例化formatter对象 并生成pair
        return new ImmutablePair<>(
                entry.getKey(),
                (InputFormat<OT, T>)
                        entry.getValue().getUserCodeObject(InputFormat.class, userCodeClassLoader));
    }

    /**
     * 同上
     * @param <IT>
     * @return
     */
    @SuppressWarnings("unchecked")
    public <IT> Pair<OperatorID, OutputFormat<IT>> getUniqueOutputFormat() {
        Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats =
                formats.getOutputFormats();
        Preconditions.checkState(outputFormats.size() == 1);

        Map.Entry<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> entry =
                outputFormats.entrySet().iterator().next();

        return new ImmutablePair<>(
                entry.getKey(),
                (OutputFormat<IT>)
                        entry.getValue()
                                .getUserCodeObject(OutputFormat.class, userCodeClassLoader));
    }

    public InputOutputFormatContainer addInputFormat(
            OperatorID operatorId, InputFormat<?, ?> inputFormat) {
        formats.addInputFormat(operatorId, new UserCodeObjectWrapper<>(inputFormat));
        return this;
    }

    public InputOutputFormatContainer addInputFormat(
            OperatorID operatorId, UserCodeWrapper<? extends InputFormat<?, ?>> wrapper) {
        formats.addInputFormat(operatorId, wrapper);
        return this;
    }

    public InputOutputFormatContainer addOutputFormat(
            OperatorID operatorId, OutputFormat<?> outputFormat) {
        formats.addOutputFormat(operatorId, new UserCodeObjectWrapper<>(outputFormat));
        return this;
    }

    public InputOutputFormatContainer addOutputFormat(
            OperatorID operatorId, UserCodeWrapper<? extends OutputFormat<?>> wrapper) {
        formats.addOutputFormat(operatorId, wrapper);
        return this;
    }

    /**
     * 包装后的config对象 仅会返回该operatorid相关的数据
     * @param operatorId
     * @return
     */
    public Configuration getParameters(OperatorID operatorId) {
        return new DelegatingConfiguration(parameters, getParamKeyPrefix(operatorId));
    }

    public InputOutputFormatContainer addParameters(
            OperatorID operatorId, Configuration parameters) {
        for (String key : parameters.keySet()) {
            addParameters(operatorId, key, parameters.getString(key, null));
        }
        return this;
    }

    public InputOutputFormatContainer addParameters(
            OperatorID operatorId, String key, String value) {
        parameters.setString(getParamKeyPrefix(operatorId) + key, value);
        return this;
    }

    /**
     * 将format信息 以及参数信息写入config
     * @param config
     */
    public void write(TaskConfig config) {
        config.setStubWrapper(new UserCodeObjectWrapper<>(formats));
        config.setStubParameters(parameters);
    }

    private String getParamKeyPrefix(OperatorID operatorId) {
        return operatorId + ".";
    }

    /**
     * Container for multiple wrappers containing {@link InputFormat} and {@link OutputFormat} code.
     *
     * 表示一个容器对象  用于存储输入/输出格式对象
     */
    public static class FormatUserCodeTable implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats;
        private final Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats;

        public FormatUserCodeTable() {
            this.inputFormats = new HashMap<>();
            this.outputFormats = new HashMap<>();
        }

        /**
         * 每个format对象 会对应一个operatorId
         * @param operatorId
         * @param wrapper
         */
        public void addInputFormat(
                OperatorID operatorId, UserCodeWrapper<? extends InputFormat<?, ?>> wrapper) {
            if (inputFormats.containsKey(checkNotNull(operatorId))) {
                throw new IllegalStateException(
                        "The input format has been set for the operator: " + operatorId);
            }

            inputFormats.put(operatorId, checkNotNull(wrapper));
        }

        public void addOutputFormat(
                OperatorID operatorId, UserCodeWrapper<? extends OutputFormat<?>> wrapper) {
            if (outputFormats.containsKey(checkNotNull(operatorId))) {
                throw new IllegalStateException(
                        "The output format has been set for the operator: " + operatorId);
            }

            outputFormats.put(operatorId, checkNotNull(wrapper));
        }

        public Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> getInputFormats() {
            return Collections.unmodifiableMap(inputFormats);
        }

        public Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> getOutputFormats() {
            return Collections.unmodifiableMap(outputFormats);
        }
    }
}
