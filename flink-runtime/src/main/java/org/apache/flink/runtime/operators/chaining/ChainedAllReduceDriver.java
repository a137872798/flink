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
package org.apache.flink.runtime.operators.chaining;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.BatchTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实现了一些关键方法
 * all reduce 就体现在 只有在close时才将合并的最终结果发往下游
 * @param <IT>
 */
public class ChainedAllReduceDriver<IT> extends ChainedDriver<IT, IT> {
    private static final Logger LOG = LoggerFactory.getLogger(ChainedAllReduceDriver.class);

    // --------------------------------------------------------------------------------------------

    /**
     * 将2个值合并成1个
     */
    private ReduceFunction<IT> reducer;
    /**
     * IT相关的序列化对象
     */
    private TypeSerializer<IT> serializer;

    private IT base;

    // --------------------------------------------------------------------------------------------
    @Override
    public void setup(AbstractInvokable parent) {

        // 从配置中读取了用户定义函数
        final ReduceFunction<IT> red =
                BatchTask.instantiateUserCode(
                        this.config, userCodeClassLoader, ReduceFunction.class);
        this.reducer = red;

        // 如果udf是 RichFunction 就设置上下文
        FunctionUtils.setFunctionRuntimeContext(red, getUdfRuntimeContext());

        // 获取序列化对象   每个task有自己的config对象  config中会存储正确的序列化对象/函数
        TypeSerializerFactory<IT> serializerFactory =
                this.config.getInputSerializer(0, userCodeClassLoader);
        this.serializer = serializerFactory.getSerializer();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "ChainedAllReduceDriver object reuse: "
                            + (this.objectReuseEnabled ? "ENABLED" : "DISABLED")
                            + ".");
        }
    }

    // 以下3个函数  当fun是 RichFunction时 分别触发相关钩子

    @Override
    public void openTask() throws Exception {
        Configuration stubConfig = this.config.getStubParameters();
        BatchTask.openUserCode(this.reducer, stubConfig);
    }

    @Override
    public void closeTask() throws Exception {
        BatchTask.closeUserCode(this.reducer);
    }

    @Override
    public void cancelTask() {
        try {
            FunctionUtils.closeFunction(this.reducer);
        } catch (Throwable t) {
            // Ignore exception.
        }
    }

    // --------------------------------------------------------------------------------------------
    @Override
    public Function getStub() {
        return this.reducer;
    }

    @Override
    public String getTaskName() {
        return this.taskName;
    }

    // --------------------------------------------------------------------------------------------

    // 每当触发一次采集时  就是通过reduce函数处理数据
    @Override
    public void collect(IT record) {
        numRecordsIn.inc();
        try {
            // 第一条记录将作为初始值
            if (base == null) {
                base = serializer.copy(record);
            } else {
                base =
                        objectReuseEnabled
                                ? reducer.reduce(base, record)
                                : serializer.copy(reducer.reduce(base, record));
            }
        } catch (Exception e) {
            throw new ExceptionInChainedStubException(taskName, e);
        }
    }

    @Override
    public void close() {
        try {
            if (base != null) {
                // 在关闭时才写出
                this.outputCollector.collect(base);
                base = null;
            }
        } catch (Exception e) {
            throw new ExceptionInChainedStubException(this.taskName, e);
        }
        this.outputCollector.close();
    }
}
