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

package org.apache.flink.api.common.operators.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.InstantiationUtil;

import java.lang.annotation.Annotation;

/** This holds a class containing user defined code.
 * 代表用户定义的类
 * */
@Internal
public class UserCodeClassWrapper<T> implements UserCodeWrapper<T> {
    private static final long serialVersionUID = 1L;

    /**
     * 用户定义的类
     */
    private Class<? extends T> userCodeClass;

    public UserCodeClassWrapper(Class<? extends T> userCodeClass) {
        this.userCodeClass = userCodeClass;
    }

    /**
     * 使用指定类加载器 进行实例话
     * @param superClass  入参是本类的父类
     * @param cl
     * @return
     */
    @Override
    public T getUserCodeObject(Class<? super T> superClass, ClassLoader cl) {
        return InstantiationUtil.instantiate(userCodeClass, superClass);
    }

    @Override
    public T getUserCodeObject() {
        return InstantiationUtil.instantiate(userCodeClass, Object.class);
    }

    /**
     * 获取类上的某个注解
     * @param annotationClass the Class object corresponding to the annotation type
     * @param <A>
     * @return
     */
    @Override
    public <A extends Annotation> A getUserCodeAnnotation(Class<A> annotationClass) {
        return userCodeClass.getAnnotation(annotationClass);
    }

    @Override
    public Class<? extends T> getUserCodeClass() {
        return userCodeClass;
    }

    @Override
    public boolean hasObject() {
        return false;
    }
}
