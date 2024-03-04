/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

/**
 * Interface which denotes that {@link State} can react to newly available resource (slots) and
 * changes in resource requirements.
 * 资源监听器   配合WaitingForResources使用  在进行执行图创建前 需要先申请资源
 */
interface ResourceListener {

    /** Notifies that new resources are available.
     * 表示有资源可用了
     * */
    void onNewResourcesAvailable();

    /** Notifies that the resource requirements have changed.
     * 表示对资源的需求发生变化
     * */
    void onNewResourceRequirements();
}
