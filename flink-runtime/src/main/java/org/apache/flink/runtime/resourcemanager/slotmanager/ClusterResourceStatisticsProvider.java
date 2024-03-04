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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;

/** Provides statistics of cluster resources.
 * 获取集群资源的统计信息
 * */
public interface ClusterResourceStatisticsProvider {

    /** Get total number of registered slots.
     * 获取当前注册的slot总数
     * */
    int getNumberRegisteredSlots();

    /** Get number of registered slots from the TaskManager with the given instance id.
     * 查看分配到某个TaskManager上的slot数量
     * */
    int getNumberRegisteredSlotsOf(InstanceID instanceId);

    /** Get total number of free slots.
     * 获取总的空闲slot数量
     * */
    int getNumberFreeSlots();

    /** Get number of free slots from the TaskManager with the given instance id.
     * 获取某个TaskManager的空闲slot数量
     * */
    int getNumberFreeSlotsOf(InstanceID instanceId);

    /** Get profile of total registered resources.
     * 获取此时注册的总资源
     * */
    ResourceProfile getRegisteredResource();

    /** Get profile of registered resources from the TaskManager with the given instance id. */
    ResourceProfile getRegisteredResourceOf(InstanceID instanceId);

    /** Get profile of total free resources. */
    ResourceProfile getFreeResource();

    /** Get profile of free resources from the TaskManager with the given instance id. */
    ResourceProfile getFreeResourceOf(InstanceID instanceId);

    /** Get profile of total pending resources. */
    ResourceProfile getPendingResource();
}
