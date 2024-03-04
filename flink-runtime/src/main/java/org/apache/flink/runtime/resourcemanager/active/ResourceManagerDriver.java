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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.runtime.blocklist.BlockedNodeRetriever;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A {@link ResourceManagerDriver} is responsible for requesting and releasing resources from/to a
 * particular external resource manager.
 * 资源管理器驱动 驱动就是用来和不同组件交互的
 */
public interface ResourceManagerDriver<WorkerType extends ResourceIDRetrievable> {

    /**
     * Initialize the deployment specific components.
     *
     * @param resourceEventHandler Handler that handles resource events.
     * @param mainThreadExecutor Rpc main thread executor.
     * @param ioExecutor IO executor.
     * @param blockedNodeRetriever To retrieve all blocked nodes  通过该对象可以找到某些被阻塞的节点   在调度模块我们看到 将execution耗时长的节点认为是阻塞节点
     *                             进行初始化工作
     */
    void initialize(
            ResourceEventHandler<WorkerType> resourceEventHandler,
            ScheduledExecutor mainThreadExecutor,
            Executor ioExecutor,
            BlockedNodeRetriever blockedNodeRetriever)
            throws Exception;

    /** Terminate the deployment specific components.
     * 终止该组件
     * */
    void terminate() throws Exception;

    /**
     * The deployment specific code to deregister the application. This should report the
     * application's final status.
     *
     * <p>This method also needs to make sure all pending containers that are not registered yet are
     * returned.
     *
     * @param finalStatus The application status to report.
     * @param optionalDiagnostics A diagnostics message or {@code null}.  提供的诊断消息
     * @throws Exception if the application could not be deregistered.
     *
     * 注销部署的应用
     */
    void deregisterApplication(ApplicationStatus finalStatus, @Nullable String optionalDiagnostics)
            throws Exception;

    /**
     * Request resource from the external resource manager.
     *
     * <p>This method request a new resource from the external resource manager, and tries to launch
     * a task manager inside the allocated resource, with respect to the provided
     * taskExecutorProcessSpec. The returned future will be completed with a worker node in the
     * deployment specific type, or exceptionally if the allocation has failed.
     *
     * <p>Note: The returned future could be cancelled by ResourceManager. This means
     * ResourceManager don't need this resource anymore, Driver should try to cancel this request
     * from the external resource manager.
     *
     * <p>Note: Completion of the returned future does not necessarily mean the success of resource
     * allocation and task manager launching. Allocation and launching failures can still happen
     * after the future completion. In such cases, {@link ResourceEventHandler#onWorkerTerminated}
     * will be called.
     *
     * <p>The future is guaranteed to be completed in the rpc main thread, before trying to launch
     * the task manager, thus before the task manager registration. It is also guaranteed that
     * {@link ResourceEventHandler#onWorkerTerminated} will not be called on the requested worker,
     * until the returned future is completed successfully.
     *
     * @param taskExecutorProcessSpec Resource specification of the requested worker.
     * @return Future that wraps worker node of the requested resource, in the deployment specific
     *     type.
     *     根据描述信息请求资源
     */
    CompletableFuture<WorkerType> requestResource(TaskExecutorProcessSpec taskExecutorProcessSpec);

    /**
     * Release resource to the external resource manager.
     *
     * @param worker Worker node to be released, in the deployment specific type.
     *               将worker持有的资源归还给外部资源管理器
     */
    void releaseResource(WorkerType worker);
}
