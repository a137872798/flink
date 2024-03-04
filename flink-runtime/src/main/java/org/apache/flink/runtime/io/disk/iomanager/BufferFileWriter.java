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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.util.event.NotificationListener;

import java.io.IOException;

/**
 * 该写入对象还可以注册监听器
 */
public interface BufferFileWriter extends BlockChannelWriterWithCallback<Buffer> {

    /** Returns the number of outstanding requests.
     * 返回囤积的写请求
     * */
    int getNumberOfOutstandingRequests();

    /**
     * Registers a listener, which is notified after all outstanding requests have been processed.
     * 注册监听器 只有当所有请求处理完时 才触发
     */
    boolean registerAllRequestsProcessedListener(NotificationListener listener) throws IOException;
}
