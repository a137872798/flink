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

package org.apache.flink.core.memory;

import java.io.IOException;
import java.util.Collection;

/** The provider used for requesting and releasing batch of memory segments.
 * 可以产生内存块
 * */
public interface MemorySegmentProvider {

    /**
     * 请求一定数量的内存块
     * @param numberOfSegmentsToRequest
     * @return
     * @throws IOException
     */
    Collection<MemorySegment> requestUnpooledMemorySegments(int numberOfSegmentsToRequest)
            throws IOException;

    /**
     * 归还内存块 便于循环利用
     * @param segments
     * @throws IOException
     */
    void recycleUnpooledMemorySegments(Collection<MemorySegment> segments) throws IOException;
}
