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

package org.apache.flink.runtime.io.network.buffer;

import java.io.IOException;

/** A dynamically sized buffer pool.
 * 通过池来管理内存块 提供/回收
 * */
public interface BufferPool extends BufferProvider, BufferRecycler {

    /**
     * Reserves the target number of segments to this pool. Will throw an exception if it can not
     * allocate enough segments.
     * 表示一次性分配多个内存块
     */
    void reserveSegments(int numberOfSegmentsToReserve) throws IOException;

    /**
     * Destroys this buffer pool.
     *
     * <p>If not all buffers are available, they are recycled lazily as soon as they are recycled.
     * 延迟销毁
     */
    void lazyDestroy();

    /** Checks whether this buffer pool has been destroyed. */
    @Override
    boolean isDestroyed();

    /** Returns the number of guaranteed (minimum number of) memory segments of this buffer pool.
     * 获取当前内存块数
     * */
    int getNumberOfRequiredMemorySegments();

    /**
     * Returns the maximum number of memory segments this buffer pool should use.
     *
     * @return maximum number of memory segments to use or <tt>-1</tt> if unlimited
     * 获取pool的最大容量
     */
    int getMaxNumberOfMemorySegments();

    /**
     * Returns the current size of this buffer pool.
     *
     * <p>The size of the buffer pool can change dynamically at runtime.
     * 获取pool内的buffer数量
     */
    int getNumBuffers();

    /**
     * Sets the current size of this buffer pool.
     *
     * <p>The size needs to be greater or equal to the guaranteed number of memory segments.
     */
    void setNumBuffers(int numBuffers);

    /** Sets the max overdraft buffer size of per gate. */
    void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate);

    /** Returns the max overdraft buffer size of per gate. */
    int getMaxOverdraftBuffersPerGate();

    /** Returns the number memory segments, which are currently held by this buffer pool. */
    int getNumberOfAvailableMemorySegments();

    /** Returns the number of used buffers of this buffer pool. */
    int bestEffortGetNumOfUsedBuffers();
}
