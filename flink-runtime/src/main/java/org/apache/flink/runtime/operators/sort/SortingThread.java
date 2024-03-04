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

package org.apache.flink.runtime.operators.sort;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.runtime.operators.sort.CircularElement.EOF_MARKER;
import static org.apache.flink.runtime.operators.sort.CircularElement.SPILLING_MARKER;

/** The thread that sorts filled buffers.
 * 该线程对数据进行排序
 * */
class SortingThread<E> extends ThreadBase<E> {

    /** Logging. */
    private static final Logger LOG = LoggerFactory.getLogger(SortingThread.class);

    private final IndexedSorter sorter;

    /**
     * Creates a new sorting thread.
     *
     * @param exceptionHandler The exception handler to call for all exceptions.
     * @param dispatcher The queues used to pass buffers between the threads.
     */
    public SortingThread(
            @Nullable ExceptionHandler<IOException> exceptionHandler,
            StageMessageDispatcher<E> dispatcher) {
        super(exceptionHandler, "SortMerger sorting thread", dispatcher);

        // members
        this.sorter = new QuickSort();
    }

    /** Entry point of the thread. */
    @Override
    public void go() throws InterruptedException {
        boolean alive = true;

        // loop as long as the thread is marked alive
        while (isRunning() && alive) {
            // 从排序队列获取元素   sort队列中的元素都是由SorterInputGateway设置的  而SorterInputGateway的数据则是 ReadingThread从某个input中读取的
            final CircularElement<E> element = this.dispatcher.take(SortStage.SORT);

            if (element != EOF_MARKER && element != SPILLING_MARKER) {

                // 归还内存块
                if (element.getBuffer().size() == 0) {
                    element.getBuffer().reset();
                    this.dispatcher.send(SortStage.READ, element);
                    continue;
                }

                LOG.debug("Sorting buffer {}.", element.getId());
                // 对内存块进行排序
                this.sorter.sort(element.getBuffer());

                LOG.debug("Sorted buffer {}.", element.getId());
            } else if (element == EOF_MARKER) {
                // 表示上层数据已经发送完了 该线程就不需要再工作了
                LOG.debug("Sorting thread done.");
                alive = false;
            }
            // 排序后的数据才方便写入
            this.dispatcher.send(SortStage.SPILL, element);
        }
    }
}
