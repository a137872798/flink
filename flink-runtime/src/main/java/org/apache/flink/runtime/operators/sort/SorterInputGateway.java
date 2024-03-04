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

import org.apache.flink.runtime.operators.sort.StageRunner.SortStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A gateway for writing records into the sort/merge process.
 * 这是一个网关对象  ReadingThread 负责从input读取数据 并写入该对象
 * 简单来说本对象所做的事 就是写入数据 写满buffer后产生一个sort事件  同时当写入数据量足够多时产生一个spill事件
 * */
final class SorterInputGateway<E> {
    /** Logging. */
    private static final Logger LOG = LoggerFactory.getLogger(SorterInputGateway.class);

    private final LargeRecordHandler<E> largeRecords;
    /** The object into which the thread reads the data from the input.
     * 该对象存储数据
     * */
    private final StageRunner.StageMessageDispatcher<E> dispatcher;

    private long bytesUntilSpilling;
    /**
     * 当前数据
     */
    private CircularElement<E> currentBuffer;

    /**
     * Creates a new gateway for pushing records into the sorter.
     *
     * @param dispatcher The queues used to pass buffers between the threads.
     */
    SorterInputGateway(
            StageRunner.StageMessageDispatcher<E> dispatcher,
            @Nullable LargeRecordHandler<E> largeRecordsHandler,
            long startSpillingBytes) {

        // members
        this.bytesUntilSpilling = startSpillingBytes;
        this.largeRecords = largeRecordsHandler;
        this.dispatcher = checkNotNull(dispatcher);

        if (bytesUntilSpilling < 1) {
            this.dispatcher.send(SortStage.SORT, CircularElement.spillingMarker());
        }
    }

    /** Writes the given record for sorting.
     * 往网关写入数据
     * */
    public void writeRecord(E record) throws IOException, InterruptedException {

        if (currentBuffer == null) {
            // READ取出来的应当是空的
            this.currentBuffer = this.dispatcher.take(SortStage.READ);
            if (!currentBuffer.getBuffer().isEmpty()) {
                throw new IOException("New buffer is not empty.");
            }
        }

        InMemorySorter<E> sorter = currentBuffer.getBuffer();

        // 写入前的大小
        long occupancyPreWrite = sorter.getOccupancy();
        // 将read线程读取出来的数据写入sorter对象
        if (!sorter.write(record)) {
            // false就表示没有内存可用了
            long recordSize = sorter.getCapacity() - occupancyPreWrite;
            signalSpillingIfNecessary(recordSize);
            // 表示数据非常大
            boolean isLarge = occupancyPreWrite == 0;
            if (isLarge) {
                // did not fit in a fresh buffer, must be large...
                writeLarge(record, sorter);
                // occupancyPreWrite == 0已经确保buffer是空的了  所以此时写入失败也许会有残留数据 直接reset
                this.currentBuffer.getBuffer().reset();
            } else {
                // 表示此时buffer被写满了  发送一个sort事件 并重试本方法
                this.dispatcher.send(SortStage.SORT, currentBuffer);
                this.currentBuffer = null;
                writeRecord(record);
            }
        } else {
            long recordSize = sorter.getOccupancy() - occupancyPreWrite;
            // 通知下游 准备倾泻  同时数据存储在currentBuffer内
            signalSpillingIfNecessary(recordSize);
        }
    }

    /** Signals the end of input. Will flush all buffers and notify later stages.
     * Read线程写完数据后触发该方法
     * */
    public void finishReading() {

        // 将残留数据添加到队列中 准备排序
        if (currentBuffer != null && !currentBuffer.getBuffer().isEmpty()) {
            this.dispatcher.send(SortStage.SORT, currentBuffer);
        }

        // add the sentinel to notify the receivers that the work is done
        // send the EOF marker
        final CircularElement<E> EOF_MARKER = CircularElement.endMarker();
        // 产生一个数据已经消费完的标记
        this.dispatcher.send(SortStage.SORT, EOF_MARKER);
        LOG.debug("Reading thread done.");
    }

    private void writeLarge(E record, InMemorySorter<E> sorter) throws IOException {
        if (this.largeRecords != null) {
            LOG.debug(
                    "Large record did not fit into a fresh sort buffer. Putting into large record store.");
            this.largeRecords.addRecord(record);
        } else {
            throw new IOException(
                    "The record exceeds the maximum size of a sort buffer (current maximum: "
                            + sorter.getCapacity()
                            + " bytes).");
        }
    }

    /**
     * 每当写入一定量的数据后 产生一个spill事件 并被其他线程发现和处理
     * @param writtenSize
     */
    private void signalSpillingIfNecessary(long writtenSize) {
        if (bytesUntilSpilling <= 0) {
            return;
        }

        bytesUntilSpilling -= writtenSize;
        if (bytesUntilSpilling < 1) {
            // add the spilling marker
            this.dispatcher.send(SortStage.SORT, CircularElement.spillingMarker());
            bytesUntilSpilling = 0;
        }
    }
}
