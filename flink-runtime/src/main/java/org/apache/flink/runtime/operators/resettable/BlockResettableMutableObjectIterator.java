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

package org.apache.flink.runtime.operators.resettable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.util.ResettableMutableObjectIterator;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implementation of an iterator that fetches a block of data into main memory and offers resettable
 * access to the data in that block.
 * 实现了ResettableMutableObjectIterator  可以迭代元素 还可以重置
 */
public class BlockResettableMutableObjectIterator<T> extends AbstractBlockResettableIterator<T>
        implements ResettableMutableObjectIterator<T> {
    public static final Logger LOG =
            LoggerFactory.getLogger(BlockResettableMutableObjectIterator.class);

    // ------------------------------------------------------------------------

    private final MutableObjectIterator<T> input;

    /**
     * 默认是false 也就是一开始没有数据可读
     */
    private boolean readPhase;

    private boolean leftOverReturned;

    /**
     * 表示buffer写满了
     */
    private boolean fullWriteBuffer;

    /**
     * 表示input的数据已经被读完
     */
    private boolean noMoreBlocks;

    private T leftOverRecord;

    // ------------------------------------------------------------------------

    public BlockResettableMutableObjectIterator(
            MemoryManager memoryManager,
            MutableObjectIterator<T> input,
            TypeSerializer<T> serializer,
            int numMemoryPages,
            AbstractInvokable ownerTask)
            throws MemoryAllocationException {
        super(serializer, memoryManager, numMemoryPages, ownerTask);

        this.input = input;
        this.leftOverRecord = serializer.createInstance();
        this.leftOverReturned = true;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 开始迭代数据
     * @param target
     * @return
     * @throws IOException
     */
    @Override
    public T next(T target) throws IOException {
        // check for the left over element
        if (this.readPhase) {
            return getNextRecord(target);
        } else {
            // writing phase. check for leftover first
            // 默认一开始在写阶段
            // leftOverReturned 为true 表示 leftOverRecord的数据已经被使用
            if (this.leftOverReturned) {
                // get next record
                // 从迭代器读取数据
                if ((target = this.input.next(target)) != null) {
                    // 并写入output中
                    if (writeNextRecord(target)) {
                        return target;
                    } else {
                        // did not fit into memory, keep as leftover
                        // 写入失败 内存不够了  使用 leftOverRecord 暂存结果
                        this.leftOverRecord = this.serializer.copy(target, this.leftOverRecord);
                        this.leftOverReturned = false;
                        this.fullWriteBuffer = true;
                        return null;
                    }
                } else {
                    //
                    this.noMoreBlocks = true;
                    return null;
                }
                // 此时buffer还是满的状态
            } else if (this.fullWriteBuffer) {
                return null;
            } else {
                // 使用 leftOverRecord 的数据
                this.leftOverReturned = true;
                target = this.serializer.copy(this.leftOverRecord, target);
                return target;
            }
        }
    }

    @Override
    public T next() throws IOException {
        // check for the left over element
        if (this.readPhase) {
            return getNextRecord();
        } else {
            // writing phase. check for leftover first
            T result = null;
            if (this.leftOverReturned) {
                // get next record
                if ((result = this.input.next()) != null) {
                    if (writeNextRecord(result)) {
                        return result;
                    } else {
                        // did not fit into memory, keep as leftover
                        this.leftOverRecord = this.serializer.copy(result);
                        this.leftOverReturned = false;
                        this.fullWriteBuffer = true;
                        return null;
                    }
                } else {
                    this.noMoreBlocks = true;
                    return null;
                }
            } else if (this.fullWriteBuffer) {
                return null;
            } else {
                this.leftOverReturned = true;
                return this.leftOverRecord;
            }
        }
    }

    public void reset() {
        // a reset always goes to the read phase
        this.readPhase = true;
        super.reset();
    }

    /**
     * 切换到下一个
     * @return
     * @throws IOException
     */
    @Override
    public boolean nextBlock() throws IOException {
        // check the state
        if (this.closed) {
            throw new IllegalStateException("Iterator has been closed.");
        }

        // check whether more blocks are available
        // noMoreBlocks  表示input已经没有数据了  不需要再切换block了
        if (this.noMoreBlocks) {
            return false;
        }

        // reset the views in the superclass
        // 切换到下个block  要重新使用之前的segment
        super.nextBlock();

        // if there is no leftover record, get a record such that we guarantee to advance
        if (this.leftOverReturned || !this.fullWriteBuffer) {
            if ((this.leftOverRecord = this.input.next(this.leftOverRecord)) != null) {
                this.leftOverReturned = false;
            } else {
                // 此时没数据了
                this.noMoreBlocks = true;
                this.fullWriteBuffer = true;
                this.readPhase = false;
                return false;
            }
        }

        // write the leftover record
        if (!writeNextRecord(this.leftOverRecord)) {
            throw new IOException(
                    "BlockResettableIterator could not serialize record into fresh memory block: "
                            + "Record is too large.");
        }
        // 因为开启了一个新的block 并且处于数据填充阶段    目前就不适合读取了
        this.readPhase = false;
        this.fullWriteBuffer = false;

        return true;
    }

    /**
     * Checks, whether the input that is blocked by this iterator, has further elements available.
     * This method may be used to forecast (for example at the point where a block is full) whether
     * there will be more data (possibly in another block).
     *
     * @return True, if there will be more data, false otherwise.
     * 表示还有数据没读取出来
     */
    public boolean hasFurtherInput() {
        return !this.noMoreBlocks;
    }

    public void close() {
        // suggest that we are in the read phase. because nothing is in the current block,
        // read requests will fail
        this.readPhase = true;
        super.close();
    }
}
