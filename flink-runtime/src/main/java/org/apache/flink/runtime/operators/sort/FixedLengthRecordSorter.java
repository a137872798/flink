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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 长度固定的容器  这里并没有直接体现出排序逻辑 只是提供了相关api  也就是内部的数据 还是无序的 需要外部手动调整顺序
 * */
public final class FixedLengthRecordSorter<T> implements InMemorySorter<T> {

    private static final int MIN_REQUIRED_BUFFERS = 3;

    // ------------------------------------------------------------------------
    //                               Members
    // ------------------------------------------------------------------------

    /**
     * 用于交换数据时 存储临时数据
     */
    private final byte[] swapBuffer;

    private final TypeSerializer<T> serializer;

    /**
     * 该对象提供T类型的比较逻辑
     */
    private final TypeComparator<T> comparator;

    // 使用下面2个对象读写数据

    private final SingleSegmentOutputView outView;

    private final SingleSegmentInputView inView;

    /**
     * 当前正在写入的segment
     */
    private MemorySegment currentSortBufferSegment;

    /**
     * 表示当前seg的偏移量
     */
    private int currentSortBufferOffset;

    private final ArrayList<MemorySegment> freeMemory;

    private final ArrayList<MemorySegment> sortBuffer;

    private long sortBufferBytes;

    private int numRecords;

    private final int numKeyBytes;

    private final int recordSize;

    /**
     * 每个segment 容纳多少条记录
     */
    private final int recordsPerSegment;

    private final int lastEntryOffset;

    /**
     * 单个seg的大小
     */
    private final int segmentSize;

    /**
     * segment数量
     */
    private final int totalNumBuffers;

    private final boolean useNormKeyUninverted;

    private final T recordInstance;

    // -------------------------------------------------------------------------
    // Constructors / Destructors
    // -------------------------------------------------------------------------

    /**
     *
     * @param serializer
     * @param comparator
     * @param memory  使用一组固定长度的seg初始化
     */
    public FixedLengthRecordSorter(
            TypeSerializer<T> serializer,
            TypeComparator<T> comparator,
            List<MemorySegment> memory) {
        if (serializer == null || comparator == null || memory == null) {
            throw new NullPointerException();
        }

        this.serializer = serializer;
        this.comparator = comparator;
        this.useNormKeyUninverted = !comparator.invertNormalizedKey();

        // check the size of the first buffer and record it. all further buffers must have the same
        // size.
        // the size must also be a power of 2
        this.totalNumBuffers = memory.size();
        if (this.totalNumBuffers < MIN_REQUIRED_BUFFERS) {
            throw new IllegalArgumentException(
                    "Normalized-Key sorter requires at least "
                            + MIN_REQUIRED_BUFFERS
                            + " memory buffers.");
        }

        //
        this.segmentSize = memory.get(0).size();
        // 一个实例化对象的长度
        this.recordSize = serializer.getLength();
        // 获取可以用于比较的key的长度
        this.numKeyBytes = this.comparator.getNormalizeKeyLen();

        // check that the serializer and comparator allow our operations
        if (this.recordSize <= 0) {
            throw new IllegalArgumentException(
                    "This sorter works only for fixed-length data types.");
        } else if (this.recordSize > this.segmentSize) {
            throw new IllegalArgumentException(
                    "This sorter works only for record lengths below the memory segment size.");
        } else if (!comparator.supportsSerializationWithKeyNormalization()) {
            throw new IllegalArgumentException(
                    "This sorter requires a comparator that supports serialization with key normalization.");
        }

        // compute the entry size and limits
        this.recordsPerSegment = segmentSize / this.recordSize;
        // 最后一个实例的偏移量 是计算出来的
        this.lastEntryOffset = (this.recordsPerSegment - 1) * this.recordSize;

        // 用于存储临时数据的buffer
        this.swapBuffer = new byte[this.recordSize];

        this.freeMemory = new ArrayList<MemorySegment>(memory);

        // create the buffer collections
        this.sortBuffer = new ArrayList<MemorySegment>(16);
        this.outView = new SingleSegmentOutputView(this.segmentSize);
        this.inView = new SingleSegmentInputView(this.lastEntryOffset + this.recordSize);
        // 从free队列中移除segment
        this.currentSortBufferSegment = nextMemorySegment();
        this.sortBuffer.add(this.currentSortBufferSegment);
        this.outView.set(this.currentSortBufferSegment);

        // 实例化对象
        this.recordInstance = this.serializer.createInstance();
    }

    @Override
    public int recordSize() {
        return recordSize;
    }

    @Override
    public int recordsPerSegment() {
        return recordsPerSegment;
    }

    // -------------------------------------------------------------------------
    // Memory Segment
    // -------------------------------------------------------------------------

    /**
     * Resets the sort buffer back to the state where it is empty. All contained data is discarded.
     * 将内部字段重置
     */
    @Override
    public void reset() {
        // reset all offsets
        this.numRecords = 0;
        this.currentSortBufferOffset = 0;
        this.sortBufferBytes = 0;

        // return all memory
        this.freeMemory.addAll(this.sortBuffer);
        this.sortBuffer.clear();

        // grab first buffers
        this.currentSortBufferSegment = nextMemorySegment();
        this.sortBuffer.add(this.currentSortBufferSegment);
        this.outView.set(this.currentSortBufferSegment);
    }

    /**
     * Checks whether the buffer is empty.
     *
     * @return True, if no record is contained, false otherwise.
     */
    @Override
    public boolean isEmpty() {
        return this.numRecords == 0;
    }

    @Override
    public void dispose() {
        this.freeMemory.clear();
        this.sortBuffer.clear();
    }

    @Override
    public long getCapacity() {
        return ((long) this.totalNumBuffers) * this.segmentSize;
    }

    @Override
    public long getOccupancy() {
        return this.sortBufferBytes;
    }

    // -------------------------------------------------------------------------
    // Retrieving and Writing
    // -------------------------------------------------------------------------

    @Override
    public T getRecord(int logicalPosition) throws IOException {
        return getRecord(serializer.createInstance(), logicalPosition);
    }


    /**
     *
     * @param reuse The reuse object to deserialize the record into.  这是一个空对象
     * @param logicalPosition The logical position of the record.   逻辑偏移量 需要处理后才能知道对应的segment 以及segment内偏移量
     * @return
     * @throws IOException
     */
    @Override
    public T getRecord(T reuse, int logicalPosition) throws IOException {
        final int buffer = logicalPosition / this.recordsPerSegment;
        // 每个逻辑偏移量转换成物理偏移量 要 * recordSize
        final int inBuffer = (logicalPosition % this.recordsPerSegment) * this.recordSize;
        // 借助该对象读取数据
        this.inView.set(this.sortBuffer.get(buffer), inBuffer);
        // 使用inView的数据来填充 reuse 对象
        return this.comparator.readWithKeyDenormalization(reuse, this.inView);
    }

    /**
     * Writes a given record to this sort buffer. The written record will be appended and take the
     * last logical position.
     *
     * @param record The record to be written.
     * @return True, if the record was successfully written, false, if the sort buffer was full.
     * @throws IOException Thrown, if an error occurred while serializing the record into the
     *     buffers.
     *     写入一条记录
     */
    @Override
    public boolean write(T record) throws IOException {
        // check whether we need a new memory segment for the sort index
        // 表示需要切换到下个seg了
        if (this.currentSortBufferOffset > this.lastEntryOffset) {
            if (memoryAvailable()) {
                this.currentSortBufferSegment = nextMemorySegment();
                this.sortBuffer.add(this.currentSortBufferSegment);
                this.outView.set(this.currentSortBufferSegment);
                this.currentSortBufferOffset = 0;
                this.sortBufferBytes += this.segmentSize;
            } else {
                // 此时已经没有seg可以使用了  一开始指定了本对象的总长度
                return false;
            }
        }

        // serialize the record into the data buffers
        try {
            // 将record的数据写入 outView 与上面相反
            this.comparator.writeWithKeyNormalization(record, this.outView);
            this.numRecords++;
            this.currentSortBufferOffset += this.recordSize;
            return true;
        } catch (EOFException eofex) {
            throw new IOException(
                    "Error: Serialization consumes more bytes than announced by the serializer.");
        }
    }

    // ------------------------------------------------------------------------
    //                           Access Utilities
    // ------------------------------------------------------------------------

    private boolean memoryAvailable() {
        return !this.freeMemory.isEmpty();
    }

    private MemorySegment nextMemorySegment() {
        return this.freeMemory.remove(this.freeMemory.size() - 1);
    }

    // -------------------------------------------------------------------------
    // Sorting
    // -------------------------------------------------------------------------

    @Override
    public int compare(int i, int j) {
        final int segmentNumberI = i / this.recordsPerSegment;
        final int segmentOffsetI = (i % this.recordsPerSegment) * this.recordSize;

        final int segmentNumberJ = j / this.recordsPerSegment;
        final int segmentOffsetJ = (j % this.recordsPerSegment) * this.recordSize;

        return compare(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    /**
     * 比较2个位置对应的数据大小
     * @param segmentNumberI index of memory segment containing first record
     * @param segmentOffsetI offset into memory segment containing first record
     * @param segmentNumberJ index of memory segment containing second record
     * @param segmentOffsetJ offset into memory segment containing second record
     * @return
     */
    @Override
    public int compare(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortBuffer.get(segmentNumberI);
        final MemorySegment segJ = this.sortBuffer.get(segmentNumberJ);

        // 只比较key
        int val = segI.compare(segJ, segmentOffsetI, segmentOffsetJ, this.numKeyBytes);
        return this.useNormKeyUninverted ? val : -val;
    }

    @Override
    public void swap(int i, int j) {
        final int segmentNumberI = i / this.recordsPerSegment;
        final int segmentOffsetI = (i % this.recordsPerSegment) * this.recordSize;

        final int segmentNumberJ = j / this.recordsPerSegment;
        final int segmentOffsetJ = (j % this.recordsPerSegment) * this.recordSize;

        swap(segmentNumberI, segmentOffsetI, segmentNumberJ, segmentOffsetJ);
    }

    /**
     * 交换2个位置的数据
     * @param segmentNumberI index of memory segment containing first record
     * @param segmentOffsetI offset into memory segment containing first record
     * @param segmentNumberJ index of memory segment containing second record
     * @param segmentOffsetJ offset into memory segment containing second record
     */
    @Override
    public void swap(
            int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ) {
        final MemorySegment segI = this.sortBuffer.get(segmentNumberI);
        final MemorySegment segJ = this.sortBuffer.get(segmentNumberJ);

        segI.swapBytes(this.swapBuffer, segJ, segmentOffsetI, segmentOffsetJ, this.recordSize);
    }

    @Override
    public int size() {
        return this.numRecords;
    }

    // -------------------------------------------------------------------------

    /**
     * Gets an iterator over all records in this buffer in their logical order.
     *
     * @return An iterator returning the records in their logical order.
     * 获取迭代器 用于遍历内部数据
     */
    @Override
    public final MutableObjectIterator<T> getIterator() {
        final SingleSegmentInputView startIn =
                new SingleSegmentInputView(this.recordsPerSegment * this.recordSize);

        // 从第一个buffer开始
        startIn.set(this.sortBuffer.get(0), 0);

        return new MutableObjectIterator<T>() {

            private final SingleSegmentInputView in = startIn;
            private final TypeComparator<T> comp = comparator;

            /**
             * 记录总数
             */
            private final int numTotal = size();
            private final int numPerSegment = recordsPerSegment;

            private int currentTotal = 0;
            private int currentInSegment = 0;
            private int currentSegmentIndex = 0;

            @Override
            public T next(T reuse) {
                if (this.currentTotal < this.numTotal) {

                    if (this.currentInSegment >= this.numPerSegment) {
                        this.currentInSegment = 0;
                        this.currentSegmentIndex++;
                        // 更新 inView
                        this.in.set(sortBuffer.get(this.currentSegmentIndex), 0);
                    }

                    this.currentTotal++;
                    this.currentInSegment++;

                    try {
                        return this.comp.readWithKeyDenormalization(reuse, this.in);
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                } else {
                    return null;
                }
            }

            @Override
            public T next() {
                if (this.currentTotal < this.numTotal) {

                    if (this.currentInSegment >= this.numPerSegment) {
                        this.currentInSegment = 0;
                        this.currentSegmentIndex++;
                        this.in.set(sortBuffer.get(this.currentSegmentIndex), 0);
                    }

                    this.currentTotal++;
                    this.currentInSegment++;

                    try {
                        // 每次创建新的实例对象
                        return this.comp.readWithKeyDenormalization(
                                serializer.createInstance(), this.in);
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                } else {
                    return null;
                }
            }
        };
    }

    // ------------------------------------------------------------------------
    //                Writing to a DataOutputView
    // ------------------------------------------------------------------------

    /**
     * Writes the records in this buffer in their logical order to the given output.
     *
     * @param output The output view to write the records to.
     * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
     * 将内部数据写到output
     */
    @Override
    public void writeToOutput(final ChannelWriterOutputView output) throws IOException {
        final TypeComparator<T> comparator = this.comparator;
        final TypeSerializer<T> serializer = this.serializer;
        T record = this.recordInstance;

        final SingleSegmentInputView inView = this.inView;

        final int recordsPerSegment = this.recordsPerSegment;
        int recordsLeft = this.numRecords;
        int currentMemSeg = 0;

        while (recordsLeft > 0) {
            // 每次定位一个segment
            final MemorySegment currentIndexSegment = this.sortBuffer.get(currentMemSeg++);
            inView.set(currentIndexSegment, 0);

            // check whether we have a full or partially full segment
            // 表示这个段被写满了
            if (recordsLeft >= recordsPerSegment) {
                // full segment     将所有数据序列化后写入output
                for (int numInMemSeg = 0; numInMemSeg < recordsPerSegment; numInMemSeg++) {
                    record = comparator.readWithKeyDenormalization(record, inView);
                    serializer.serialize(record, output);
                }
                recordsLeft -= recordsPerSegment;
            } else {
                // partially filled segment
                // 剩余部分数据 序列化后写入
                for (; recordsLeft > 0; recordsLeft--) {
                    record = comparator.readWithKeyDenormalization(record, inView);
                    serializer.serialize(record, output);
                }
            }
        }
    }

    @Override
    public void writeToOutput(
            ChannelWriterOutputView output, LargeRecordHandler<T> largeRecordsOutput)
            throws IOException {
        writeToOutput(output);
    }

    /**
     * Writes a subset of the records in this buffer in their logical order to the given output.
     *
     * @param output The output view to write the records to.
     * @param start The logical start position of the subset.
     * @param num The number of elements to write.
     * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
     */
    @Override
    public void writeToOutput(final ChannelWriterOutputView output, final int start, int num)
            throws IOException {
        final TypeComparator<T> comparator = this.comparator;
        final TypeSerializer<T> serializer = this.serializer;
        T record = this.recordInstance;

        final SingleSegmentInputView inView = this.inView;

        final int recordsPerSegment = this.recordsPerSegment;

        // 找到起始段和偏移量
        int currentMemSeg = start / recordsPerSegment;
        int offset = (start % recordsPerSegment) * this.recordSize;

        while (num > 0) {
            final MemorySegment currentIndexSegment = this.sortBuffer.get(currentMemSeg++);
            inView.set(currentIndexSegment, offset);

            // check whether we have a full or partially full segment
            // 满足完整段的条件
            if (num >= recordsPerSegment && offset == 0) {
                // full segment
                for (int numInMemSeg = 0; numInMemSeg < recordsPerSegment; numInMemSeg++) {
                    record = comparator.readWithKeyDenormalization(record, inView);
                    serializer.serialize(record, output);
                }
                num -= recordsPerSegment;
            } else {
                // partially filled segment
                for (;
                        num > 0 && offset <= this.lastEntryOffset;
                        num--, offset += this.recordSize) {
                    record = comparator.readWithKeyDenormalization(record, inView);
                    serializer.serialize(record, output);
                }
            }

            offset = 0;
        }
    }

    /**
     * 表示该对象中仅包含一个seg
     */
    private static final class SingleSegmentOutputView extends AbstractPagedOutputView {

        SingleSegmentOutputView(int segmentSize) {
            super(segmentSize, 0);
        }

        /**
         * 允许直接替换内部的seg
         * @param segment
         */
        void set(MemorySegment segment) {
            seekOutput(segment, 0);
        }

        /**
         * 该对象无法调用 nextSegment
         * @param current The current memory segment
         * @param positionInCurrent The position in the segment, one after the last valid byte.
         * @return
         * @throws IOException
         */
        @Override
        protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
                throws IOException {
            throw new EOFException();
        }
    }

    /**
     * 内部只有一个segment
     */
    private static final class SingleSegmentInputView extends AbstractPagedInputView {

        private final int limit;

        SingleSegmentInputView(int limit) {
            super(0);
            this.limit = limit;
        }

        protected void set(MemorySegment segment, int offset) {
            seekInput(segment, offset, this.limit);
        }

        @Override
        protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
            throw new EOFException();
        }

        @Override
        protected int getLimitForSegment(MemorySegment segment) {
            return this.limit;
        }
    }
}
