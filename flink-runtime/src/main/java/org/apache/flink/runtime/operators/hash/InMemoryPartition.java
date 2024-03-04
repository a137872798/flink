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

package org.apache.flink.runtime.operators.hash;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.core.memory.SeekableDataInputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.runtime.memory.ListMemorySegmentSource;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * In-memory partition with overflow buckets for {@link CompactingHashTable}
 *
 * @param <T> record type
 *           这个对象是辅助CompactingHashTable使用的
 *           在内存中存储分区数据
 */
public class InMemoryPartition<T> {

    // --------------------------------- Table Structure Auxiliaries
    // ------------------------------------

    protected MemorySegment[]
            overflowSegments; // segments in which overflow buckets from the table structure are
    // stored

    protected int
            numOverflowSegments; // the number of actual segments in the overflowSegments array

    protected int nextOverflowBucket; // the next free bucket in the current overflow segment

    // -------------------------------------  Type Accessors
    // --------------------------------------------

    private final TypeSerializer<T> serializer;

    // -------------------------------------- Record Buffers
    // --------------------------------------------

    private final ArrayList<MemorySegment> partitionPages;

    /**
     * 通过该对象提供内存块
     */
    private final ListMemorySegmentSource availableMemory;

    private WriteView writeView;

    private ReadView readView;

    private long recordCounter; // number of records in this partition including garbage

    // ----------------------------------------- General
    // ------------------------------------------------

    private int partitionNumber; // the number of the partition

    private boolean compacted; // overwritten records since allocation or last full compaction

    private int pageSize; // segment size in bytes

    private int pageSizeInBits;

    // --------------------------------------------------------------------------------------------------

    /**
     * Creates a new partition, in memory, with one buffer.
     *
     * @param serializer Serializer for T.  通过该对象序列化要存储的数据
     * @param partitionNumber The number of the partition.  每个分区有一个编号
     * @param memSource memory pool  通过该对象提供内存块
     * @param pageSize segment size in bytes   单个segment的大小
     * @param pageSizeInBits
     */
    public InMemoryPartition(
            TypeSerializer<T> serializer,
            int partitionNumber,
            ListMemorySegmentSource memSource,
            int pageSize,
            int pageSizeInBits) {
        this.overflowSegments = new MemorySegment[2];
        this.numOverflowSegments = 0;
        this.nextOverflowBucket = 0;

        this.serializer = serializer;
        // 表示每个分区使用了多少数据页
        this.partitionPages = new ArrayList<MemorySegment>(64);
        this.availableMemory = memSource;

        this.partitionNumber = partitionNumber;

        // add the first segment
        // 初始化时 先设置一页
        this.partitionPages.add(memSource.nextSegment());
        // empty partitions have no garbage
        // 此时没数据 假设已经压缩过了
        this.compacted = true;

        this.pageSize = pageSize;

        this.pageSizeInBits = pageSizeInBits;

        // 2个视图分别用于读写page中的数据
        this.writeView = new WriteView(this.partitionPages, memSource, pageSize, pageSizeInBits);
        this.readView = new ReadView(this.partitionPages, pageSize, pageSizeInBits);
    }

    // --------------------------------------------------------------------------------------------------

    /**
     * Gets the partition number of this partition.
     *
     * @return This partition's number.
     */
    public int getPartitionNumber() {
        return this.partitionNumber;
    }

    /**
     * overwrites partition number and should only be used on compaction partition
     *
     * @param number new partition
     */
    public void setPartitionNumber(int number) {
        this.partitionNumber = number;
    }

    /** @return number of segments owned by partition
     * 该分区总计有多少page
     * */
    public int getBlockCount() {
        return this.partitionPages.size();
    }

    /**
     * number of records in partition including garbage
     *
     * @return number record count
     * 一个page中有多条记录  这里返回总记录数
     */
    public long getRecordCount() {
        return this.recordCounter;
    }

    /** sets record counter to zero and should only be used on compaction partition */
    public void resetRecordCounter() {
        this.recordCounter = 0L;
    }

    /** resets read and write views and should only be used on compaction partition */
    public void resetRWViews() {
        this.writeView.resetTo(0L);
        this.readView.setReadPosition(0L);
    }

    /**
     * 更新视图
     */
    public void pushDownPages() {
        this.writeView =
                new WriteView(this.partitionPages, availableMemory, pageSize, pageSizeInBits);
        this.readView = new ReadView(this.partitionPages, pageSize, pageSizeInBits);
    }

    /**
     * resets overflow bucket counters and returns freed memory and should only be used for resizing
     *
     * @return freed memory segments
     * 重置溢出的bucket
     */
    public ArrayList<MemorySegment> resetOverflowBuckets() {
        this.numOverflowSegments = 0;
        this.nextOverflowBucket = 0;

        ArrayList<MemorySegment> result =
                new ArrayList<MemorySegment>(this.overflowSegments.length);
        for (int i = 0; i < this.overflowSegments.length; i++) {
            if (this.overflowSegments[i] != null) {
                result.add(this.overflowSegments[i]);
            }
        }
        this.overflowSegments = new MemorySegment[2];
        // 将旧数据取出
        return result;
    }

    /** @return true if garbage exists in partition */
    public boolean isCompacted() {
        return this.compacted;
    }

    /**
     * sets compaction status (should only be set <code>true</code> directly after compaction and
     * <code>false</code> when garbage was created)
     *
     * @param compacted compaction status
     */
    public void setIsCompacted(boolean compacted) {
        this.compacted = compacted;
    }

    // --------------------------------------------------------------------------------------------------

    /**
     * Inserts the given object into the current buffer. This method returns a pointer that can be
     * used to address the written record in this partition.
     *
     * @param record The object to be written to the partition.
     * @return A pointer to the object in the partition.
     * @throws IOException Thrown when the write failed.
     * 添加一条记录
     */
    public final long appendRecord(T record) throws IOException {
        long pointer = this.writeView.getCurrentPointer();
        try {
            // 借助序列化对象写入output  同时返回原位置
            this.serializer.serialize(record, this.writeView);
            this.recordCounter++;
            return pointer;
        } catch (EOFException e) {
            // we ran out of pages.
            // first, reset the pages and then we need to trigger a compaction
            // int oldCurrentBuffer =
            this.writeView.resetTo(pointer);
            // for (int bufNum = this.partitionPages.size() - 1; bufNum > oldCurrentBuffer;
            // bufNum--) {
            //	this.availableMemory.addMemorySegment(this.partitionPages.remove(bufNum));
            // }
            throw e;
        }
    }

    /**
     * 先通过指针定位 后读取数据
     * @param pointer
     * @param reuse
     * @return
     * @throws IOException
     */
    public T readRecordAt(long pointer, T reuse) throws IOException {
        this.readView.setReadPosition(pointer);
        return this.serializer.deserialize(reuse, this.readView);
    }

    public T readRecordAt(long pointer) throws IOException {
        this.readView.setReadPosition(pointer);
        return this.serializer.deserialize(this.readView);
    }

    /**
     * UNSAFE!! overwrites record causes inconsistency or data loss for overwriting everything but
     * records of the exact same size
     *
     * @param pointer pointer to start of record
     * @param record record to overwrite old one with
     * @throws IOException
     * @deprecated Don't use this, overwrites record and causes inconsistency or data loss for
     *     overwriting everything but records of the exact same size
     */
    @Deprecated
    public void overwriteRecordAt(long pointer, T record) throws IOException {
        long tmpPointer = this.writeView.getCurrentPointer();
        this.writeView.resetTo(pointer);
        this.serializer.serialize(record, this.writeView);
        this.writeView.resetTo(tmpPointer);
    }

    /**
     * releases all of the partition's segments (pages and overflow buckets)
     *
     * @param target memory pool to release segments to
     *               将现有数据加入target后返回
     */
    public void clearAllMemory(List<MemorySegment> target) {
        // return the overflow segments
        if (this.overflowSegments != null) {
            for (int k = 0; k < this.numOverflowSegments; k++) {
                target.add(this.overflowSegments[k]);
            }
        }
        // return the partition buffers
        target.addAll(this.partitionPages);
        this.partitionPages.clear();
    }

    /**
     * attempts to allocate specified number of segments and should only be used by compaction
     * partition fails silently if not enough segments are available since next compaction could
     * still succeed
     *
     * @param numberOfSegments allocation count
     *                         申请直到pool中有足够的segment
     */
    public void allocateSegments(int numberOfSegments) {
        while (getBlockCount() < numberOfSegments) {
            MemorySegment next = this.availableMemory.nextSegment();
            if (next != null) {
                this.partitionPages.add(next);
            } else {
                return;
            }
        }
    }

    @Override
    public String toString() {
        return String.format(
                "Partition %d - %d records, %d partition blocks, %d bucket overflow blocks",
                getPartitionNumber(), getRecordCount(), getBlockCount(), this.numOverflowSegments);
    }

    // ============================================================================================

    /**
     * 借助该对象写入数据
     */
    private static final class WriteView extends AbstractPagedOutputView {

        /**
         * 存储内存块的列表
         */
        private final ArrayList<MemorySegment> pages;

        /**
         * 通过该对象继续申请数据
         */
        private final MemorySegmentSource memSource;

        private final int sizeBits;

        private final int sizeMask;

        /**
         * 当前segment
         */
        private int currentPageNumber;

        /**
         * 对segment做一层偏移  可以得到不同的segment
         */
        private int segmentNumberOffset;

        private WriteView(
                ArrayList<MemorySegment> pages,
                MemorySegmentSource memSource,
                int pageSize,
                int pageSizeBits) {
            super(pages.get(0), pageSize, 0);

            this.pages = pages;
            this.memSource = memSource;
            this.sizeBits = pageSizeBits;
            this.sizeMask = pageSize - 1;
            this.segmentNumberOffset = 0;
        }

        @Override
        protected MemorySegment nextSegment(MemorySegment current, int bytesUsed)
                throws IOException {
            MemorySegment next = this.memSource.nextSegment();
            if (next == null) {
                throw new EOFException();
            }
            this.pages.add(next);

            // 切换了page  所以推进 currentPageNumber
            this.currentPageNumber++;
            return next;
        }

        /**
         * 获取总偏移量  而不是单个segment上的偏移量
         * @return
         */
        private long getCurrentPointer() {
            return (((long) this.currentPageNumber) << this.sizeBits)
                    + getCurrentPositionInSegment();
        }

        /**
         * 跳跃到指定位置
         * @param pointer
         * @return
         */
        private int resetTo(long pointer) {
            final int pageNum = (int) (pointer >>> this.sizeBits);
            final int offset = (int) (pointer & this.sizeMask);

            this.currentPageNumber = pageNum;

            // 通过segmentNumberOffset 修正访问的segment
            int posInArray = pageNum - this.segmentNumberOffset;
            seekOutput(this.pages.get(posInArray), offset);

            return posInArray;
        }

        /**
         * 设置偏移量
         * @param offset
         */
        @SuppressWarnings("unused")
        public void setSegmentNumberOffset(int offset) {
            this.segmentNumberOffset = offset;
        }
    }

    /**
     * 该对象提供访问数据的接口
     */
    private static final class ReadView extends AbstractPagedInputView
            implements SeekableDataInputView {

        private final ArrayList<MemorySegment> segments;

        private final int segmentSizeBits;

        private final int segmentSizeMask;

        private int currentSegmentIndex;

        /**
         * 用于修正segment
         */
        private int segmentNumberOffset;

        public ReadView(ArrayList<MemorySegment> segments, int segmentSize, int segmentSizeBits) {
            super(segments.get(0), segmentSize, 0);

            if ((segmentSize & (segmentSize - 1)) != 0) {
                throw new IllegalArgumentException("Segment size must be a power of 2!");
            }

            this.segments = segments;
            this.segmentSizeBits = segmentSizeBits;
            this.segmentSizeMask = segmentSize - 1;
            this.segmentNumberOffset = 0;
        }

        @Override
        protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
            if (++this.currentSegmentIndex < this.segments.size()) {
                return this.segments.get(this.currentSegmentIndex);
            } else {
                throw new EOFException();
            }
        }

        /**
         * 返回一个segment的大小
         * @param segment The segment to determine the limit for.
         * @return
         */
        @Override
        protected int getLimitForSegment(MemorySegment segment) {
            return this.segmentSizeMask + 1;
        }

        /**
         * 设置读指针
         * @param position The new read position.
         */
        @Override
        public void setReadPosition(long position) {
            // 通过offset修正后 计算出正确的下标
            final int bufferNum =
                    ((int) (position >>> this.segmentSizeBits)) - this.segmentNumberOffset;
            final int offset = (int) (position & this.segmentSizeMask);

            this.currentSegmentIndex = bufferNum;
            seekInput(this.segments.get(bufferNum), offset, this.segmentSizeMask + 1);
        }

        @SuppressWarnings("unused")
        public void setSegmentNumberOffset(int offset) {
            this.segmentNumberOffset = offset;
        }
    }
}
