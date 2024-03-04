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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.SameTypePairComparator;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This hash table supports updating elements. If the new element has the same size as the old
 * element, then the update is done in-place. Otherwise a hole is created at the place of the old
 * record, which will eventually be removed by a compaction.
 *
 * <p>The memory is divided into three areas: - Bucket area: they contain bucket heads: an 8 byte
 * pointer to the first link of a linked list in the record area - Record area: this contains the
 * actual data in linked list elements. A linked list element starts with an 8 byte pointer to the
 * next element, and then the record follows. - Staging area: This is a small, temporary storage
 * area for writing updated records. This is needed, because before serializing a record, there is
 * no way to know in advance how large will it be. Therefore, we can't serialize directly into the
 * record area when we are doing an update, because if it turns out to be larger than the old
 * record, then it would override some other record that happens to be after the old one in memory.
 * The solution is to serialize to the staging area first, and then copy it to the place of the
 * original if it has the same size, otherwise allocate a new linked list element at the end of the
 * record area, and mark the old one as abandoned. This creates "holes" in the record area, so
 * compactions are eventually needed.
 *
 * <p>Compaction happens by deleting everything in the bucket area, and then reinserting all
 * elements. The reinsertion happens by forgetting the structure (the linked lists) of the record
 * area, and reading it sequentially, and inserting all non-abandoned records, starting from the
 * beginning of the record area. Note, that insertions never override a record that hasn't been read
 * by the reinsertion sweep, because both the insertions and readings happen sequentially in the
 * record area, and the insertions obviously never overtake the reading sweep.
 *
 * <p>Note: we have to abandon the old linked list element even when the updated record has a
 * smaller size than the original, because otherwise we wouldn't know where the next record starts
 * during a reinsertion sweep.
 *
 * <p>The number of buckets depends on how large are the records. The serializer might be able to
 * tell us this, so in this case, we will calculate the number of buckets upfront, and won't do
 * resizes. If the serializer doesn't know the size, then we start with a small number of buckets,
 * and do resizes as more elements are inserted than the number of buckets.
 *
 * <p>The number of memory segments given to the staging area is usually one, because it just needs
 * to hold one record.
 *
 * <p>Note: For hashing, we couldn't just take the lower bits, but have to use a proper hash
 * function from MathUtils because of its avalanche property, so that changing only some high bits
 * of the original value won't leave the lower bits of the hash unaffected. This is because when
 * choosing the bucket for a record, we mask only the lower bits (see numBucketsMask). Lots of
 * collisions would occur when, for example, the original value that is hashed is some bitset, where
 * lots of different values that are different only in the higher bits will actually occur.
 *
 * 简单理解就是一个存储数据的hashTable  并支持使用reduce函数处理相同的记录  通过线性探测法解决hash冲突
 */
public class InPlaceMutableHashTable<T> extends AbstractMutableHashTable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(InPlaceMutableHashTable.class);

    /**
     * The minimum number of memory segments InPlaceMutableHashTable needs to be supplied with in
     * order to work.
     */
    private static final int MIN_NUM_MEMORY_SEGMENTS = 3;

    // Note: the following two constants can't be negative, because negative values are reserved for
    // storing the
    // negated size of the record, when it is abandoned (not part of any linked list).

    /** The last link in the linked lists will have this as next pointer. */
    private static final long END_OF_LIST = Long.MAX_VALUE;

    /**
     * This value means that prevElemPtr is "pointing to the bucket head", and not into the record
     * segments.
     */
    private static final long INVALID_PREV_POINTER = Long.MAX_VALUE - 1;

    private static final long RECORD_OFFSET_IN_LINK = 8;

    /**
     * This initially contains all the memory we have, and then segments are taken from it by
     * bucketSegments, recordArea, and stagingSegments.
     */
    private final ArrayList<MemorySegment> freeMemorySegments;

    /**
     * 总共有多少segment
     */
    private final int numAllMemorySegments;

    private final int segmentSize;

    /**
     * These will contain the bucket heads. The bucket heads are pointers to the linked lists
     * containing the actual records.
     * 用于存储bucket数据的segment
     */
    private MemorySegment[] bucketSegments;

    private static final int bucketSize = 8, bucketSizeBits = 3;

    /**
     * 这个也是桶的数量 其实跟 hashTable差不多
     */
    private int numBuckets;
    private int numBucketsMask;

    /**
     * 一个segment中有多少bucket  每个bucket的长度固定
     */
    private final int numBucketsPerSegment, numBucketsPerSegmentBits, numBucketsPerSegmentMask;

    /** The segments where the actual data is stored. */
    private final RecordArea recordArea;

    /** Segments for the staging area. (It should contain at most one record at all times.) */
    private final ArrayList<MemorySegment> stagingSegments;

    private final RandomAccessInputView stagingSegmentsInView;
    private final StagingOutputView stagingSegmentsOutView;

    private T reuse;

    /** This is the internal prober that insertOrReplaceRecord uses. */
    private final HashTableProber<T> prober;

    /** The number of elements currently held by the table. */
    private long numElements = 0;

    /**
     * The number of bytes wasted by updates that couldn't overwrite the old record due to size
     * change.
     */
    private long holes = 0;

    /**
     * If the serializer knows the size of the records, then we can calculate the optimal number of
     * buckets upfront, so we don't need resizes.
     */
    private boolean enableResize;

    /**
     * 使用序列化对象/比较器/一组内存对象 进行初始化
     * @param serializer
     * @param comparator
     * @param memory
     */
    public InPlaceMutableHashTable(
            TypeSerializer<T> serializer,
            TypeComparator<T> comparator,
            List<MemorySegment> memory) {
        super(serializer, comparator);
        this.numAllMemorySegments = memory.size();
        this.freeMemorySegments = new ArrayList<>(memory);

        // some sanity checks first   seg数量不能小于这个值
        if (freeMemorySegments.size() < MIN_NUM_MEMORY_SEGMENTS) {
            throw new IllegalArgumentException(
                    "Too few memory segments provided. InPlaceMutableHashTable needs at least "
                            + MIN_NUM_MEMORY_SEGMENTS
                            + " memory segments.");
        }

        // Get the size of the first memory segment and record it. All further buffers must have the
        // same size.
        // the size must also be a power of 2
        segmentSize = freeMemorySegments.get(0).size();
        if ((segmentSize & segmentSize - 1) != 0) {
            throw new IllegalArgumentException(
                    "Hash Table requires buffers whose size is a power of 2.");
        }

        this.numBucketsPerSegment = segmentSize / bucketSize;
        this.numBucketsPerSegmentBits = MathUtils.log2strict(this.numBucketsPerSegment);
        this.numBucketsPerSegmentMask = (1 << this.numBucketsPerSegmentBits) - 1;

        // area 会从freeMemorySegments 申请segment
        recordArea = new RecordArea(segmentSize);

        // 这2个in/out视图对象 关联同一个segment列表
        stagingSegments = new ArrayList<>();
        stagingSegments.add(forcedAllocateSegment());
        stagingSegmentsInView = new RandomAccessInputView(stagingSegments, segmentSize);
        stagingSegmentsOutView = new StagingOutputView(stagingSegments, segmentSize);

        // 生成探测器对象
        prober =
                new HashTableProber<>(
                        buildSideComparator, new SameTypePairComparator<>(buildSideComparator));

        enableResize = buildSideSerializer.getLength() == -1;
    }

    /**
     * Gets the total capacity of this hash table, in bytes.
     *
     * @return The hash table's total capacity.
     */
    public long getCapacity() {
        return numAllMemorySegments * (long) segmentSize;
    }

    /**
     * Gets the number of bytes currently occupied in this hash table.
     *
     * @return The number of bytes occupied.
     *
     * 表示当前已经使用了多少
     */
    public long getOccupancy() {
        return numAllMemorySegments * segmentSize - freeMemorySegments.size() * segmentSize;
    }

    /**
     *
     * @param numBucketSegments   根据一定规则计算了一个合适值 使用多少个segment来存储bucket数据
     */
    private void open(int numBucketSegments) {
        synchronized (stateLock) {
            if (!closed) {
                throw new IllegalStateException("currently not closed.");
            }
            closed = false;
        }

        // 分配存储bucket的segment  并做一些初始化的工作 (填充数据)
        allocateBucketSegments(numBucketSegments);

        stagingSegments.add(forcedAllocateSegment());

        // 创建一个便于复用的对象
        reuse = buildSideSerializer.createInstance();
    }

    /** Initialize the hash table */
    @Override
    public void open() {
        open(calcInitialNumBucketSegments());
    }

    /**
     * 关闭本对象
     */
    @Override
    public void close() {
        // make sure that we close only once
        synchronized (stateLock) {
            if (closed) {
                // We have to do this here, because the ctor already allocates a segment to the
                // record area and
                // the staging area, even before we are opened. So we might have segments to free,
                // even if we
                // are closed.
                recordArea.giveBackSegments();  // 将segment归还到内存池
                freeMemorySegments.addAll(stagingSegments);
                stagingSegments.clear();

                return;
            }
            closed = true;
        }

        LOG.debug("Closing InPlaceMutableHashTable and releasing resources.");

        // 释放存储bucket的segment
        releaseBucketSegments();

        // 归还segment
        recordArea.giveBackSegments();

        freeMemorySegments.addAll(stagingSegments);
        stagingSegments.clear();

        numElements = 0;
        holes = 0;
    }

    /**
     * 间接触发close
     */
    @Override
    public void abort() {
        LOG.debug("Aborting InPlaceMutableHashTable.");
        close();
    }

    @Override
    public List<MemorySegment> getFreeMemory() {
        if (!this.closed) {
            throw new IllegalStateException(
                    "Cannot return memory while InPlaceMutableHashTable is open.");
        }

        return freeMemorySegments;
    }

    /**
     * 计算一开始有多少bucket
     * @return
     */
    private int calcInitialNumBucketSegments() {
        // 单条记录的长度
        int recordLength = buildSideSerializer.getLength();
        double fraction; // fraction of memory to use for the buckets
        // 表示长度是不确定的
        if (recordLength == -1) {
            // We don't know the record length, so we start with a small number of buckets, and do
            // resizes if
            // necessary.
            // It seems that resizing is quite efficient, so we can err here on the too few bucket
            // segments side.
            // Even with small records, we lose only ~15% speed.
            fraction = 0.1;
        } else {
            // We know the record length, so we can find a good value for the number of buckets
            // right away, and
            // won't need any resizes later. (enableResize is false in this case, so no resizing
            // will happen.)
            // Reasoning behind the formula:
            // We are aiming for one bucket per record, and one bucket contains one 8 byte pointer.
            // The total
            // memory overhead of an element will be approximately 8+8 bytes, as the record in the
            // record area
            // is preceded by a pointer (for the linked list).
            fraction = 8.0 / (16 + recordLength);
        }

        // We make the number of buckets a power of 2 so that taking modulo is efficient.
        int ret =
                Math.max(1, MathUtils.roundDownToPowerOf2((int) (numAllMemorySegments * fraction)));

        // We can't handle more than Integer.MAX_VALUE buckets (eg. because hash functions return
        // int)
        if ((long) ret * numBucketsPerSegment > Integer.MAX_VALUE) {
            ret = MathUtils.roundDownToPowerOf2(Integer.MAX_VALUE / numBucketsPerSegment);
        }
        return ret;
    }

    /**
     *
     * @param numBucketSegments
     */
    private void allocateBucketSegments(int numBucketSegments) {
        if (numBucketSegments < 1) {
            throw new RuntimeException("Bug in InPlaceMutableHashTable");
        }

        bucketSegments = new MemorySegment[numBucketSegments];
        for (int i = 0; i < bucketSegments.length; i++) {
            // 根据数量来申请segment
            bucketSegments[i] = forcedAllocateSegment();
            // Init all pointers in all buckets to END_OF_LIST
            for (int j = 0; j < numBucketsPerSegment; j++) {
                // 提前在segment 各个bucket开始的位置 写入特殊标记
                bucketSegments[i].putLong(j << bucketSizeBits, END_OF_LIST);
            }
        }

        // 总的bucket数
        numBuckets = numBucketSegments * numBucketsPerSegment;
        numBucketsMask = (1 << MathUtils.log2strict(numBuckets)) - 1;
    }

    private void releaseBucketSegments() {
        freeMemorySegments.addAll(Arrays.asList(bucketSegments));
        bucketSegments = null;
    }

    /**
     * 申请一个新的段
     * @return
     */
    private MemorySegment allocateSegment() {
        int s = freeMemorySegments.size();
        if (s > 0) {
            return freeMemorySegments.remove(s - 1);
        } else {
            return null;
        }
    }

    private MemorySegment forcedAllocateSegment() {
        MemorySegment segment = allocateSegment();
        if (segment == null) {
            throw new RuntimeException(
                    "Bug in InPlaceMutableHashTable: A free segment should have been available.");
        }
        return segment;
    }

    /**
     * Searches the hash table for a record with the given key. If it is found, then it is
     * overridden with the specified record. Otherwise, the specified record is inserted.
     *
     * @param record The record to insert or to replace with.
     * @throws IOException (EOFException specifically, if memory ran out)
     *
     * 添加一条记录
     */
    @Override
    public void insertOrReplaceRecord(T record) throws IOException {
        if (closed) {
            return;
        }

        // 判断是新增还是替换
        T match = prober.getMatchFor(record, reuse);
        // 新增记录
        if (match == null) {
            prober.insertAfterNoMatch(record);
        } else {
            // 需要更新记录 这里采用的是替换的方式
            prober.updateMatch(record);
        }
    }

    /**
     * Inserts the given record into the hash table. Note: this method doesn't care about whether a
     * record with the same key is already present.
     *
     * @param record The record to insert.
     * @throws IOException (EOFException specifically, if memory ran out)
     * 插入一条新记录
     */
    @Override
    public void insert(T record) throws IOException {
        if (closed) {
            return;
        }

        final int hashCode = MathUtils.jenkinsHash(buildSideComparator.hash(record));
        final int bucket = hashCode & numBucketsMask;
        final int bucketSegmentIndex =
                bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
        final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
        final int bucketOffset =
                (bucket & numBucketsPerSegmentMask)
                        << bucketSizeBits; // offset of the bucket in the segment

        // 找到对应的指针
        final long firstPointer = bucketSegment.getLong(bucketOffset);

        try {
            // 追加在末尾
            final long newFirstPointer = recordArea.appendPointerAndRecord(firstPointer, record);
            bucketSegment.putLong(bucketOffset, newFirstPointer);
        } catch (EOFException ex) {
            compactOrThrow();
            insert(record);
            return;
        }

        numElements++;
        resizeTableIfNecessary();
    }

    /**
     * 判断是否需要调整大小
     * @throws IOException
     */
    private void resizeTableIfNecessary() throws IOException {

        // 表示要产生的冲突可能比较多  需要多次寻址
        if (enableResize && numElements > numBuckets) {

            // 更新后维护bucket的segment数量  涨了一倍
            final long newNumBucketSegments = 2L * bucketSegments.length;
            // Checks:
            // - we can't handle more than Integer.MAX_VALUE buckets
            // - don't take more memory than the free memory we have left
            // - the buckets shouldn't occupy more than half of all our memory
            //
            if (newNumBucketSegments * numBucketsPerSegment < Integer.MAX_VALUE
                    && newNumBucketSegments - bucketSegments.length < freeMemorySegments.size()   // 表示segment的数量足够
                    && newNumBucketSegments < numAllMemorySegments / 2) {  // 不推荐超过一般的segment
                // do the resize
                rebuild(newNumBucketSegments);
            }
        }
    }

    /**
     * Returns an iterator that can be used to iterate over all the elements in the table. WARNING:
     * Doing any other operation on the table invalidates the iterator! (Even using getMatchFor of a
     * prober!)
     *
     * @return the iterator
     */
    @Override
    public EntryIterator getEntryIterator() {
        return new EntryIterator();
    }

    public <PT> HashTableProber<PT> getProber(
            TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator) {
        return new HashTableProber<>(probeTypeComparator, pairComparator);
    }

    /**
     * This function reinitializes the bucket segments, reads all records from the record segments
     * (sequentially, without using the pointers or the buckets), and rebuilds the hash table.
     */
    private void rebuild() throws IOException {
        rebuild(bucketSegments.length);
    }

    /** Same as above, but the number of bucket segments of the new table can be specified.
     * 使用该大小重建 bucket
     * */
    private void rebuild(long newNumBucketSegments) throws IOException {
        // Get new bucket segments
        releaseBucketSegments();

        // 这里会更新numBucketsMask
        allocateBucketSegments((int) newNumBucketSegments);

        T record = buildSideSerializer.createInstance();
        try {
            // 通过该对象可以遍历 recordArea的数据
            EntryIterator iter = getEntryIterator();
            // 复原指针
            recordArea.resetAppendPosition();
            recordArea.setWritePosition(0);

            // 挨个读取记录
            while ((record = iter.next(record)) != null && !closed) {

                // 重新计算hash值 并根据现在的
                final int hashCode = MathUtils.jenkinsHash(buildSideComparator.hash(record));
                // 由于numBucketsMask的变化  bucket位置也发生了变化
                final int bucket = hashCode & numBucketsMask;
                final int bucketSegmentIndex =
                        bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
                final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];
                final int bucketOffset =
                        (bucket & numBucketsPerSegmentMask)
                                << bucketSizeBits; // offset of the bucket in the segment

                // 找到现在该bucket 指向的record位置
                final long firstPointer = bucketSegment.getLong(bucketOffset);

                long ptrToAppended = recordArea.noSeekAppendPointerAndRecord(firstPointer, record);
                // 将bucket与 在recordArea的位置关联起来
                bucketSegment.putLong(bucketOffset, ptrToAppended);
            }
            // 尝试将多余的 segment释放掉
            recordArea.freeSegmentsAfterAppendPosition();
            // 因为此时数据都是连续存储的   所以空洞数为0
            holes = 0;

        } catch (EOFException ex) {
            throw new RuntimeException(
                    "Bug in InPlaceMutableHashTable: we shouldn't get out of memory during a rebuild, "
                            + "because we aren't allocating any new memory.");
        }
    }

    /**
     * If there is wasted space (due to updated records not fitting in their old places), then do a
     * compaction. Else, throw EOFException to indicate that memory ran out.
     *
     * @throws IOException
     * 满足条件则进行 rebuild (其实也是在压缩)
     */
    private void compactOrThrow() throws IOException {
        if (holes > (double) recordArea.getTotalSize() * 0.05) {
            rebuild();
        } else {
            throw new EOFException(
                    "InPlaceMutableHashTable memory ran out. " + getMemoryConsumptionString());
        }
    }

    /** @return String containing a summary of the memory consumption for error messages */
    private String getMemoryConsumptionString() {
        return "InPlaceMutableHashTable memory stats:\n"
                + "Total memory:     "
                + numAllMemorySegments * segmentSize
                + "\n"
                + "Free memory:      "
                + freeMemorySegments.size() * segmentSize
                + "\n"
                + "Bucket area:      "
                + numBuckets * 8
                + "\n"
                + "Record area:      "
                + recordArea.getTotalSize()
                + "\n"
                + "Staging area:     "
                + stagingSegments.size() * segmentSize
                + "\n"
                + "Num of elements:  "
                + numElements
                + "\n"
                + "Holes total size: "
                + holes;
    }

    /**
     * This class encapsulates the memory segments that belong to the record area. It - can append a
     * record - can overwrite a record at an arbitrary position (WARNING: the new record must have
     * the same size as the old one) - can be rewritten by calling resetAppendPosition - takes
     * memory from InPlaceMutableHashTable.freeMemorySegments on append
     * 表示一块区域 内部有多少segment
     */
    private final class RecordArea {
        private final ArrayList<MemorySegment> segments = new ArrayList<>();

        private final RecordAreaOutputView outView;

        /**
         * 该对象在 io.disk中出现 内部也是一组segment 可以随意定位下标
         */
        private final RandomAccessInputView inView;

        private final int segmentSizeBits;
        private final int segmentSizeMask;

        private long appendPosition = 0;

        /**
         * 初始化时 指定segment的数量
         * @param segmentSize
         */
        public RecordArea(int segmentSize) {
            int segmentSizeBits = MathUtils.log2strict(segmentSize);

            if ((segmentSize & (segmentSize - 1)) != 0) {
                throw new IllegalArgumentException("Segment size must be a power of 2!");
            }

            this.segmentSizeBits = segmentSizeBits;
            this.segmentSizeMask = segmentSize - 1;

            outView = new RecordAreaOutputView(segmentSize);
            try {
                // 先补充一个segment
                addSegment();
            } catch (EOFException ex) {
                throw new RuntimeException(
                        "Bug in InPlaceMutableHashTable: we should have caught it earlier "
                                + "that we don't have enough segments.");
            }
            inView = new RandomAccessInputView(segments, segmentSize);
        }

        /**
         * 需要再添加一个segment
         * @throws EOFException
         */
        private void addSegment() throws EOFException {
            MemorySegment m = allocateSegment();
            if (m == null) {
                throw new EOFException();
            }
            segments.add(m);
        }

        /**
         * Moves all its memory segments to freeMemorySegments. Warning: this will leave the
         * RecordArea in an unwritable state: you have to call setWritePosition before writing
         * again.
         * 归还本对象占用的所有segment
         */
        public void giveBackSegments() {
            freeMemorySegments.addAll(segments);
            segments.clear();

            // 重置相关变量
            resetAppendPosition();
        }

        public long getTotalSize() {
            return segments.size() * (long) segmentSize;
        }

        // ----------------------- Output -----------------------

        /**
         * 将output定位到指定的位置
         * @param position
         * @throws EOFException
         */
        private void setWritePosition(long position) throws EOFException {
            if (position > appendPosition) {
                throw new IndexOutOfBoundsException();
            }

            // 找到对应位置
            final int segmentIndex = (int) (position >>> segmentSizeBits);
            final int offset = (int) (position & segmentSizeMask);

            // If position == appendPosition and the last buffer is full,
            // then we will be seeking to the beginning of a new segment
            if (segmentIndex == segments.size()) {
                addSegment();
            }

            outView.currentSegmentIndex = segmentIndex;
            outView.seekOutput(segments.get(segmentIndex), offset);
        }

        /**
         * Sets appendPosition and the write position to 0, so that appending starts overwriting
         * elements from the beginning. (This is used in rebuild.)
         *
         * <p>Note: if data was written to the area after the current appendPosition before a call
         * to resetAppendPosition, it should still be readable. To release the segments after the
         * current append position, call freeSegmentsAfterAppendPosition()
         * 重置 output的相关指标
         */
        public void resetAppendPosition() {
            appendPosition = 0;

            // this is just for safety (making sure that we fail immediately
            // if a write happens without calling setWritePosition)
            outView.currentSegmentIndex = -1;
            outView.seekOutput(null, -1);
        }

        /**
         * Releases the memory segments that are after the current append position. Note: The
         * situation that there are segments after the current append position can arise from a call
         * to resetAppendPosition().
         * 回收 appendPosition 之后的所有segment
         */
        public void freeSegmentsAfterAppendPosition() {
            // 找到当前在使用的segment下标
            final int appendSegmentIndex = (int) (appendPosition >>> segmentSizeBits);
            // 回收之后的segment
            while (segments.size() > appendSegmentIndex + 1 && !closed) {
                freeMemorySegments.add(segments.get(segments.size() - 1));
                segments.remove(segments.size() - 1);
            }
        }

        /**
         * Overwrites the long value at the specified position.
         *
         * @param pointer Points to the position to overwrite.
         * @param value The value to write.
         * @throws IOException
         * 将output定位到特定的位置后  写入value值
         */
        public void overwritePointerAt(long pointer, long value) throws IOException {
            setWritePosition(pointer);
            outView.writeLong(value);
        }

        /**
         * Overwrites a record at the specified position. The record is read from a DataInputView
         * (this will be the staging area). WARNING: The record must not be larger than the original
         * record.
         *
         * @param pointer Points to the position to overwrite.
         * @param input The DataInputView to read the record from
         * @param size The size of the record
         * @throws IOException
         * 定位到某个地方  然后将input后 size长度的数据写入
         */
        public void overwriteRecordAt(long pointer, DataInputView input, int size)
                throws IOException {
            setWritePosition(pointer);
            outView.write(input, size);
        }

        /**
         * Appends a pointer and a record. The record is read from a DataInputView (this will be the
         * staging area).
         *
         * @param pointer The pointer to write (Note: this is NOT the position to write to!)
         * @param input The DataInputView to read the record from
         * @param recordSize The size of the record
         * @return A pointer to the written data
         * @throws IOException (EOFException specifically, if memory ran out)
         * 上面的api相当于是覆盖   这里是追加数据
         */
        public long appendPointerAndCopyRecord(long pointer, DataInputView input, int recordSize)
                throws IOException {
            setWritePosition(appendPosition);
            final long oldLastPosition = appendPosition;
            outView.writeLong(pointer);
            outView.write(input, recordSize);
            appendPosition += 8 + recordSize;
            return oldLastPosition;
        }

        /**
         * Appends a pointer and a record.
         *
         * @param pointer The pointer to write (Note: this is NOT the position to write to!)
         * @param record The record to write
         * @return A pointer to the written data
         * @throws IOException (EOFException specifically, if memory ran out)
         *
         */
        public long appendPointerAndRecord(long pointer, T record) throws IOException {
            setWritePosition(appendPosition);
            return noSeekAppendPointerAndRecord(pointer, record);
        }

        /**
         * Appends a pointer and a record. Call this function only if the write position is at the
         * end!
         *
         * @param pointer The pointer to write (Note: this is NOT the position to write to!)
         * @param record The record to write
         * @return A pointer to the written data
         * @throws IOException (EOFException specifically, if memory ran out)
         *
         * 写入一个指针和一条记录
         */
        public long noSeekAppendPointerAndRecord(long pointer, T record) throws IOException {
            final long oldLastPosition = appendPosition;
            // 获取之前的位置
            final long oldPositionInSegment = outView.getCurrentPositionInSegment();
            final long oldSegmentIndex = outView.currentSegmentIndex;

            // 写入指针和记录
            outView.writeLong(pointer);
            buildSideSerializer.serialize(record, outView);
            // 计算出 appendPosition 的位置
            appendPosition +=
                    outView.getCurrentPositionInSegment()
                            - oldPositionInSegment
                            + outView.getSegmentSize()
                                    * (outView.currentSegmentIndex - oldSegmentIndex);
            return oldLastPosition;
        }

        public long getAppendPosition() {
            return appendPosition;
        }

        // ----------------------- Input -----------------------

        /**
         * 设置当前的读指针
         * @param position
         */
        public void setReadPosition(long position) {
            inView.setReadPosition(position);
        }

        public long getReadPosition() {
            return inView.getReadPosition();
        }

        /**
         * Note: this is sometimes a negated length instead of a pointer (see
         * HashTableProber.updateMatch).
         * 写入记录时 都是一个指针 + 一条记录  所以读取数据时 也是按照 readPointer() + readRecord() 的顺序
         */
        public long readPointer() throws IOException {
            return inView.readLong();
        }

        public T readRecord(T reuse) throws IOException {
            return buildSideSerializer.deserialize(reuse, inView);
        }

        public void skipBytesToRead(int numBytes) throws IOException {
            inView.skipBytesToRead(numBytes);
        }

        // -----------------------------------------------------

        /**
         * AbstractPagedOutputView 表示基于segment写出数据的对象
         */
        private final class RecordAreaOutputView extends AbstractPagedOutputView {

            public int currentSegmentIndex;

            public RecordAreaOutputView(int segmentSize) {
                super(segmentSize, 0);
            }

            @Override
            protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
                    throws EOFException {
                currentSegmentIndex++;
                // 该对象外层有一个segment列表 当相等时 代表本对象的segment已经不够了
                if (currentSegmentIndex == segments.size()) {
                    // 继续申请segment 并添加到列表中
                    addSegment();
                }
                return segments.get(currentSegmentIndex);
            }

            @Override
            public void seekOutput(MemorySegment seg, int position) {
                super.seekOutput(seg, position);
            }
        }
    }

    /**
     * 这是一个临时使用的对象
     */
    private final class StagingOutputView extends AbstractPagedOutputView {

        private final ArrayList<MemorySegment> segments;

        private final int segmentSizeBits;

        private int currentSegmentIndex;

        public StagingOutputView(ArrayList<MemorySegment> segments, int segmentSize) {
            super(segmentSize, 0);
            this.segmentSizeBits = MathUtils.log2strict(segmentSize);
            this.segments = segments;
        }

        /** Seeks to the beginning.
         * 重置偏移量
         * */
        public void reset() {
            seekOutput(segments.get(0), 0);
            currentSegmentIndex = 0;
        }

        @Override
        protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
                throws EOFException {
            currentSegmentIndex++;
            if (currentSegmentIndex == segments.size()) {
                MemorySegment m = allocateSegment();
                if (m == null) {
                    throw new EOFException();
                }
                segments.add(m);
            }
            return segments.get(currentSegmentIndex);
        }

        /**
         * 计算当前偏移量
         * @return
         */
        public long getWritePosition() {
            return (((long) currentSegmentIndex) << segmentSizeBits)
                    + getCurrentPositionInSegment();
        }
    }

    /**
     * A prober for accessing the table. In addition to getMatchFor and updateMatch, it also has
     * insertAfterNoMatch. Warning: Don't modify the table between calling getMatchFor and the other
     * methods!
     *
     * @param <PT> The type of the records that we are probing with
     *
     *            该对象内部包含2个比较器  这是一个探测对象 用于检查某个尝试插入的数据是已存在还是未存在的
     */
    public final class HashTableProber<PT> extends AbstractHashTableProber<PT, T> {

        public HashTableProber(
                TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator) {
            super(probeTypeComparator, pairComparator);
        }

        /**
         * 当前查找的bucket所在的segment
         */
        private int bucketSegmentIndex;
        private int bucketOffset;
        private long curElemPtr;
        private long prevElemPtr;
        private long nextPtr;
        private long recordEnd;

        /**
         * Searches the hash table for the record with the given key. (If there would be multiple
         * matches, only one is returned.)
         *
         * @param record The record whose key we are searching for    需要检测的记录
         * @param targetForMatch If a match is found, it will be written here
         * @return targetForMatch if a match is found, otherwise null.
         */
        @Override
        public T getMatchFor(PT record, T targetForMatch) {
            if (closed) {
                return null;
            }

            // 产生hash值
            final int hashCode = MathUtils.jenkinsHash(probeTypeComparator.hash(record));

            // 通过hash值找到bucket  其实跟一般的hashTable是一样的
            final int bucket = hashCode & numBucketsMask;

            // 找到segment的下标
            bucketSegmentIndex =
                    bucket >>> numBucketsPerSegmentBits; // which segment contains the bucket
            final MemorySegment bucketSegment = bucketSegments[bucketSegmentIndex];

            // 找到bucket在segment的位置
            bucketOffset =
                    (bucket & numBucketsPerSegmentMask)
                            << bucketSizeBits; // offset of the bucket in the segment

            // 读取指针数据  在初始化segment时  这些bucket的位置会设置 END_OF_LIST
            curElemPtr = bucketSegment.getLong(bucketOffset);

            // 先设置对象 用于之后的比较
            pairComparator.setReference(record);

            T currentRecordInList = targetForMatch;

            prevElemPtr = INVALID_PREV_POINTER;
            try {
                // 从这个指针开始处理数据了
                while (curElemPtr != END_OF_LIST && !closed) {

                    // recordArea存储的是下条记录的pointer和record   bucket存储的是指针信息
                    recordArea.setReadPosition(curElemPtr);
                    nextPtr = recordArea.readPointer();

                    // 使用数据设置字段
                    currentRecordInList = recordArea.readRecord(currentRecordInList);
                    // 获取当前停留的位置  也就是这条记录的结尾处
                    recordEnd = recordArea.getReadPosition();

                    // 发现二者是一致的 返回当前记录
                    if (pairComparator.equalToReference(currentRecordInList)) {
                        // we found an element with a matching key, and not just a hash collision
                        return currentRecordInList;
                    }

                    // 用线性探测法解决冲突
                    prevElemPtr = curElemPtr;
                    curElemPtr = nextPtr;
                }
            } catch (IOException ex) {
                throw new RuntimeException(
                        "Error deserializing record from the hashtable: " + ex.getMessage(), ex);
            }
            // 相同hash的record 会用pointer关联起来 当某个ptr为END_OF_LIST 即表示该hash对应的记录已经被检索完了  返回null 表示相同的不存在
            return null;
        }

        @Override
        public T getMatchFor(PT probeSideRecord) {
            return getMatchFor(probeSideRecord, buildSideSerializer.createInstance());
        }

        /**
         * This method can be called after getMatchFor returned a match. It will overwrite the
         * record that was found by getMatchFor. Warning: The new record should have the same key as
         * the old! WARNING; Don't do any modifications to the table between getMatchFor and
         * updateMatch!
         *
         * @param newRecord The record to override the old record with.
         * @throws IOException (EOFException specifically, if memory ran out)
         * 替换 equals判断为true的record
         */
        @Override
        public void updateMatch(T newRecord) throws IOException {
            if (closed) {
                return;
            }
            if (curElemPtr == END_OF_LIST) {
                throw new RuntimeException(
                        "updateMatch was called after getMatchFor returned no match");
            }

            try {
                // determine the new size
                stagingSegmentsOutView.reset();
                // 将记录写入 stagingSegmentsOutView
                buildSideSerializer.serialize(newRecord, stagingSegmentsOutView);
                // 判断序列化后的长度
                final int newRecordSize = (int) stagingSegmentsOutView.getWritePosition();
                stagingSegmentsInView.setReadPosition(0);

                // Determine the size of the place of the old record.
                // 计算原记录的长度    因为对象长度可能发生变化
                final int oldRecordSize = (int) (recordEnd - (curElemPtr + RECORD_OFFSET_IN_LINK));

                // 长度相同的情况 直接替换即可
                if (newRecordSize == oldRecordSize) {
                    // overwrite record at its original place   只替换记录 pointer什么的不做修改
                    recordArea.overwriteRecordAt(
                            curElemPtr + RECORD_OFFSET_IN_LINK,
                            stagingSegmentsInView,
                            newRecordSize);
                } else {
                    // new record has a different size than the old one, append new at the end of
                    // the record area.
                    // Note: we have to do this, even if the new record is smaller, because
                    // otherwise EntryIterator
                    // wouldn't know the size of this place, and wouldn't know where does the next
                    // record start.

                    // 追加记录 nextPtr是原本指向下个相同hash记录的位置
                    final long pointerToAppended =
                            recordArea.appendPointerAndCopyRecord(
                                    nextPtr, stagingSegmentsInView, newRecordSize);

                    // modify the pointer in the previous link  代表首次增加 让bucket记录指针位置
                    if (prevElemPtr == INVALID_PREV_POINTER) {
                        // list had only one element, so prev is in the bucketSegments
                        bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
                    } else {
                        // 找到上条记录 并更新指针
                        recordArea.overwritePointerAt(prevElemPtr, pointerToAppended);
                    }

                    // write the negated size of the hole to the place where the next pointer was,
                    // so that EntryIterator
                    // will know the size of the place without reading the old record.
                    // The negative sign will mean that the record is abandoned, and the
                    // the -1 is for avoiding trouble in case of a record having 0 size. (though I
                    // think this should
                    // never actually happen)
                    // Note: the last record in the record area can't be abandoned. (EntryIterator
                    // makes use of this fact.)
                    // 因为数据不连续了 就产生了一个空洞  此时写入的pointer 用于寻址下个相邻的数据 而不是hash相同的数据
                    // (相同hash的数据由 正值pointer来寻址)
                    recordArea.overwritePointerAt(curElemPtr, -oldRecordSize - 1);

                    holes += oldRecordSize;
                }
            } catch (EOFException ex) {
                compactOrThrow();
                insertOrReplaceRecord(newRecord);
            }
        }

        /**
         * This method can be called after getMatchFor returned null. It inserts the given record to
         * the hash table. Important: The given record should have the same key as the record that
         * was given to getMatchFor! WARNING; Don't do any modifications to the table between
         * getMatchFor and insertAfterNoMatch!
         *
         * @throws IOException (EOFException specifically, if memory ran out)
         *
         * 当发现record 在存储数据的  recordArea中没有 comparator判断相同的记录    就代表首次添加
         */
        public void insertAfterNoMatch(T record) throws IOException {
            if (closed) {
                return;
            }

            // create new link   先写记录 得到指针 然后再回填到bucket上
            long pointerToAppended;
            try {
                pointerToAppended = recordArea.appendPointerAndRecord(END_OF_LIST, record);
            } catch (EOFException ex) {
                compactOrThrow();
                insert(record);
                return;
            }

            // add new link to the end of the list
            if (prevElemPtr == INVALID_PREV_POINTER) {
                // list was empty   表示该record计算出的hash对应的bucket还没有被设置  现在设置指针
                bucketSegments[bucketSegmentIndex].putLong(bucketOffset, pointerToAppended);
            } else {
                // update the pointer of the last element of the list.
                // 更新前一个record指向next对象的指针
                recordArea.overwritePointerAt(prevElemPtr, pointerToAppended);
            }

            numElements++;
            resizeTableIfNecessary();
        }
    }

    /**
     * WARNING: Doing any other operation on the table invalidates the iterator! (Even using
     * getMatchFor of a prober!)
     * 该对象用于遍历 recordArea中的数据
     */
    public final class EntryIterator implements MutableObjectIterator<T> {

        private final long endPosition;

        public EntryIterator() {
            endPosition = recordArea.getAppendPosition();
            // 表示没有数据
            if (endPosition == 0) {
                return;
            }
            // 从头开始读取数据
            recordArea.setReadPosition(0);
        }

        /**
         * 挨个读取记录
         * @param reuse The target object into which to place next element if E is mutable.
         * @return
         * @throws IOException
         */
        @Override
        public T next(T reuse) throws IOException {
            if (endPosition != 0 && recordArea.getReadPosition() < endPosition) {
                // Loop until we find a non-abandoned record.
                // Note: the last record in the record area can't be abandoned.
                while (!closed) {
                    final long pointerOrNegatedLength = recordArea.readPointer();
                    // 表示该位置无数据
                    final boolean isAbandoned = pointerOrNegatedLength < 0;
                    if (!isAbandoned) {
                        // 正常情况下 就顺着segment 挨个读取数据
                        reuse = recordArea.readRecord(reuse);
                        return reuse;
                    } else {
                        // pointerOrNegatedLength is storing a length, because the record was
                        // abandoned.
                        // 记录的是与下个数据的距离  并且特地使用负数来区分  (正数指向的就是有相同hash值的下条记录 负数则是相邻记录)
                        recordArea.skipBytesToRead((int) -(pointerOrNegatedLength + 1));
                    }
                }
                return null; // (we were closed)
            } else {
                return null;
            }
        }

        @Override
        public T next() throws IOException {
            return next(buildSideSerializer.createInstance());
        }
    }

    /**
     * A facade for doing such operations on the hash table that are needed for a reduce operator
     * driver.
     * 该对象可以进行数据的累加
     */
    public final class ReduceFacade {

        /**
         * 该对象用于探测 recordArea
         */
        private final HashTableProber<T> prober;

        /**
         * 对象不能重用  要反复创建 这样也会产生大量的GC开销
         */
        private final boolean objectReuseEnabled;

        private final ReduceFunction<T> reducer;

        private final Collector<T> outputCollector;

        private T reuse;

        public ReduceFacade(
                ReduceFunction<T> reducer,
                Collector<T> outputCollector,
                boolean objectReuseEnabled) {
            this.reducer = reducer;
            this.outputCollector = outputCollector;
            this.objectReuseEnabled = objectReuseEnabled;
            this.prober =
                    getProber(
                            buildSideComparator, new SameTypePairComparator<>(buildSideComparator));
            this.reuse = buildSideSerializer.createInstance();
        }

        /**
         * Looks up the table entry that has the same key as the given record, and updates it by
         * performing a reduce step.
         *
         * @param record The record to update.
         * @throws Exception
         */
        public void updateTableEntryWithReduce(T record) throws Exception {
            T match = prober.getMatchFor(record, reuse);
            if (match == null) {
                // 与该记录判定 equals为true的记录还不存在 直接插入
                prober.insertAfterNoMatch(record);
            } else {
                // do the reduce step
                // 将之前的数据 与当前数据合并后 进行更新
                T res = reducer.reduce(match, record);

                // We have given reuse to the reducer UDF, so create new one if object reuse is
                // disabled
                if (!objectReuseEnabled) {
                    reuse = buildSideSerializer.createInstance();
                }

                prober.updateMatch(res);
            }
        }

        /** Emits all elements currently held by the table to the collector.
         * 产生一条记录
         * */
        public void emit() throws IOException {
            T record = buildSideSerializer.createInstance();
            EntryIterator iter = getEntryIterator();

            // 遍历所有数据 并发送到下游
            while ((record = iter.next(record)) != null && !closed) {
                outputCollector.collect(record);
                if (!objectReuseEnabled) {
                    record = buildSideSerializer.createInstance();
                }
            }
        }

        /**
         * Emits all elements currently held by the table to the collector, and resets the table.
         * The table will have the same number of buckets as before the reset, to avoid doing
         * resizes again.
         */
        public void emitAndReset() throws IOException {
            final int oldNumBucketSegments = bucketSegments.length;
            // 发送所有数据
            emit();
            // 释放资源
            close();
            // 重新打开
            open(oldNumBucketSegments);
        }
    }
}
