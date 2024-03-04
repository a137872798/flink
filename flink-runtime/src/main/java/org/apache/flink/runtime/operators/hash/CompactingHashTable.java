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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.ListMemorySegmentSource;
import org.apache.flink.runtime.util.IntArrayList;
import org.apache.flink.runtime.util.LongArrayList;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A hash table that uses Flink's managed memory and supports replacement of records or updates to
 * records. For an overview of the general data structure of the hash table, please refer to the
 * description of the {@link org.apache.flink.runtime.operators.hash.MutableHashTable}.
 *
 * <p>The hash table is internally divided into two parts: The hash index, and the partition buffers
 * that store the actual records. When records are inserted or updated, the hash table appends the
 * records to its corresponding partition, and inserts or updates the entry in the hash index. In
 * the case that the hash table runs out of memory, it compacts a partition by walking through the
 * hash index and copying all reachable elements into a fresh partition. After that, it releases the
 * memory of the partition to compact.
 *
 * @param <T> Record type stored in hash table
 *           表示一个紧凑的hashTable  应该是表示数据是紧贴的  InPlaceMutableHashTable会产生一些空洞
 */
public class CompactingHashTable<T> extends AbstractMutableHashTable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CompactingHashTable.class);

    // ------------------------------------------------------------------------
    //                         Internal Constants
    // ------------------------------------------------------------------------

    /**
     * The minimum number of memory segments that the compacting hash table needs to work properly
     */
    private static final int MIN_NUM_MEMORY_SEGMENTS = 33;

    /** The maximum number of partitions */
    private static final int MAX_NUM_PARTITIONS = 32;

    /**
     * The default record width that is used when no width is given. The record width is used to
     * determine the ratio of the number of memory segments intended for partition buffers and the
     * number of memory segments in the hash-table structure.
     */
    private static final int DEFAULT_RECORD_LEN = 24;

    /** The length of the hash code stored in the bucket. */
    private static final int HASH_CODE_LEN = 4;

    /** The length of a pointer from a hash bucket to the record in the buffers. */
    private static final int POINTER_LEN = 8;

    /**
     * The number of bytes that the entry in the hash structure occupies, in bytes. It corresponds
     * to a 4 byte hash value and an 8 byte pointer.
     */
    private static final int RECORD_TABLE_BYTES = HASH_CODE_LEN + POINTER_LEN;

    /**
     * The total storage overhead per record, in bytes. This corresponds to the space in the actual
     * hash table buckets, consisting of a 4 byte hash value and an 8 byte pointer, plus the
     * overhead for the stored length field.
     */
    private static final int RECORD_OVERHEAD_BYTES = RECORD_TABLE_BYTES + 2;

    // -------------------------- Bucket Size and Structure -------------------------------------

    private static final int NUM_INTRA_BUCKET_BITS = 7;

    private static final int HASH_BUCKET_SIZE = 0x1 << NUM_INTRA_BUCKET_BITS;

    private static final int BUCKET_HEADER_LENGTH = 16;

    private static final int NUM_ENTRIES_PER_BUCKET =
            (HASH_BUCKET_SIZE - BUCKET_HEADER_LENGTH) / RECORD_TABLE_BYTES;

    private static final int BUCKET_POINTER_START_OFFSET =
            BUCKET_HEADER_LENGTH + (NUM_ENTRIES_PER_BUCKET * HASH_CODE_LEN);

    // ------------------------------ Bucket Header Fields ------------------------------

    /** Offset of the field in the bucket header indicating the bucket's partition. */
    private static final int HEADER_PARTITION_OFFSET = 0;

    /**
     * Offset of the field in the bucket header indicating the bucket's status (spilled or
     * in-memory).
     */
    private static final int HEADER_COUNT_OFFSET = 4;

    /**
     * Offset of the field in the bucket header that holds the forward pointer to its first overflow
     * bucket.
     */
    private static final int HEADER_FORWARD_OFFSET = 8;

    /** Constant for the forward pointer, indicating that the pointer is not set. */
    private static final long BUCKET_FORWARD_POINTER_NOT_SET = ~0x0L;

    // ------------------------------------------------------------------------
    //                              Members
    // ------------------------------------------------------------------------

    /** The free memory segments currently available to the hash join. */
    private final ArrayList<MemorySegment> availableMemory;

    /**
     * The size of the segments used by the hash join buckets. All segments must be of equal size to
     * ease offset computations.
     */
    private final int segmentSize;

    /**
     * The number of hash table buckets in a single memory segment - 1. Because memory segments can
     * be comparatively large, we fit multiple buckets into one memory segment. This variable is a
     * mask that is 1 in the lower bits that define the number of a bucket in a segment.
     */
    private final int bucketsPerSegmentMask;

    /**
     * The number of bits that describe the position of a bucket in a memory segment. Computed as
     * log2(bucketsPerSegment).
     */
    private final int bucketsPerSegmentBits;

    /** An estimate for the average record length. */
    private final int avgRecordLen;

    private final int pageSizeInBits;

    // ------------------------------------------------------------------------

    /** The partitions of the hash table.
     * 每个InMemoryPartition 对应一个分区的数据
     * 每个分区由多个page(segment)组成
     * */
    private final ArrayList<InMemoryPartition<T>> partitions;

    /**
     * The array of memory segments that contain the buckets which form the actual hash-table of
     * hash-codes and pointers to the elements.
     */
    private MemorySegment[] buckets;

    /**
     * Temporary storage for partition compaction (always attempts to allocate as many segments as
     * the largest partition)
     * 在压缩分区数据时 使用的临时对象
     */
    private InMemoryPartition<T> compactionMemory;

    /**
     * The number of buckets in the current table. The bucket array is not necessarily fully used,
     * when not all buckets that would fit into the last segment are actually used.
     * record计算hash后 在bucket中找到数据的偏移量
     */
    private int numBuckets;

    /** Flag to interrupt closed loops */
    private boolean running = true;

    /**
     * Flag necessary so a resize is never triggered during a resize since the code paths are
     * interleaved
     */
    private boolean isResizing;

    // ------------------------------------------------------------------------
    //                         Construction and Teardown
    // ------------------------------------------------------------------------

    public CompactingHashTable(
            TypeSerializer<T> buildSideSerializer,
            TypeComparator<T> buildSideComparator,
            List<MemorySegment> memorySegments) {
        this(buildSideSerializer, buildSideComparator, memorySegments, DEFAULT_RECORD_LEN);
    }

    /**
     *
     * @param buildSideSerializer
     * @param buildSideComparator
     * @param memorySegments
     * @param avgRecordLen  表示一条记录的平均长度
     */
    public CompactingHashTable(
            TypeSerializer<T> buildSideSerializer,
            TypeComparator<T> buildSideComparator,
            List<MemorySegment> memorySegments,
            int avgRecordLen) {

        super(buildSideSerializer, buildSideComparator);

        // some sanity checks first
        if (memorySegments == null) {
            throw new NullPointerException();
        }
        if (memorySegments.size() < MIN_NUM_MEMORY_SEGMENTS) {
            throw new IllegalArgumentException(
                    "Too few memory segments provided. Hash Table needs at least "
                            + MIN_NUM_MEMORY_SEGMENTS
                            + " memory segments.");
        }

        this.availableMemory =
                (memorySegments instanceof ArrayList)
                        ? (ArrayList<MemorySegment>) memorySegments
                        : new ArrayList<MemorySegment>(memorySegments);

        // getLength() == -1  表示长度不固定
        this.avgRecordLen =
                buildSideSerializer.getLength() > 0
                        ? buildSideSerializer.getLength()
                        : avgRecordLen;

        // check the size of the first buffer and record it. all further buffers must have the same
        // size.
        // the size must also be a power of 2
        this.segmentSize = memorySegments.get(0).size();
        if ((this.segmentSize & this.segmentSize - 1) != 0) {
            throw new IllegalArgumentException(
                    "Hash Table requires buffers whose size is a power of 2.");
        }

        this.pageSizeInBits = MathUtils.log2strict(this.segmentSize);

        // 表示一个segment中 应当有多少个bucket
        int bucketsPerSegment = this.segmentSize >> NUM_INTRA_BUCKET_BITS;
        if (bucketsPerSegment == 0) {
            throw new IllegalArgumentException(
                    "Hash Table requires buffers of at least " + HASH_BUCKET_SIZE + " bytes.");
        }
        this.bucketsPerSegmentMask = bucketsPerSegment - 1;
        this.bucketsPerSegmentBits = MathUtils.log2strict(bucketsPerSegment);

        this.partitions = new ArrayList<InMemoryPartition<T>>();

        // so far no partition has any MemorySegments
    }

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /** Initialize the hash table
     * 进行初始化工作
     * */
    @Override
    public void open() {
        synchronized (stateLock) {
            if (!closed) {
                throw new IllegalStateException("currently not closed.");
            }
            closed = false;
        }

        // create the partitions  一开始还是不知道会有多少个分区
        // 通过计算获得分区数
        final int partitionFanOut = getPartitioningFanOutNoEstimates(this.availableMemory.size());

        // 创建分区对象
        createPartitions(partitionFanOut);

        // set up the table structure. the write behind buffers are taken away, as are one buffer
        // per partition
        final int numBuckets =
                getInitialTableSize(
                        this.availableMemory.size(),
                        this.segmentSize,
                        partitionFanOut,   // 预估会产生多少个分区
                        this.avgRecordLen);

        // 初始化每个segment的bucket数据 (head数据)
        initTable(numBuckets, (byte) partitionFanOut);
    }

    /**
     * Closes the hash table. This effectively releases all internal structures and closes all open
     * files and removes them. The call to this method is valid both as a cleanup after the complete
     * inputs were properly processed, and as an cancellation call, which cleans up all resources
     * that are currently held by the hash join. If another process still access the hash table
     * after close has been called no operations will be performed.
     * 关闭 hash表对象
     */
    @Override
    public void close() {
        // make sure that we close only once
        synchronized (this.stateLock) {
            if (this.closed) {
                return;
            }
            this.closed = true;
        }

        LOG.debug("Closing hash table and releasing resources.");

        // release the table structure
        // 释放bucket对应的segment
        releaseTable();

        // clear the memory in the partitions
        // 清理每个分区的数据
        clearPartitions();
    }

    @Override
    public void abort() {
        this.running = false;
        LOG.debug("Cancelling hash table operations.");
    }

    @Override
    public List<MemorySegment> getFreeMemory() {
        if (!this.closed) {
            throw new IllegalStateException("Cannot return memory while join is open.");
        }

        return this.availableMemory;
    }

    // ------------------------------------------------------------------------
    //  adding data to the hash table
    // ------------------------------------------------------------------------

    /**
     * 使用迭代器中的数据 来填充本对象
     * @param input
     * @throws IOException
     */
    public void buildTableWithUniqueKey(final MutableObjectIterator<T> input) throws IOException {
        // go over the complete input and insert every element into the hash table

        T value;
        while (this.running && (value = input.next()) != null) {
            insertOrReplaceRecord(value);
        }
    }

    /**
     * 插入一条新记录
     * @param record
     * @throws IOException
     */
    @Override
    public final void insert(T record) throws IOException {
        if (this.closed) {
            return;
        }

        final int hashCode = MathUtils.jenkinsHash(this.buildSideComparator.hash(record));
        final int posHashCode = hashCode % this.numBuckets;

        // get the bucket for the given hash code
        final int bucketArrayPos = posHashCode >>> this.bucketsPerSegmentBits;
        final int bucketInSegmentPos =
                (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
        final MemorySegment bucket = this.buckets[bucketArrayPos];

        // get the basic characteristics of the bucket
        // 找到对应的分区
        final int partitionNumber = bucket.get(bucketInSegmentPos + HEADER_PARTITION_OFFSET);
        // 数据存储在该分区中
        InMemoryPartition<T> partition = this.partitions.get(partitionNumber);

        // 将数据插入到该分区对应的数据块中  就是简单的追加数据
        long pointer = insertRecordIntoPartition(record, partition, false);
        // 尝试更新bucket的指针数据
        insertBucketEntryFromStart(bucket, bucketInSegmentPos, hashCode, pointer, partitionNumber);
    }

    /**
     * Replaces record in hash table if record already present or append record if not. May trigger
     * expensive compaction.
     *
     * @param record record to insert or replace
     * @throws IOException
     * 插入数据 当遇到相同的时候 则进行替换
     */
    public void insertOrReplaceRecord(T record) throws IOException {
        if (this.closed) {
            return;
        }

        // 先计算record的hash值
        final int searchHashCode = MathUtils.jenkinsHash(this.buildSideComparator.hash(record));
        final int posHashCode = searchHashCode % this.numBuckets;

        // get the bucket for the given hash code
        final MemorySegment originalBucket =
                this.buckets[posHashCode >> this.bucketsPerSegmentBits];
        final int originalBucketOffset =
                (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;

        MemorySegment bucket = originalBucket;
        int bucketInSegmentOffset = originalBucketOffset;

        // get the basic characteristics of the bucket
        // 找到bucket 并获取索引信息
        final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
        final InMemoryPartition<T> partition = this.partitions.get(partitionNumber);
        // 顺便取出 分区相关的overflow
        final MemorySegment[] overflowSegments = partition.overflowSegments;

        this.buildSideComparator.setReference(record);

        // 分别获取数量和 hashcode/pointer指针
        int countInSegment = bucket.getInt(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
        int numInSegment = 0;
        int posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;

        // loop over all segments that are involved in the bucket (original bucket plus overflow
        // buckets)
        while (true) {

            // 开始挨个遍历 落在同一bucket上的entry
            while (numInSegment < countInSegment) {

                final int thisCode = bucket.getInt(posInSegment);
                posInSegment += HASH_CODE_LEN;

                // check if the hash code matches
                // 表示hashCode匹配  一个entry可能存在多个相同的hashCode  但是还是要通过比较(查看)对象才能确定是否相等
                if (thisCode == searchHashCode) {
                    // get the pointer to the pair
                    // 找到对应的pointer
                    final int pointerOffset =
                            bucketInSegmentOffset
                                    + BUCKET_POINTER_START_OFFSET
                                    + (numInSegment * POINTER_LEN);
                    final long pointer = bucket.getLong(pointerOffset);

                    // deserialize the key to check whether it is really equal, or whether we had
                    // only a hash collision
                    // 获取对应的值
                    T valueAtPosition = partition.readRecordAt(pointer);
                    // 表示对应的记录已经存在 进行替换
                    if (this.buildSideComparator.equalToReference(valueAtPosition)) {
                        // 注意这里是追加数据 并没有进行替换 而是修改了entry对应的指针
                        long newPointer = insertRecordIntoPartition(record, partition, true);
                        bucket.putLong(pointerOffset, newPointer);
                        return;
                    }
                }
                numInSegment++;
            }

            // 首先外层的bucket检查完了  还要检查overflow中的bucket

            // this segment is done. check if there is another chained bucket
            long newForwardPointer = bucket.getLong(bucketInSegmentOffset + HEADER_FORWARD_OFFSET);

            // overflow中没有相关数据 那么就可以执行插入逻辑了
            if (newForwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {

                // nothing found. append and insert
                long pointer = insertRecordIntoPartition(record, partition, false);

                // 下面准备添加索引
                if (countInSegment < NUM_ENTRIES_PER_BUCKET) {
                    // we are good in our current bucket, put the values
                    bucket.putInt(
                            bucketInSegmentOffset
                                    + BUCKET_HEADER_LENGTH
                                    + (countInSegment * HASH_CODE_LEN),
                            searchHashCode); // hash code
                    bucket.putLong(
                            bucketInSegmentOffset
                                    + BUCKET_POINTER_START_OFFSET
                                    + (countInSegment * POINTER_LEN),
                            pointer); // pointer
                    bucket.putInt(
                            bucketInSegmentOffset + HEADER_COUNT_OFFSET,
                            countInSegment + 1); // update count
                } else {
                    insertBucketEntryFromStart(
                            originalBucket,
                            originalBucketOffset,
                            searchHashCode,
                            pointer,
                            partitionNumber);
                }
                return;
            }

            final int overflowSegNum = (int) (newForwardPointer >>> 32);
            bucket = overflowSegments[overflowSegNum];
            bucketInSegmentOffset = (int) newForwardPointer;
            countInSegment = bucket.getInt(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
            posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
            numInSegment = 0;
        }
    }

    /**
     * 将数据插入到某个分区中
     * @param record    需要处理的记录
     * @param partition   目标分区
     * @param fragments
     * @return
     * @throws IOException
     */
    private long insertRecordIntoPartition(
            T record, InMemoryPartition<T> partition, boolean fragments) throws IOException {
        try {
            // 这个是总偏移量
            long pointer = partition.appendRecord(record);
            if (fragments) {
                partition.setIsCompacted(false);
            }
            if ((pointer >> this.pageSizeInBits) > this.compactionMemory.getBlockCount()) {
                // compactionMemory 要保证有相同数量的segment
                this.compactionMemory.allocateSegments((int) (pointer >> this.pageSizeInBits));
            }
            return pointer;
        } catch (Exception e) {
            if (e instanceof EOFException || e instanceof IndexOutOfBoundsException) {
                // this indicates an out of memory situation
                try {
                    final int partitionNumber = partition.getPartitionNumber();
                    compactPartition(partitionNumber);

                    // retry append
                    partition =
                            this.partitions.get(
                                    partitionNumber); // compaction invalidates reference
                    long newPointer = partition.appendRecord(record);
                    if ((newPointer >> this.pageSizeInBits)
                            > this.compactionMemory.getBlockCount()) {
                        this.compactionMemory.allocateSegments(
                                (int) (newPointer >> this.pageSizeInBits));
                    }
                    return newPointer;
                } catch (EOFException | IndexOutOfBoundsException ex) {
                    throw new RuntimeException(
                            "Memory ran out. Compaction failed. "
                                    + getMemoryConsumptionString()
                                    + " Message: "
                                    + ex.getMessage());
                }
            } else if (e instanceof IOException) {
                throw (IOException) e;
            } else //noinspection ConstantConditions
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException("Writing record to compacting hash table failed", e);
            }
        }
    }

    /**
     * IMPORTANT!!! We pass only the partition number, because we must make sure we get a fresh
     * partition reference. The partition reference used during search for the key may have become
     * invalid during the compaction.
     * 尝试更新bucket的指针数据
     * @param bucket  该bucket 所处的segment
     * @param bucketInSegmentPos  该bucket在segment的位置
     * @param pointer 本次需要存储的指针位置
     */
    private void insertBucketEntryFromStart(
            MemorySegment bucket,
            int bucketInSegmentPos,
            int hashCode,
            long pointer,
            int partitionNumber)
            throws IOException {
        boolean checkForResize = false;
        // find the position to put the hash code and pointer
        // 一个bucket可以存储多个hash值
        final int count = bucket.getInt(bucketInSegmentPos + HEADER_COUNT_OFFSET);
        if (count < NUM_ENTRIES_PER_BUCKET) {
            // we are good in our current bucket, put the values
            // 一个位置记录多个hash值  也有可能是相同的值
            bucket.putInt(
                    bucketInSegmentPos + BUCKET_HEADER_LENGTH + (count * HASH_CODE_LEN),
                    hashCode); // hash code
            // 维护该hash值对应的指针
            bucket.putLong(
                    bucketInSegmentPos + BUCKET_POINTER_START_OFFSET + (count * POINTER_LEN),
                    pointer); // pointer
            // 更新该entry维护的hash值数量
            bucket.putInt(bucketInSegmentPos + HEADER_COUNT_OFFSET, count + 1); // update count
        } else {
            // we need to go to the overflow buckets
            // 表示该bucket存储的数量过多  需要借助 overflow buckets
            final InMemoryPartition<T> p = this.partitions.get(partitionNumber);

            final long originalForwardPointer =
                    bucket.getLong(bucketInSegmentPos + HEADER_FORWARD_OFFSET);
            final long forwardForNewBucket;

            // 表示已经使用了 overflow容器
            if (originalForwardPointer != BUCKET_FORWARD_POINTER_NOT_SET) {

                // forward pointer set
                final int overflowSegNum = (int) (originalForwardPointer >>> 32);
                final int segOffset = (int) originalForwardPointer;
                final MemorySegment seg = p.overflowSegments[overflowSegNum];

                // 该segment总存储的类似与bucket数据
                final int obCount = seg.getInt(segOffset + HEADER_COUNT_OFFSET);

                // check if there is space in this overflow bucket
                // 将指针记录在overflow bucket
                if (obCount < NUM_ENTRIES_PER_BUCKET) {
                    // space in this bucket and we are done
                    seg.putInt(
                            segOffset + BUCKET_HEADER_LENGTH + (obCount * HASH_CODE_LEN),
                            hashCode); // hash code
                    seg.putLong(
                            segOffset + BUCKET_POINTER_START_OFFSET + (obCount * POINTER_LEN),
                            pointer); // pointer
                    seg.putInt(segOffset + HEADER_COUNT_OFFSET, obCount + 1); // update count
                    return;
                } else {
                    // no space here, we need a new bucket. this current overflow bucket will be the
                    // target of the new overflow bucket
                    // 看来要构成一个链式结构
                    forwardForNewBucket = originalForwardPointer;
                }
            } else {
                // no overflow bucket yet, so we need a first one
                // 表示此时映射到同一个bucket的entry过多
                forwardForNewBucket = BUCKET_FORWARD_POINTER_NOT_SET;
            }

            // we need a new overflow bucket
            // 此时表示需要一个新的bucket  (还没有使用overflow作为bucket记录 或者上个bucket记满了)
            MemorySegment overflowSeg;
            final int overflowBucketNum;
            final int overflowBucketOffset;

            // first, see if there is space for an overflow bucket remaining in the last overflow
            // segment
            // 表示还没有段 或者需要一个新段
            if (p.nextOverflowBucket == 0) {
                // no space left in last bucket, or no bucket yet, so create an overflow segment
                overflowSeg = getNextBuffer();
                overflowBucketOffset = 0;
                overflowBucketNum = p.numOverflowSegments;

                // add the new overflow segment
                // 每次写满了就会对数组进行扩容
                if (p.overflowSegments.length <= p.numOverflowSegments) {
                    MemorySegment[] newSegsArray = new MemorySegment[p.overflowSegments.length * 2];
                    System.arraycopy(
                            p.overflowSegments, 0, newSegsArray, 0, p.overflowSegments.length);
                    p.overflowSegments = newSegsArray;
                }
                // 添加一个 overflow容器
                p.overflowSegments[p.numOverflowSegments] = overflowSeg;
                p.numOverflowSegments++;
                checkForResize = true;
            } else {
                // there is space in the last overflow bucket
                // 找到最后一个段  因为是按照顺序写的
                overflowBucketNum = p.numOverflowSegments - 1;
                overflowSeg = p.overflowSegments[overflowBucketNum];
                // 找到下个bucket对应的偏移量
                overflowBucketOffset = p.nextOverflowBucket << NUM_INTRA_BUCKET_BITS;
            }

            // next overflow bucket is one ahead. if the segment is full, the next will be at the
            // beginning
            // of a new segment
            // 先尝试更新 nextOverflowBucket
            p.nextOverflowBucket =
                    (p.nextOverflowBucket == this.bucketsPerSegmentMask
                            ? 0
                            : p.nextOverflowBucket + 1);

            // insert the new overflow bucket in the chain of buckets
            // 1) set the old forward pointer
            // 2) let the bucket in the main table point to this one
            // overflow 中的bucket 通过pointer组成了链表
            overflowSeg.putLong(overflowBucketOffset + HEADER_FORWARD_OFFSET, forwardForNewBucket);

            // 这个指针要更新到外面的bucket中  通过反向查询就可以找到所有相关的entry了
            final long pointerToNewBucket =
                    (((long) overflowBucketNum) << 32) | ((long) overflowBucketOffset);
            bucket.putLong(bucketInSegmentPos + HEADER_FORWARD_OFFSET, pointerToNewBucket);

            // finally, insert the values into the overflow buckets
            // 这里记录hash值和指针
            overflowSeg.putInt(overflowBucketOffset + BUCKET_HEADER_LENGTH, hashCode); // hash code
            overflowSeg.putLong(
                    overflowBucketOffset + BUCKET_POINTER_START_OFFSET, pointer); // pointer

            // set the count to one
            // 记录出现的数量  当超过3个时 又需要新的entry了
            overflowSeg.putInt(overflowBucketOffset + HEADER_COUNT_OFFSET, 1);

            // 当entry数量很多的时候 会尝试进行重建
            if (checkForResize && !this.isResizing) {
                // check if we should resize buckets
                // 表示分区的overflow segment已经很多了
                if (this.buckets.length <= getOverflowSegmentCount()) {
                    resizeHashTable();
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Access to the entries
    // --------------------------------------------------------------------------------------------

    /**
     * 获取本对象相关的探测器
     * @param probeSideComparator
     * @param pairComparator
     * @param <PT>
     * @return
     */
    @Override
    public <PT> HashTableProber<PT> getProber(
            TypeComparator<PT> probeSideComparator, TypePairComparator<PT, T> pairComparator) {
        return new HashTableProber<PT>(probeSideComparator, pairComparator);
    }

    /**
     * @return Iterator over hash table
     * @see EntryIterator
     */
    public MutableObjectIterator<T> getEntryIterator() {
        return new EntryIterator(this);
    }

    // --------------------------------------------------------------------------------------------
    //  Setup and Tear Down of Structures
    // --------------------------------------------------------------------------------------------

    /**
     * 创建指定数量的分区
     * @param numPartitions
     */
    private void createPartitions(int numPartitions) {
        this.partitions.clear();

        // 每个分区都是从这里申请segment
        ListMemorySegmentSource memSource = new ListMemorySegmentSource(this.availableMemory);

        for (int i = 0; i < numPartitions; i++) {
            this.partitions.add(
                    new InMemoryPartition<T>(
                            this.buildSideSerializer,
                            i,
                            memSource,
                            this.segmentSize,
                            pageSizeInBits));
        }
        // 应该是在压缩分区时使用的临时对象
        this.compactionMemory =
                new InMemoryPartition<T>(
                        this.buildSideSerializer, -1, memSource, this.segmentSize, pageSizeInBits);
    }

    /**
     * 清理分区数据
     */
    private void clearPartitions() {
        // 每个InMemoryPartition内有 多个segment  用于存储数据
        for (InMemoryPartition<T> p : this.partitions) {
            p.clearAllMemory(this.availableMemory);
        }
        this.partitions.clear();
        this.compactionMemory.clearAllMemory(availableMemory);
    }

    /**
     * 初始化table
     * @param numBuckets   总计需要多少个bucket  每个bucket存储一个hash对应的起始偏移量
     * @param numPartitions
     */
    private void initTable(int numBuckets, byte numPartitions) {
        final int bucketsPerSegment = this.bucketsPerSegmentMask + 1;

        // 计算需要使用多少segment
        final int numSegs =
                (numBuckets >>> this.bucketsPerSegmentBits)
                        + ((numBuckets & this.bucketsPerSegmentMask) == 0 ? 0 : 1);

        // 这个table就是维护hash与偏移量的关系
        final MemorySegment[] table = new MemorySegment[numSegs];

        // go over all segments that are part of the table
        for (int i = 0, bucket = 0; i < numSegs && bucket < numBuckets; i++) {

            // 获取一个segment
            final MemorySegment seg = getNextBuffer();

            // go over all buckets in the segment
            // 为每个segment 设置bucket的初始值
            for (int k = 0; k < bucketsPerSegment && bucket < numBuckets; k++, bucket++) {
                final int bucketOffset = k * HASH_BUCKET_SIZE;

                // compute the partition that the bucket corresponds to
                // 每个桶对应一个分区的数据
                final byte partition = assignPartition(bucket, numPartitions);

                // initialize the header fields
                // 插入每个bucket的头部信息  第一个记录了该bucket对应的分区数据
                seg.put(bucketOffset + HEADER_PARTITION_OFFSET, partition);
                seg.putInt(bucketOffset + HEADER_COUNT_OFFSET, 0);
                seg.putLong(bucketOffset + HEADER_FORWARD_OFFSET, BUCKET_FORWARD_POINTER_NOT_SET);
            }

            table[i] = seg;
        }
        this.buckets = table;
        this.numBuckets = numBuckets;
    }

    /**
     * 回收存储bucket的segment
     */
    private void releaseTable() {
        // set the counters back
        this.numBuckets = 0;
        if (this.buckets != null) {
            for (MemorySegment bucket : this.buckets) {
                this.availableMemory.add(bucket);
            }
            this.buckets = null;
        }
    }

    private MemorySegment getNextBuffer() {
        // check if the list directly offers memory
        int s = this.availableMemory.size();
        if (s > 0) {
            return this.availableMemory.remove(s - 1);
        } else {
            throw new RuntimeException("Memory ran out. " + getMemoryConsumptionString());
        }
    }

    // --------------------------------------------------------------------------------------------
    //                             Utility Computational Functions
    // --------------------------------------------------------------------------------------------

    /**
     * Gets the number of partitions to be used for an initial hash-table, when no estimates are
     * available.
     *
     * <p>The current logic makes sure that there are always between 10 and 32 partitions, and close
     * to 0.1 of the number of buffers.
     *
     * @param numBuffers The number of buffers available.
     * @return The number of partitions to use.
     */
    private static int getPartitioningFanOutNoEstimates(int numBuffers) {
        return Math.max(10, Math.min(numBuffers / 10, MAX_NUM_PARTITIONS));
    }

    /** @return String containing a summary of the memory consumption for error messages */
    private String getMemoryConsumptionString() {
        return "numPartitions: "
                + this.partitions.size()
                + " minPartition: "
                + getMinPartition()
                + " maxPartition: "
                + getMaxPartition()
                + " number of overflow segments: "
                + getOverflowSegmentCount()
                + " bucketSize: "
                + this.buckets.length
                + " Overall memory: "
                + getSize()
                + " Partition memory: "
                + getPartitionSize();
    }

    /**
     * Size of all memory segments owned by this hash table
     *
     * @return size in bytes
     */
    private long getSize() {
        long numSegments = 0;
        numSegments += this.availableMemory.size();
        numSegments += this.buckets.length;
        for (InMemoryPartition<T> p : this.partitions) {
            numSegments += p.getBlockCount();
            numSegments += p.numOverflowSegments;
        }
        numSegments += this.compactionMemory.getBlockCount();
        return numSegments * this.segmentSize;
    }

    /**
     * Size of all memory segments owned by the partitions of this hash table excluding the
     * compaction partition
     *
     * @return size in bytes
     */
    private long getPartitionSize() {
        long numSegments = 0;
        for (InMemoryPartition<T> p : this.partitions) {
            numSegments += p.getBlockCount();
        }
        return numSegments * this.segmentSize;
    }

    /** @return number of memory segments in the largest partition */
    private int getMaxPartition() {
        int maxPartition = 0;
        for (InMemoryPartition<T> p1 : this.partitions) {
            if (p1.getBlockCount() > maxPartition) {
                maxPartition = p1.getBlockCount();
            }
        }
        return maxPartition;
    }

    /** @return number of memory segments in the smallest partition */
    private int getMinPartition() {
        int minPartition = Integer.MAX_VALUE;
        for (InMemoryPartition<T> p1 : this.partitions) {
            if (p1.getBlockCount() < minPartition) {
                minPartition = p1.getBlockCount();
            }
        }
        return minPartition;
    }

    /** @return number of memory segments used in overflow buckets */
    private int getOverflowSegmentCount() {
        int result = 0;
        for (InMemoryPartition<T> p : this.partitions) {
            result += p.numOverflowSegments;
        }
        return result;
    }

    /**
     * tries to find a good value for the number of buckets will ensure that the number of buckets
     * is a multiple of numPartitions
     *
     * @param numPartitions 预估会产生多少个分区
     * @return number of buckets
     * 计算table的大小
     */
    private static int getInitialTableSize(
            int numBuffers, int bufferSize, int numPartitions, int recordLenBytes) {
        final long totalSize = ((long) bufferSize) * numBuffers;
        // 预估可以存储多少条记录
        final long numRecordsStorable = totalSize / (recordLenBytes + RECORD_OVERHEAD_BYTES);
        // 这个是 bucket的预估总开销
        final long bucketBytes = numRecordsStorable * RECORD_OVERHEAD_BYTES;

        // 计算大概会使用多少个bucket
        long numBuckets = bucketBytes / (2 * HASH_BUCKET_SIZE) + 1;

        // 根据分区数 做略微的调整
        numBuckets += numPartitions - numBuckets % numPartitions;
        return numBuckets > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) numBuckets;
    }

    /**
     * Assigns a partition to a bucket.
     *
     * @param bucket bucket index
     * @param numPartitions number of partitions
     * @return The hash code for the integer.
     */
    private static byte assignPartition(int bucket, byte numPartitions) {
        return (byte) (bucket % numPartitions);
    }

    /**
     * Attempts to double the number of buckets
     *
     * @return true on success
     * @throws IOException
     * 重建hash表
     */
    @VisibleForTesting
    boolean resizeHashTable() throws IOException {
        // 需要扩容的原因是  分区中的overflow被大量使用  这样间接寻址次数会变多  不如将其展开(展开到bucket中)
        final int newNumBuckets = 2 * this.numBuckets;
        final int bucketsPerSegment = this.bucketsPerSegmentMask + 1;
        // 计算扩容需要的segment数量
        final int newNumSegments = (newNumBuckets + (bucketsPerSegment - 1)) / bucketsPerSegment;

        // 表示新需要的segment
        final int additionalSegments = newNumSegments - this.buckets.length;
        final int numPartitions = this.partitions.size();

        // 此时内存块不足  先对数据进行压缩
        if (this.availableMemory.size() < additionalSegments) {
            for (int i = 0; i < numPartitions; i++) {
                // 每次压缩 都会有内存块被归还
                compactPartition(i);
                if (this.availableMemory.size() >= additionalSegments) {
                    break;
                }
            }
        }

        // 表示内存还是不足 无法扩容
        if (this.availableMemory.size() < additionalSegments || this.closed) {
            return false;
        } else {

            // 开始扩容
            this.isResizing = true;
            // allocate new buckets
            final int startOffset = (this.numBuckets * HASH_BUCKET_SIZE) % this.segmentSize;
            final int oldNumBuckets = this.numBuckets;
            final int oldNumSegments = this.buckets.length;
            MemorySegment[] mergedBuckets = new MemorySegment[newNumSegments];
            System.arraycopy(this.buckets, 0, mergedBuckets, 0, this.buckets.length);
            this.buckets = mergedBuckets;
            this.numBuckets = newNumBuckets;
            // initialize all new buckets
            boolean oldSegment = (startOffset != 0);
            final int startSegment = oldSegment ? (oldNumSegments - 1) : oldNumSegments;

            // 外层遍历segment
            for (int i = startSegment, bucket = oldNumBuckets;
                    i < newNumSegments && bucket < this.numBuckets;
                    i++) {
                MemorySegment seg;
                int bucketOffset;
                // 表示最后一个segment还有空间 要分配bucket
                if (oldSegment) { // the first couple of new buckets may be located on an old
                    // segment
                    seg = this.buckets[i];
                    // 内层遍历bucket
                    for (int k = (oldNumBuckets % bucketsPerSegment);
                            k < bucketsPerSegment && bucket < this.numBuckets;
                            k++, bucket++) {
                        bucketOffset = k * HASH_BUCKET_SIZE;
                        // initialize the header fields  这是在初始化头部信息
                        seg.put(
                                bucketOffset + HEADER_PARTITION_OFFSET,
                                assignPartition(bucket, (byte) numPartitions));
                        seg.putInt(bucketOffset + HEADER_COUNT_OFFSET, 0);
                        seg.putLong(
                                bucketOffset + HEADER_FORWARD_OFFSET,
                                BUCKET_FORWARD_POINTER_NOT_SET);
                    }
                } else {
                    // 申请新segment 并初始化bucket
                    seg = getNextBuffer();
                    // go over all buckets in the segment
                    for (int k = 0;
                            k < bucketsPerSegment && bucket < this.numBuckets;
                            k++, bucket++) {
                        bucketOffset = k * HASH_BUCKET_SIZE;
                        // initialize the header fields
                        seg.put(
                                bucketOffset + HEADER_PARTITION_OFFSET,
                                assignPartition(bucket, (byte) numPartitions));
                        seg.putInt(bucketOffset + HEADER_COUNT_OFFSET, 0);
                        seg.putLong(
                                bucketOffset + HEADER_FORWARD_OFFSET,
                                BUCKET_FORWARD_POINTER_NOT_SET);
                    }
                }
                this.buckets[i] = seg;
                oldSegment = false; // we write on at most one old segment
            }

            // 上面完成了bucket的初始化工作

            int hashOffset;
            int hash;
            int pointerOffset;
            long pointer;
            IntArrayList hashList = new IntArrayList(NUM_ENTRIES_PER_BUCKET);
            LongArrayList pointerList = new LongArrayList(NUM_ENTRIES_PER_BUCKET);
            IntArrayList overflowHashes = new IntArrayList(64);
            LongArrayList overflowPointers = new LongArrayList(64);

            // go over all buckets and split them between old and new buckets
            // 以分区为单位处理
            for (int i = 0; i < numPartitions; i++) {
                InMemoryPartition<T> partition = this.partitions.get(i);
                final MemorySegment[] overflowSegments = partition.overflowSegments;

                int posHashCode;

                for (int j = 0, bucket = i;
                        j < this.buckets.length && bucket < oldNumBuckets;
                        j++) {
                    MemorySegment segment = this.buckets[j];
                    // go over all buckets in the segment belonging to the partition
                    // 处理老数据
                    for (int k = bucket % bucketsPerSegment;
                            k < bucketsPerSegment && bucket < oldNumBuckets;
                            k += numPartitions, bucket += numPartitions) {
                        int bucketOffset = k * HASH_BUCKET_SIZE;
                        if ((int) segment.get(bucketOffset + HEADER_PARTITION_OFFSET) != i) {
                            throw new IOException(
                                    "Accessed wrong bucket! wanted: "
                                            + i
                                            + " got: "
                                            + segment.get(bucketOffset + HEADER_PARTITION_OFFSET));
                        }
                        // loop over all segments that are involved in the bucket (original bucket
                        // plus overflow buckets)
                        int countInSegment = segment.getInt(bucketOffset + HEADER_COUNT_OFFSET);
                        int numInSegment = 0;
                        pointerOffset = bucketOffset + BUCKET_POINTER_START_OFFSET;
                        hashOffset = bucketOffset + BUCKET_HEADER_LENGTH;

                        // 取出该bucket关联到的所有hash和pointer
                        while (true) {
                            while (numInSegment < countInSegment) {
                                hash = segment.getInt(hashOffset);
                                if ((hash % this.numBuckets) != bucket
                                        && (hash % this.numBuckets) != (bucket + oldNumBuckets)) {
                                    throw new IOException(
                                            "wanted: "
                                                    + bucket
                                                    + " or "
                                                    + (bucket + oldNumBuckets)
                                                    + " got: "
                                                    + hash % this.numBuckets);
                                }
                                pointer = segment.getLong(pointerOffset);
                                // 老数据的hash值和pointer都存储在list中
                                hashList.add(hash);
                                pointerList.add(pointer);
                                pointerOffset += POINTER_LEN;
                                hashOffset += HASH_CODE_LEN;
                                numInSegment++;
                            }
                            // this segment is done. check if there is another chained bucket
                            final long forwardPointer =
                                    segment.getLong(bucketOffset + HEADER_FORWARD_OFFSET);
                            if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
                                break;
                            }
                            final int overflowSegNum = (int) (forwardPointer >>> 32);
                            segment = overflowSegments[overflowSegNum];
                            bucketOffset = (int) forwardPointer;
                            countInSegment = segment.getInt(bucketOffset + HEADER_COUNT_OFFSET);
                            pointerOffset = bucketOffset + BUCKET_POINTER_START_OFFSET;
                            hashOffset = bucketOffset + BUCKET_HEADER_LENGTH;
                            numInSegment = 0;
                        }

                        segment = this.buckets[j];
                        bucketOffset = k * HASH_BUCKET_SIZE;
                        // reset bucket for re-insertion  重置该bucket的entry信息 因为扩容后之前的信息失效了
                        segment.putInt(bucketOffset + HEADER_COUNT_OFFSET, 0);
                        segment.putLong(
                                bucketOffset + HEADER_FORWARD_OFFSET,
                                BUCKET_FORWARD_POINTER_NOT_SET);
                        // refill table
                        if (hashList.size() != pointerList.size()) {
                            throw new IOException(
                                    "Pointer and hash counts do not match. hashes: "
                                            + hashList.size()
                                            + " pointer: "
                                            + pointerList.size());
                        }

                        // 找到扩容后对应的新下标
                        int newSegmentIndex = (bucket + oldNumBuckets) / bucketsPerSegment;
                        MemorySegment newSegment = this.buckets[newSegmentIndex];

                        // we need to avoid overflows in the first run
                        int oldBucketCount = 0;
                        int newBucketCount = 0;

                        // 遍历之前读到的所有hash和pointer
                        while (!hashList.isEmpty()) {
                            hash = hashList.removeLast();
                            pointer = pointerList.removeLong(pointerList.size() - 1);

                            posHashCode = hash % this.numBuckets;
                            // 表示落在原bucket上
                            if (posHashCode == bucket && oldBucketCount < NUM_ENTRIES_PER_BUCKET) {
                                bucketOffset = (bucket % bucketsPerSegment) * HASH_BUCKET_SIZE;
                                insertBucketEntryFromStart(
                                        segment,
                                        bucketOffset,
                                        hash,
                                        pointer,
                                        partition.getPartitionNumber());
                                oldBucketCount++;

                                // 落在新bucket上
                            } else if (posHashCode == (bucket + oldNumBuckets)
                                    && newBucketCount < NUM_ENTRIES_PER_BUCKET) {
                                bucketOffset =
                                        ((bucket + oldNumBuckets) % bucketsPerSegment)
                                                * HASH_BUCKET_SIZE;
                                insertBucketEntryFromStart(
                                        newSegment,
                                        bucketOffset,
                                        hash,
                                        pointer,
                                        partition.getPartitionNumber());
                                newBucketCount++;
                                // 此时entry数量超过了3  需要占用overflow
                            } else if (posHashCode == (bucket + oldNumBuckets)
                                    || posHashCode == bucket) {
                                overflowHashes.add(hash);
                                overflowPointers.add(pointer);
                            } else {
                                throw new IOException(
                                        "Accessed wrong bucket. Target: "
                                                + bucket
                                                + " or "
                                                + (bucket + oldNumBuckets)
                                                + " Hit: "
                                                + posHashCode);
                            }
                        }
                        hashList.clear();
                        pointerList.clear();
                    }
                }

                // 上面已经将新pointer添加到bucket中了 剩下的是多出来的部分 需要加入到overflow中
                // reset partition's overflow buckets and reclaim their memory
                // 现在可以回收partition原本的overflow数据了
                this.availableMemory.addAll(partition.resetOverflowBuckets());
                // clear overflow lists
                int bucketArrayPos;
                int bucketInSegmentPos;
                MemorySegment bucket;
                while (!overflowHashes.isEmpty()) {
                    hash = overflowHashes.removeLast();
                    pointer = overflowPointers.removeLong(overflowPointers.size() - 1);
                    posHashCode = hash % this.numBuckets;
                    bucketArrayPos = posHashCode >>> this.bucketsPerSegmentBits;
                    bucketInSegmentPos =
                            (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
                    bucket = this.buckets[bucketArrayPos];
                    insertBucketEntryFromStart(
                            bucket,
                            bucketInSegmentPos,
                            hash,
                            pointer,
                            partition.getPartitionNumber());
                }
                overflowHashes.clear();
                overflowPointers.clear();
            }
            this.isResizing = false;
            return true;
        }
    }

    /**
     * 对每个分区进行数据压缩
     * @throws IOException
     */
    @VisibleForTesting
    void compactPartitions() throws IOException {
        for (int x = 0; x < partitions.size(); x++) {
            compactPartition(x);
        }
    }

    /**
     * Compacts (garbage collects) partition with copy-compact strategy using compaction partition
     *
     * @param partitionNumber partition to compact
     * @throws IOException
     * 可以看到更新数据并不是通过覆盖实现的  而是直接追加 所以在压缩分区数据时就是去掉重复数据
     */
    private void compactPartition(final int partitionNumber) throws IOException {
        // do nothing if table was closed, parameter is invalid or no garbage exists
        if (this.closed
                || partitionNumber >= this.partitions.size()
                || this.partitions.get(partitionNumber).isCompacted()) {
            return;
        }
        // release all segments owned by compaction partition
        // 使用临时容器
        this.compactionMemory.clearAllMemory(availableMemory);
        this.compactionMemory.allocateSegments(1);
        this.compactionMemory.pushDownPages();
        T tempHolder = this.buildSideSerializer.createInstance();
        final int numPartitions = this.partitions.size();
        InMemoryPartition<T> partition = this.partitions.remove(partitionNumber);
        MemorySegment[] overflowSegments = partition.overflowSegments;
        long pointer;
        int pointerOffset;
        int bucketOffset;
        final int bucketsPerSegment = this.bucketsPerSegmentMask + 1;

        // 开始处理
        for (int i = 0, bucket = partitionNumber;
                i < this.buckets.length && bucket < this.numBuckets;
                i++) {

            // 外层是遍历存储bucket的每个segment
            MemorySegment segment = this.buckets[i];
            // go over all buckets in the segment belonging to the partition

            // 遍历segment的每个bucket
            for (int k = bucket % bucketsPerSegment;
                    k < bucketsPerSegment && bucket < this.numBuckets;
                    // 每次推进numPartitions  这样读取到的bucket属于同一个分区
                    k += numPartitions, bucket += numPartitions) {
                bucketOffset = k * HASH_BUCKET_SIZE;
                if ((int) segment.get(bucketOffset + HEADER_PARTITION_OFFSET) != partitionNumber) {
                    throw new IOException(
                            "Accessed wrong bucket! wanted: "
                                    + partitionNumber
                                    + " got: "
                                    + segment.get(bucketOffset + HEADER_PARTITION_OFFSET));
                }
                // loop over all segments that are involved in the bucket (original bucket plus
                // overflow buckets)
                int countInSegment = segment.getInt(bucketOffset + HEADER_COUNT_OFFSET);
                int numInSegment = 0;
                pointerOffset = bucketOffset + BUCKET_POINTER_START_OFFSET;
                while (true) {
                    while (numInSegment < countInSegment) {
                        pointer = segment.getLong(pointerOffset);
                        tempHolder = partition.readRecordAt(pointer, tempHolder);
                        // 将数据加入临时容器中
                        pointer = this.compactionMemory.appendRecord(tempHolder);
                        // 同时更新指针
                        segment.putLong(pointerOffset, pointer);
                        pointerOffset += POINTER_LEN;
                        numInSegment++;
                    }
                    // this segment is done. check if there is another chained bucket
                    final long forwardPointer =
                            segment.getLong(bucketOffset + HEADER_FORWARD_OFFSET);
                    if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
                        break;
                    }
                    final int overflowSegNum = (int) (forwardPointer >>> 32);
                    segment = overflowSegments[overflowSegNum];
                    bucketOffset = (int) forwardPointer;
                    countInSegment = segment.getInt(bucketOffset + HEADER_COUNT_OFFSET);
                    pointerOffset = bucketOffset + BUCKET_POINTER_START_OFFSET;
                    numInSegment = 0;
                }
                segment = this.buckets[i];
            }
        }
        // 以上已经处理完所有有关该分区的数据了

        // swap partition with compaction partition
        this.compactionMemory.setPartitionNumber(partitionNumber);
        // 覆盖原分区数据
        this.partitions.add(partitionNumber, compactionMemory);
        // overflow相关的数据要保留
        this.partitions.get(partitionNumber).overflowSegments = partition.overflowSegments;
        this.partitions.get(partitionNumber).numOverflowSegments = partition.numOverflowSegments;
        this.partitions.get(partitionNumber).nextOverflowBucket = partition.nextOverflowBucket;
        this.partitions.get(partitionNumber).setIsCompacted(true);
        // this.partitions.get(partitionNumber).pushDownPages();
        this.compactionMemory = partition;
        this.compactionMemory.resetRecordCounter();
        this.compactionMemory.setPartitionNumber(-1);
        this.compactionMemory.overflowSegments = null;
        this.compactionMemory.numOverflowSegments = 0;
        this.compactionMemory.nextOverflowBucket = 0;
        // try to allocate maximum segment count
        this.compactionMemory.clearAllMemory(this.availableMemory);
        int maxSegmentNumber = this.getMaxPartition();
        this.compactionMemory.allocateSegments(maxSegmentNumber);
        this.compactionMemory.resetRWViews();
        this.compactionMemory.pushDownPages();
    }

    /**
     * Iterator that traverses the whole hash table once
     *
     * <p>If entries are inserted during iteration they may be overlooked by the iterator
     *
     * 该对象用于迭代 内部元素
     */
    public class EntryIterator implements MutableObjectIterator<T> {

        private CompactingHashTable<T> table;

        private ArrayList<T> cache; // holds full bucket including its overflow buckets

        private int currentBucketIndex = 0;
        private int currentSegmentIndex = 0;
        private int currentBucketOffset = 0;
        private int bucketsPerSegment;

        private boolean done;

        private EntryIterator(CompactingHashTable<T> compactingHashTable) {
            this.table = compactingHashTable;
            this.cache = new ArrayList<T>(64);
            this.done = false;
            this.bucketsPerSegment = table.bucketsPerSegmentMask + 1;
        }

        @Override
        public T next(T reuse) throws IOException {
            return next();
        }

        /**
         * 挨个读取记录
         * @return
         * @throws IOException
         */
        @Override
        public T next() throws IOException {
            if (done || this.table.closed) {
                return null;
            } else if (!cache.isEmpty()) {
                return cache.remove(cache.size() - 1);
            } else {
                while (!done && cache.isEmpty()) {
                    done = !fillCache();
                }
                if (!done) {
                    return cache.remove(cache.size() - 1);
                } else {
                    return null;
                }
            }
        }

        /**
         * utility function that inserts all entries from a bucket and its overflow buckets into the
         * cache
         *
         * @return true if last bucket was not reached yet
         * @throws IOException
         *
         * 通过该方法补充数据
         */
        private boolean fillCache() throws IOException {
            // 表示所有bucket都已经被加载了
            if (currentBucketIndex >= table.numBuckets) {
                return false;
            }
            // 挨个读取bucket
            MemorySegment bucket = table.buckets[currentSegmentIndex];
            // get the basic characteristics of the bucket
            // 加载每个bucket的数据
            final int partitionNumber = bucket.get(currentBucketOffset + HEADER_PARTITION_OFFSET);
            final InMemoryPartition<T> partition = table.partitions.get(partitionNumber);
            final MemorySegment[] overflowSegments = partition.overflowSegments;

            int countInSegment = bucket.getInt(currentBucketOffset + HEADER_COUNT_OFFSET);
            int numInSegment = 0;
            int posInSegment = currentBucketOffset + BUCKET_POINTER_START_OFFSET;
            int bucketOffset = currentBucketOffset;

            // loop over all segments that are involved in the bucket (original bucket plus overflow
            // buckets)
            while (true) {
                while (numInSegment < countInSegment) {
                    long pointer = bucket.getLong(posInSegment);
                    posInSegment += POINTER_LEN;
                    numInSegment++;
                    T target = table.buildSideSerializer.createInstance();
                    try {
                        target = partition.readRecordAt(pointer, target);
                        // 读取到记录就加入cache
                        cache.add(target);
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Error deserializing record from the Hash Table: " + e.getMessage(),
                                e);
                    }
                }
                // this segment is done. check if there is another chained bucket
                final long forwardPointer = bucket.getLong(bucketOffset + HEADER_FORWARD_OFFSET);
                if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
                    break;
                }
                final int overflowSegNum = (int) (forwardPointer >>> 32);
                bucket = overflowSegments[overflowSegNum];
                bucketOffset = (int) forwardPointer;
                countInSegment = bucket.getInt(bucketOffset + HEADER_COUNT_OFFSET);
                posInSegment = bucketOffset + BUCKET_POINTER_START_OFFSET;
                numInSegment = 0;
            }

            // 以上操作是读取完一个bucket  所有bucket都要读取
            currentBucketIndex++;
            if (currentBucketIndex % bucketsPerSegment == 0) {
                currentSegmentIndex++;
                currentBucketOffset = 0;
            } else {
                currentBucketOffset += HASH_BUCKET_SIZE;
            }
            return true;
        }
    }

    /**
     * 探测器 可以扫描各分区各page的记录 并判断是否有相同的记录
     * @param <PT>
     */
    public final class HashTableProber<PT> extends AbstractHashTableProber<PT, T> {

        private InMemoryPartition<T> partition;

        private MemorySegment bucket;

        private int pointerOffsetInBucket;

        private HashTableProber(
                TypeComparator<PT> probeTypeComparator, TypePairComparator<PT, T> pairComparator) {
            super(probeTypeComparator, pairComparator);
        }

        /**
         *
         * @param probeSideRecord  检查是否有与该条匹配的记录
         * @param reuse
         * @return
         */
        public T getMatchFor(PT probeSideRecord, T reuse) {
            if (closed) {
                return null;
            }
            final int searchHashCode =
                    MathUtils.jenkinsHash(this.probeTypeComparator.hash(probeSideRecord));

            final int posHashCode = searchHashCode % numBuckets;

            // get the bucket for the given hash code
            MemorySegment bucket = buckets[posHashCode >> bucketsPerSegmentBits];
            int bucketInSegmentOffset =
                    (posHashCode & bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;

            // get the basic characteristics of the bucket
            // 先找到分区
            final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
            final InMemoryPartition<T> p = partitions.get(partitionNumber);
            // 获取overflow
            final MemorySegment[] overflowSegments = p.overflowSegments;

            this.pairComparator.setReference(probeSideRecord);

            int countInSegment = bucket.getInt(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
            int numInSegment = 0;
            int posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;

            // loop over all segments that are involved in the bucket (original bucket plus overflow
            // buckets)
            while (true) {

                while (numInSegment < countInSegment) {

                    final int thisCode = bucket.getInt(posInSegment);
                    posInSegment += HASH_CODE_LEN;

                    // check if the hash code matches
                    if (thisCode == searchHashCode) {
                        // get the pointer to the pair
                        final int pointerOffset =
                                bucketInSegmentOffset
                                        + BUCKET_POINTER_START_OFFSET
                                        + (numInSegment * POINTER_LEN);
                        final long pointer = bucket.getLong(pointerOffset);
                        numInSegment++;

                        // deserialize the key to check whether it is really equal, or whether we
                        // had only a hash collision
                        try {
                            // 读取数据
                            reuse = p.readRecordAt(pointer, reuse);

                            // 数据匹配 返回
                            if (this.pairComparator.equalToReference(reuse)) {
                                this.partition = p;
                                this.bucket = bucket;
                                this.pointerOffsetInBucket = pointerOffset;
                                return reuse;
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(
                                    "Error deserializing record from the hashtable: "
                                            + e.getMessage(),
                                    e);
                        }
                    } else {
                        numInSegment++;
                    }
                }

                // this segment is done. check if there is another chained bucket
                final long forwardPointer =
                        bucket.getLong(bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
                if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
                    return null;
                }

                final int overflowSegNum = (int) (forwardPointer >>> 32);
                bucket = overflowSegments[overflowSegNum];
                bucketInSegmentOffset = (int) forwardPointer;
                countInSegment = bucket.getInt(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
                posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
                numInSegment = 0;
            }
        }

        /**
         * 查找匹配的记录 跟上面步骤一样
         * @param probeSideRecord
         * @return
         */
        public T getMatchFor(PT probeSideRecord) {
            if (closed) {
                return null;
            }
            final int searchHashCode =
                    MathUtils.jenkinsHash(this.probeTypeComparator.hash(probeSideRecord));

            final int posHashCode = searchHashCode % numBuckets;

            // get the bucket for the given hash code
            MemorySegment bucket = buckets[posHashCode >> bucketsPerSegmentBits];
            int bucketInSegmentOffset =
                    (posHashCode & bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;

            // get the basic characteristics of the bucket
            final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
            final InMemoryPartition<T> p = partitions.get(partitionNumber);
            final MemorySegment[] overflowSegments = p.overflowSegments;

            this.pairComparator.setReference(probeSideRecord);

            int countInSegment = bucket.getInt(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
            int numInSegment = 0;
            int posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;

            // loop over all segments that are involved in the bucket (original bucket plus overflow
            // buckets)
            while (true) {

                while (numInSegment < countInSegment) {

                    final int thisCode = bucket.getInt(posInSegment);
                    posInSegment += HASH_CODE_LEN;

                    // check if the hash code matches
                    if (thisCode == searchHashCode) {
                        // get the pointer to the pair
                        final int pointerOffset =
                                bucketInSegmentOffset
                                        + BUCKET_POINTER_START_OFFSET
                                        + (numInSegment * POINTER_LEN);
                        final long pointer = bucket.getLong(pointerOffset);
                        numInSegment++;

                        // deserialize the key to check whether it is really equal, or whether we
                        // had only a hash collision
                        try {
                            T result = p.readRecordAt(pointer);

                            if (this.pairComparator.equalToReference(result)) {
                                this.partition = p;
                                this.bucket = bucket;
                                this.pointerOffsetInBucket = pointerOffset;
                                return result;
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(
                                    "Error deserializing record from the hashtable: "
                                            + e.getMessage(),
                                    e);
                        }
                    } else {
                        numInSegment++;
                    }
                }

                // this segment is done. check if there is another chained bucket
                final long forwardPointer =
                        bucket.getLong(bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
                if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
                    return null;
                }

                final int overflowSegNum = (int) (forwardPointer >>> 32);
                bucket = overflowSegments[overflowSegNum];
                bucketInSegmentOffset = (int) forwardPointer;
                countInSegment = bucket.getInt(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
                posInSegment = bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
                numInSegment = 0;
            }
        }

        public void updateMatch(T record) throws IOException {
            if (closed) {
                return;
            }
            long newPointer = insertRecordIntoPartition(record, this.partition, true);
            this.bucket.putLong(this.pointerOffsetInBucket, newPointer);
        }
    }
}
