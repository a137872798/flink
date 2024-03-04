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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypePairComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.core.memory.SeekableDataOutputView;
import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BulkBlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.HeaderlessChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.operators.util.BitSet;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An implementation of a Hybrid Hash Join. The join starts operating in memory and gradually starts
 * spilling contents to disk, when the memory is not sufficient. It does not need to know a priori
 * how large the input will be.
 *
 * <p>The design of this class follows in many parts the design presented in "Hash joins and hash
 * teams in Microsoft SQL Server", by Goetz Graefe et al. In its current state, the implementation
 * lacks features like dynamic role reversal, partition tuning, or histogram guided partitioning.
 *
 * <p>The layout of the buckets inside a memory segment is as follows:
 *
 * <pre>
 * +----------------------------- Bucket x ----------------------------
 * |Partition (1 byte) | Status (1 byte) | element count (2 bytes) |
 * | next-bucket-in-chain-pointer (8 bytes) | probedFlags (2 bytes) | reserved (2 bytes) |
 * |
 * |hashCode 1 (4 bytes) | hashCode 2 (4 bytes) | hashCode 3 (4 bytes) |
 * | ... hashCode n-1 (4 bytes) | hashCode n (4 bytes)
 * |
 * |pointer 1 (8 bytes) | pointer 2 (8 bytes) | pointer 3 (8 bytes) |
 * | ... pointer n-1 (8 bytes) | pointer n (8 bytes)
 * |
 * +---------------------------- Bucket x + 1--------------------------
 * |Partition (1 byte) | Status (1 byte) | element count (2 bytes) |
 * | next-bucket-in-chain-pointer (8 bytes) | probedFlags (2 bytes) | reserved (2 bytes) |
 * |
 * |hashCode 1 (4 bytes) | hashCode 2 (4 bytes) | hashCode 3 (4 bytes) |
 * | ... hashCode n-1 (4 bytes) | hashCode n (4 bytes)
 * |
 * |pointer 1 (8 bytes) | pointer 2 (8 bytes) | pointer 3 (8 bytes) |
 * | ... pointer n-1 (8 bytes) | pointer n (8 bytes)
 * +-------------------------------------------------------------------
 * | ...
 * |
 * </pre>
 *
 * @param <BT> The type of records from the build side that are stored in the hash table.
 * @param <PT> The type of records from the probe side that are stored in the hash table.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class MutableHashTable<BT, PT> implements MemorySegmentSource {

    private static final Logger LOG = LoggerFactory.getLogger(MutableHashTable.class);

    // ------------------------------------------------------------------------
    //                         Internal Constants
    // ------------------------------------------------------------------------

    /** The maximum number of recursive partitionings that the join does before giving up. */
    private static final int MAX_RECURSION_DEPTH = 3;

    /**
     * The minimum number of memory segments the hash join needs to be supplied with in order to
     * work.
     */
    private static final int MIN_NUM_MEMORY_SEGMENTS = 33;

    /**
     * The maximum number of partitions, which defines the spilling granularity. Each recursion, the
     * data is divided maximally into that many partitions, which are processed in one chuck.
     */
    private static final int MAX_NUM_PARTITIONS = Byte.MAX_VALUE;

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

    // -------------------------- Bucket Size and Structure -------------------------------------

    static final int NUM_INTRA_BUCKET_BITS = 7;

    static final int HASH_BUCKET_SIZE = 0x1 << NUM_INTRA_BUCKET_BITS;

    static final int BUCKET_HEADER_LENGTH = 16;

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
    private static final int HEADER_STATUS_OFFSET = 1;

    /** Offset of the field in the bucket header indicating the bucket's element count. */
    private static final int HEADER_COUNT_OFFSET = 2;

    /**
     * Offset of the field in the bucket header that holds the forward pointer to its first overflow
     * bucket.
     */
    private static final int HEADER_FORWARD_OFFSET = 4;

    /** Offset of the field in the bucket header that holds the probed bit set. */
    static final int HEADER_PROBED_FLAGS_OFFSET = 12;

    /** Constant for the forward pointer, indicating that the pointer is not set. */
    private static final long BUCKET_FORWARD_POINTER_NOT_SET = ~0x0L;

    /** Constant for the bucket status, indicating that the bucket is in memory. */
    private static final byte BUCKET_STATUS_IN_MEMORY = 0;

    /** Constant for the bucket status, indicating that the bucket has filter. */
    private static final byte BUCKET_STATUS_IN_FILTER = 1;

    // ------------------------------------------------------------------------
    //                              Members
    // ------------------------------------------------------------------------

    /** The utilities to serialize the build side data types. */
    protected final TypeSerializer<BT> buildSideSerializer;

    /** The utilities to serialize the probe side data types. */
    protected final TypeSerializer<PT> probeSideSerializer;

    /** The utilities to hash and compare the build side data types. */
    protected final TypeComparator<BT> buildSideComparator;

    /** The utilities to hash and compare the probe side data types. */
    private final TypeComparator<PT> probeSideComparator;

    /** The comparator used to determine (in)equality between probe side and build side records. */
    private final TypePairComparator<PT, BT> recordComparator;

    /** The free memory segments currently available to the hash join. */
    protected final List<MemorySegment> availableMemory;

    /**
     * The queue of buffers that can be used for write-behind. Any buffer that is written
     * asynchronously to disk is returned through this queue. hence, it may sometimes contain more
     */
    protected final LinkedBlockingQueue<MemorySegment> writeBehindBuffers;

    /** The I/O manager used to instantiate writers for the spilled partitions. */
    protected final IOManager ioManager;

    /**
     * The size of the segments used by the hash join buckets. All segments must be of equal size to
     * ease offset computations.
     */
    protected final int segmentSize;

    /** The total number of memory segments available to the hash join. */
    private final int totalNumBuffers;

    /** The number of write-behind buffers used. */
    private final int numWriteBehindBuffers;

    /**
     * The number of hash table buckets in a single memory segment - 1. Because memory segments can
     * be comparatively large, we fit multiple buckets into one memory segment. This variable is a
     * mask that is 1 in the lower bits that define the number of a bucket in a segment.
     */
    protected final int bucketsPerSegmentMask;

    /**
     * The number of bits that describe the position of a bucket in a memory segment. Computed as
     * log2(bucketsPerSegment).
     */
    protected final int bucketsPerSegmentBits;

    /** An estimate for the average record length. */
    private final int avgRecordLen;

    /** Flag to enable/disable bloom filters for spilled partitions */
    private final boolean useBloomFilters;

    // ------------------------------------------------------------------------

    /** The partitions that are built by processing the current partition. */
    protected final ArrayList<HashPartition<BT, PT>> partitionsBeingBuilt;

    /** The partitions that have been spilled previously and are pending to be processed. */
    private final ArrayList<HashPartition<BT, PT>> partitionsPending;

    /** Iterator over the elements in the hash table. */
    private HashBucketIterator<BT, PT> bucketIterator;

    /** Iterator over the elements from the probe side. */
    protected ProbeIterator<PT> probeIterator;

    /** The reader for the spilled-file of the build partition that is currently read. */
    private BlockChannelReader<MemorySegment> currentSpilledBuildSide;

    /** The reader for the spilled-file of the probe partition that is currently read. */
    private BlockChannelReader<MemorySegment> currentSpilledProbeSide;

    /**
     * The channel enumerator that is used while processing the current partition to create channels
     * for the spill partitions it requires.
     */
    protected FileIOChannel.Enumerator currentEnumerator;

    /**
     * The array of memory segments that contain the buckets which form the actual hash-table of
     * hash-codes and pointers to the elements.
     */
    protected MemorySegment[] buckets;

    /**
     * The bloom filter utility used to transform hash buckets of spilled partitions into a
     * probabilistic filter
     */
    private BloomFilter bloomFilter;

    /**
     * The number of buckets in the current table. The bucket array is not necessarily fully used,
     * when not all buckets that would fit into the last segment are actually used.
     */
    protected int numBuckets;

    /**
     * The number of buffers in the write behind queue that are actually not write behind buffers,
     * but regular buffers that only have not yet returned. This is part of an optimization that the
     * spilling code needs not wait until the partition is completely spilled before proceeding.
     * 这个可以理解为后备的buffer  当可用buffer不足时可以从这里获取
     */
    protected int writeBehindBuffersAvailable;

    /**
     * The recursion depth of the partition that is currently processed. The initial table has a
     * recursion depth of 0. Partitions spilled from a table that is built for a partition with
     * recursion depth <i>n</i> have a recursion depth of <i>n+1</i>.
     */
    protected int currentRecursionDepth;

    /** Flag indicating that the closing logic has been invoked. */
    protected AtomicBoolean closed = new AtomicBoolean();

    /** If true, build side partitions are kept for multiple probe steps. */
    protected boolean keepBuildSidePartitions;

    /**
     * BitSet which used to mark whether the element(int build side) has successfully matched during
     * probe phase. As there are 9 elements in each bucket, we assign 2 bytes to BitSet.
     */
    private final BitSet probedSet = new BitSet(2);

    protected boolean furtherPartitioning;

    private boolean running = true;

    private boolean buildSideOuterJoin = false;

    private MutableObjectIterator<BT> unmatchedBuildIterator;

    private boolean probeMatchedPhase = true;

    private boolean unmatchedBuildVisited = false;

    // ------------------------------------------------------------------------
    //                         Construction and Teardown
    // ------------------------------------------------------------------------

    /**
     * 使用几个序列化对象和 比较器对象 进行初始化
     * @param buildSideSerializer
     * @param probeSideSerializer
     * @param buildSideComparator
     * @param probeSideComparator
     * @param comparator
     * @param memorySegments
     * @param ioManager  该对象用于创建异步写入对象
     */
    public MutableHashTable(
            TypeSerializer<BT> buildSideSerializer,
            TypeSerializer<PT> probeSideSerializer,
            TypeComparator<BT> buildSideComparator,
            TypeComparator<PT> probeSideComparator,
            TypePairComparator<PT, BT> comparator,
            List<MemorySegment> memorySegments,
            IOManager ioManager) {
        this(
                buildSideSerializer,
                probeSideSerializer,
                buildSideComparator,
                probeSideComparator,
                comparator,
                memorySegments,
                ioManager,
                true);
    }

    public MutableHashTable(
            TypeSerializer<BT> buildSideSerializer,
            TypeSerializer<PT> probeSideSerializer,
            TypeComparator<BT> buildSideComparator,
            TypeComparator<PT> probeSideComparator,
            TypePairComparator<PT, BT> comparator,
            List<MemorySegment> memorySegments,
            IOManager ioManager,
            boolean useBloomFilters) {
        this(
                buildSideSerializer,
                probeSideSerializer,
                buildSideComparator,
                probeSideComparator,
                comparator,
                memorySegments,
                ioManager,
                DEFAULT_RECORD_LEN,
                useBloomFilters);
    }

    /**
     * 初始化
     * @param buildSideSerializer
     * @param probeSideSerializer
     * @param buildSideComparator
     * @param probeSideComparator
     * @param comparator
     * @param memorySegments  用于存储数据的内存块
     * @param ioManager
     * @param avgRecordLen
     * @param useBloomFilters
     */
    public MutableHashTable(
            TypeSerializer<BT> buildSideSerializer,
            TypeSerializer<PT> probeSideSerializer,
            TypeComparator<BT> buildSideComparator,
            TypeComparator<PT> probeSideComparator,
            TypePairComparator<PT, BT> comparator,
            List<MemorySegment> memorySegments,
            IOManager ioManager,
            int avgRecordLen,
            boolean useBloomFilters) {
        // some sanity checks first
        if (memorySegments == null) {
            throw new NullPointerException();
        }
        if (memorySegments.size() < MIN_NUM_MEMORY_SEGMENTS) {
            throw new IllegalArgumentException(
                    "Too few memory segments provided. Hash Join needs at least "
                            + MIN_NUM_MEMORY_SEGMENTS
                            + " memory segments.");
        }

        // assign the members
        this.buildSideSerializer = buildSideSerializer;
        this.probeSideSerializer = probeSideSerializer;
        this.buildSideComparator = buildSideComparator;
        this.probeSideComparator = probeSideComparator;
        this.recordComparator = comparator;
        this.availableMemory = memorySegments;
        this.ioManager = ioManager;
        this.useBloomFilters = useBloomFilters;

        // 得到记录的平均长度
        this.avgRecordLen =
                avgRecordLen > 0
                        ? avgRecordLen
                        : buildSideSerializer.getLength() == -1
                                ? DEFAULT_RECORD_LEN
                                : buildSideSerializer.getLength();

        // check the size of the first buffer and record it. all further buffers must have the same
        // size.
        // the size must also be a power of 2
        this.totalNumBuffers = memorySegments.size();
        this.segmentSize = memorySegments.get(0).size();
        if ((this.segmentSize & this.segmentSize - 1) != 0) {
            throw new IllegalArgumentException(
                    "Hash Table requires buffers whose size is a power of 2.");
        }

        // 表示每个segment维护多少个bucket
        int bucketsPerSegment = this.segmentSize >> NUM_INTRA_BUCKET_BITS;
        if (bucketsPerSegment == 0) {
            throw new IllegalArgumentException(
                    "Hash Table requires buffers of at least " + HASH_BUCKET_SIZE + " bytes.");
        }
        this.bucketsPerSegmentMask = bucketsPerSegment - 1;
        this.bucketsPerSegmentBits = MathUtils.log2strict(bucketsPerSegment);

        // take away the write behind buffers
        this.writeBehindBuffers = new LinkedBlockingQueue<MemorySegment>();
        this.numWriteBehindBuffers = getNumWriteBehindBuffers(memorySegments.size());

        this.partitionsBeingBuilt = new ArrayList<HashPartition<BT, PT>>();
        this.partitionsPending = new ArrayList<HashPartition<BT, PT>>();

        // because we allow to open and close multiple times, the state is initially closed
        this.closed.set(true);
    }

    // ------------------------------------------------------------------------
    //                              Life-Cycle
    // ------------------------------------------------------------------------

    /**
     * Opens the hash join. This method reads the build-side input and constructs the initial hash
     * table, gradually spilling partitions that do not fit into memory.
     *
     * @param buildSide Build side input.
     * @param probeSide Probe side input.
     * @throws IOException Thrown, if an I/O problem occurs while spilling a partition.
     */
    public void open(
            final MutableObjectIterator<BT> buildSide, final MutableObjectIterator<PT> probeSide)
            throws IOException {

        open(buildSide, probeSide, false);
    }

    /**
     * Opens the hash join. This method reads the build-side input and constructs the initial hash
     * table, gradually spilling partitions that do not fit into memory.
     *
     * @param buildSide Build side input.
     * @param probeSide Probe side input.
     * @param buildOuterJoin Whether outer join on build side.
     * @throws IOException Thrown, if an I/O problem occurs while spilling a partition.
     * 打开本对象 或者说初始化
     */
    public void open(
            final MutableObjectIterator<BT> buildSide,
            final MutableObjectIterator<PT> probeSide,
            boolean buildOuterJoin)
            throws IOException {

        this.buildSideOuterJoin = buildOuterJoin;

        // sanity checks
        if (!this.closed.compareAndSet(true, false)) {
            throw new IllegalStateException(
                    "Hash Join cannot be opened, because it is currently not closed.");
        }

        // grab the write behind buffers first
        // 从内存池中借走内存块   填入 numWriteBehindBuffers
        for (int i = this.numWriteBehindBuffers; i > 0; --i) {
            this.writeBehindBuffers.add(
                    this.availableMemory.remove(this.availableMemory.size() - 1));
        }
        // open builds the initial table by consuming the build-side input
        this.currentRecursionDepth = 0;

        // 使用迭代器来初始化内部表  (完成每个分区的数据构建阶段)
        buildInitialTable(buildSide);

        // the first prober is the probe-side input
        // 基于输入的探测数据 创建探测迭代器
        this.probeIterator =
                new ProbeIterator<PT>(probeSide, this.probeSideSerializer.createInstance());

        // the bucket iterator can remain constant over the time
        // 使用相关对象初始化 bucket迭代器
        this.bucketIterator =
                new HashBucketIterator<BT, PT>(
                        this.buildSideSerializer, this.recordComparator, probedSet, buildOuterJoin);
    }

    /**
     * 只有在可以探测的阶段才能调用该方法
     * @return
     * @throws IOException
     */
    protected boolean processProbeIter() throws IOException {

        // 探测迭代器在open时  会通过探测数据进行初始化
        final ProbeIterator<PT> probeIter = this.probeIterator;
        // 这个是比较器
        final TypeComparator<PT> probeAccessors = this.probeSideComparator;

        if (!this.probeMatchedPhase) {
            return false;
        }

        PT next;

        // 迭代探测数据
        while ((next = probeIter.next()) != null) {

            // currentRecursionDepth 在调用open后为0
            final int hash = hash(probeAccessors.hash(next), this.currentRecursionDepth);
            final int posHashCode = hash % this.numBuckets;

            // get the bucket for the given hash code
            final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
            final int bucketInSegmentOffset =
                    (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
            final MemorySegment bucket = this.buckets[bucketArrayPos];

            // get the basic characteristics of the bucket
            final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
            final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(partitionNumber);

            // 上面都是检索的常规操作

            // for an in-memory partition, process set the return iterators, else spill the probe
            // records
            if (p.isInMemory()) {
                // 设置好记录后 准备使用迭代器寻找记录
                this.recordComparator.setReference(next);
                this.bucketIterator.set(bucket, p.overflowSegments, p, hash, bucketInSegmentOffset);
                return true;
            } else {
                byte status = bucket.get(bucketInSegmentOffset + HEADER_STATUS_OFFSET);
                // 数据已经在布隆过滤器里了
                if (status == BUCKET_STATUS_IN_FILTER) {
                    this.bloomFilter.setBitsLocation(
                            bucket, bucketInSegmentOffset + BUCKET_HEADER_LENGTH);
                    // Use BloomFilter to filter out all the probe records which would not match any
                    // key in spilled build table buckets.
                    if (this.bloomFilter.testHash(hash)) {
                        // 将命中的数据加入探测容器
                        p.insertIntoProbeBuffer(next);
                    }
                } else {
                    // 这种情况代表bucket下没有entry
                    p.insertIntoProbeBuffer(next);
                }
            }
        }
        // -------------- partition done ---------------

        return false;
    }

    /**
     * 准备处理某个未探测过的分区
     * @return
     * @throws IOException
     */
    protected boolean processUnmatchedBuildIter() throws IOException {
        // 表示已经处理过 unmatchedIter了
        if (this.unmatchedBuildVisited) {
            return false;
        }

        // 当初始化尚未探测过的数据时  就自动退出探测阶段
        this.probeMatchedPhase = false;

        // 使用当前分区数据创建迭代器  就是挨个读取bucket的数据
        UnmatchedBuildIterator<BT, PT> unmatchedBuildIter =
                new UnmatchedBuildIterator<>(
                        this.buildSideSerializer,
                        this.numBuckets,
                        this.bucketsPerSegmentBits,
                        this.bucketsPerSegmentMask,
                        this.buckets,
                        this.partitionsBeingBuilt,
                        probedSet);
        this.unmatchedBuildIterator = unmatchedBuildIter;

        // There maybe none unmatched build element, so we add a verification here to make sure we
        // do not return (null, null) to user.
        // 表示没有数据
        if (unmatchedBuildIter.next() == null) {
            this.unmatchedBuildVisited = true;
            return false;
        }

        unmatchedBuildIter.back();

        // While visit the unmatched build elements, the probe element is null, and the
        // unmatchedBuildIterator
        // would iterate all the unmatched build elements, so we return false during the second
        // calling of this method.
        this.unmatchedBuildVisited = true;
        return true;
    }

    /**
     * 准备下个分区的数据
     * @return
     * @throws IOException
     */
    protected boolean prepareNextPartition() throws IOException {
        // finalize and cleanup the partitions of the current table
        int buffersAvailable = 0;

        // 先要清理当前的数据

        // 在open时 会将分区数据填充到partitionsBeingBuilt中
        for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
            final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(i);
            p.setFurtherPatitioning(this.furtherPartitioning);
            // 该方法会将内存块回收到 availableMemory
            buffersAvailable +=
                    p.finalizeProbePhase(
                            this.availableMemory, this.partitionsPending, this.buildSideOuterJoin);
        }

        // 清理当前分区数据
        this.partitionsBeingBuilt.clear();
        this.writeBehindBuffersAvailable += buffersAvailable;

        // 清理bucket数据
        releaseTable();

        // 如果build/probe side数据未清除 进行清除
        if (this.currentSpilledBuildSide != null) {
            this.currentSpilledBuildSide.closeAndDelete();
            this.currentSpilledBuildSide = null;
        }

        if (this.currentSpilledProbeSide != null) {
            this.currentSpilledProbeSide.closeAndDelete();
            this.currentSpilledProbeSide = null;
        }

        // 表示没有待处理的数据了 partitionsPending 维护的是还未探测数据
        if (this.partitionsPending.isEmpty()) {
            // no more data
            return false;
        }

        // there are pending partitions
        final HashPartition<BT, PT> p = this.partitionsPending.get(0);

        // 表示该分区还没有探测
        if (p.probeSideRecordCounter == 0) {
            // unprobed spilled partitions are only re-processed for a build-side outer join;
            // there is no need to create a hash table since there are no probe-side records

            List<MemorySegment> memory = new ArrayList<MemorySegment>();
            MemorySegment seg1 = getNextBuffer();
            if (seg1 != null) {
                memory.add(seg1);
                MemorySegment seg2 = getNextBuffer();
                if (seg2 != null) {
                    memory.add(seg2);
                }
            } else {
                throw new IllegalStateException(
                        "Attempting to begin reading spilled partition without any memory available");
            }

            // 要把之前刷盘的数据再读取出来  因为该分区的数据还没有探测过
            this.currentSpilledBuildSide =
                    this.ioManager.createBlockChannelReader(p.getBuildSideChannel().getChannelID());
            final ChannelReaderInputView inView =
                    new HeaderlessChannelReaderInputView(
                            currentSpilledBuildSide,
                            memory,
                            p.getBuildSideBlockCount(),
                            p.getLastSegmentLimit(),
                            false);

            // 通过该对象可以挨个读取记录
            final ChannelReaderInputViewIterator<BT> inIter =
                    new ChannelReaderInputViewIterator<BT>(
                            inView, this.availableMemory, this.buildSideSerializer);

            this.unmatchedBuildIterator = inIter;

            // 表示一个未探测过的分区数据已经准备好被重新使用了  从pending中移除
            this.partitionsPending.remove(0);

            return true;
        }

        // 表示有未处理的探测
        this.probeMatchedPhase = true;
        this.unmatchedBuildVisited = false;

        // build the next table; memory must be allocated after this call
        // 从channel中读取分区数据 并加载到内存
        buildTableFromSpilledPartition(p);

        // set the probe side - gather memory segments for reading
        LinkedBlockingQueue<MemorySegment> returnQueue = new LinkedBlockingQueue<MemorySegment>();

        // 这个是读取探测数据的
        this.currentSpilledProbeSide =
                this.ioManager.createBlockChannelReader(
                        p.getProbeSideChannel().getChannelID(), returnQueue);

        List<MemorySegment> memory = new ArrayList<MemorySegment>();
        MemorySegment seg1 = getNextBuffer();
        if (seg1 != null) {
            memory.add(seg1);
            MemorySegment seg2 = getNextBuffer();
            if (seg2 != null) {
                memory.add(seg2);
            }
        } else {
            throw new IllegalStateException(
                    "Attempting to begin probing of partition without any memory available");
        }

        ChannelReaderInputViewIterator<PT> probeReader =
                new ChannelReaderInputViewIterator<PT>(
                        this.currentSpilledProbeSide,
                        returnQueue,
                        memory,
                        this.availableMemory,
                        this.probeSideSerializer,
                        p.getProbeSideBlockCount());
        this.probeIterator.set(probeReader);

        // unregister the pending partition
        this.partitionsPending.remove(0);
        this.currentRecursionDepth = p.getRecursionLevel() + 1;

        // recursively get the next
        return nextRecord();
    }

    /**
     * 获取下一条记录
     * @return
     * @throws IOException
     */
    public boolean nextRecord() throws IOException {
        if (buildSideOuterJoin) {
            return processProbeIter() || processUnmatchedBuildIter() || prepareNextPartition();
        } else {
            return processProbeIter() || prepareNextPartition();
        }
    }

    /**
     * 获取与record匹配的记录  相当于就是探测了
     * @param record
     * @return
     * @throws IOException
     */
    public HashBucketIterator<BT, PT> getMatchesFor(PT record) throws IOException {
        final TypeComparator<PT> probeAccessors = this.probeSideComparator;
        final int hash = hash(probeAccessors.hash(record), this.currentRecursionDepth);
        final int posHashCode = hash % this.numBuckets;

        // get the bucket for the given hash code
        final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
        final int bucketInSegmentOffset =
                (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
        final MemorySegment bucket = this.buckets[bucketArrayPos];

        // get the basic characteristics of the bucket
        final int partitionNumber = bucket.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
        final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(partitionNumber);

        // 以上也是常规操作

        // for an in-memory partition, process set the return iterators, else spill the probe
        // records  要求此时分区数据还维护在内存中
        if (p.isInMemory()) {
            this.recordComparator.setReference(record);
            this.bucketIterator.set(bucket, p.overflowSegments, p, hash, bucketInSegmentOffset);
            return this.bucketIterator;
        } else {
            throw new IllegalStateException(
                    "Method is not applicable to partially spilled hash tables.");
        }
    }

    public PT getCurrentProbeRecord() {
        if (this.probeMatchedPhase) {
            return this.probeIterator.getCurrent();
        } else {
            return null;
        }
    }

    public MutableObjectIterator<BT> getBuildSideIterator() {
        if (this.probeMatchedPhase) {
            return this.bucketIterator;
        } else {
            return this.unmatchedBuildIterator;
        }
    }

    /**
     * Closes the hash table. This effectively releases all internal structures and closes all open
     * files and removes them. The call to this method is valid both as a cleanup after the complete
     * inputs were properly processed, and as an cancellation call, which cleans up all resources
     * that are currently held by the hash join.
     * 关闭本对象
     */
    public void close() {
        // make sure that we close only once
        if (!this.closed.compareAndSet(false, true)) {
            return;
        }

        // clear the iterators, so the next call to next() will notice
        this.bucketIterator = null;
        this.probeIterator = null;

        // release the table structure  释放bucket数据
        releaseTable();

        // clear the memory in the partitions  释放分区数据
        clearPartitions();

        // clear the current probe side channel, if there is one
        if (this.currentSpilledProbeSide != null) {
            try {
                this.currentSpilledProbeSide.closeAndDelete();
            } catch (Throwable t) {
                LOG.warn(
                        "Could not close and delete the temp file for the current spilled partition probe side.",
                        t);
            }
        }

        // clear the partitions that are still to be done (that have files on disk)
        // 表示有待处理的数据  现在也是直接释放
        for (int i = 0; i < this.partitionsPending.size(); i++) {
            final HashPartition<BT, PT> p = this.partitionsPending.get(i);
            p.clearAllMemory(this.availableMemory);
        }

        // return the write-behind buffers   归还buffer
        for (int i = 0; i < this.numWriteBehindBuffers + this.writeBehindBuffersAvailable; i++) {
            try {
                this.availableMemory.add(this.writeBehindBuffers.take());
            } catch (InterruptedException iex) {
                throw new RuntimeException("Hashtable closing was interrupted");
            }
        }
        this.writeBehindBuffersAvailable = 0;
    }

    public void abort() {
        this.running = false;
    }

    public List<MemorySegment> getFreedMemory() {
        if (!this.closed.get()) {
            throw new IllegalStateException("Cannot return memory while join is open.");
        }

        return this.availableMemory;
    }

    // ------------------------------------------------------------------------
    //                       Hash Table Building
    // ------------------------------------------------------------------------

    /**
     * Creates the initial hash table. This method sets up partitions, hash index, and inserts the
     * data from the given iterator.
     *
     * @param input The iterator with the build side data.
     * @throws IOException Thrown, if an element could not be fetched and deserialized from the
     *     iterator, or if serialization fails.
     *     使用迭代器的数据来初始化 内部表
     */
    protected void buildInitialTable(final MutableObjectIterator<BT> input) throws IOException {
        // create the partitions
        // 根据可用的内存块数量  产生一定数量的分区
        final int partitionFanOut = getPartitioningFanOutNoEstimates(this.availableMemory.size());
        if (partitionFanOut > MAX_NUM_PARTITIONS) {
            throw new RuntimeException(
                    "Hash join partitions estimate exeeds maximum number of partitions.");
        }
        // 创建分区
        createPartitions(partitionFanOut, 0);

        // set up the table structure. the write behind buffers are taken away, as are one buffer
        // per partition
        // 根据公式 计算出应该有多少个bucket
        final int numBuckets =
                getInitialTableSize(
                        this.availableMemory.size(),
                        this.segmentSize,
                        partitionFanOut,
                        this.avgRecordLen);

        // 初始化table  (主要就是初始化bucket)
        initTable(numBuckets, (byte) partitionFanOut);

        final TypeComparator<BT> buildTypeComparator = this.buildSideComparator;
        BT record = this.buildSideSerializer.createInstance();

        // go over the complete input and insert every element into the hash table
        // 不断取出数据 用于填充table
        while (this.running && ((record = input.next(record)) != null)) {
            final int hashCode = hash(buildTypeComparator.hash(record), 0);
            insertIntoTable(record, hashCode);
        }

        if (!this.running) {
            return;
        }

        // finalize the partitions
        // 此时已经填充完各分区的数据了
        for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
            HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(i);
            // 表示完成了数据构建阶段
            p.finalizeBuildPhase(this.ioManager, this.currentEnumerator, this.writeBehindBuffers);
        }
    }

    /**
     * 初始化布隆过滤器   布隆过滤器就是可能会误判存在  但是不会误判不存在
     * @param numBuckets
     */
    private void initBloomFilter(int numBuckets) {
        int avgNumRecordsPerBucket =
                getEstimatedMaxBucketEntries(
                        this.availableMemory.size(),
                        this.segmentSize,
                        numBuckets,
                        this.avgRecordLen);
        // Assign all bucket size to bloom filter except bucket header length.
        int byteSize = HASH_BUCKET_SIZE - BUCKET_HEADER_LENGTH;
        this.bloomFilter = new BloomFilter(avgNumRecordsPerBucket, byteSize);
        if (LOG.isDebugEnabled()) {
            double fpp =
                    BloomFilter.estimateFalsePositiveProbability(
                            avgNumRecordsPerBucket, byteSize << 3);
            LOG.debug(
                    String.format(
                            "Create BloomFilter with average input entries per bucket[%d], bytes size[%d], false positive probability[%f].",
                            avgNumRecordsPerBucket, byteSize, fpp));
        }
    }

    private int getEstimatedMaxBucketEntries(
            int numBuffers, int bufferSize, int numBuckets, int recordLenBytes) {
        final long totalSize = ((long) bufferSize) * numBuffers;
        final long numRecordsStorable = totalSize / (recordLenBytes + RECORD_TABLE_BYTES);
        final long maxNumRecordsStorable = (MAX_RECURSION_DEPTH + 1) * numRecordsStorable;
        final long maxNumRecordsPerBucket = maxNumRecordsStorable / numBuckets;
        return (int) maxNumRecordsPerBucket;
    }

    /**
     * p中有未处理的探测数据   并且此时分区数据在channel中
     * @param p   需要处理的分区数据
     * @throws IOException
     */
    protected void buildTableFromSpilledPartition(final HashPartition<BT, PT> p)
            throws IOException {

        // 每当将一个磁盘数据恢复到内存中 该分区就要增加一个level
        final int nextRecursionLevel = p.getRecursionLevel() + 1;
        if (nextRecursionLevel > MAX_RECURSION_DEPTH) {
            throw new RuntimeException(
                    "Hash join exceeded maximum number of recursions, without reducing "
                            + "partitions enough to be memory resident. Probably cause: Too many duplicate keys.");
        }

        // we distinguish two cases here:
        // 1) The partition fits entirely into main memory. That is the case if we have enough
        // buffers for
        //    all partition segments, plus enough buffers to hold the table structure.
        //    --> We read the partition in as it is and create a hashtable that references only
        //        that single partition.
        // 2) We can not guarantee that enough memory segments are available and read the partition
        //    in, distributing its data among newly created partitions.
        final int totalBuffersAvailable =
                this.availableMemory.size() + this.writeBehindBuffersAvailable;
        if (totalBuffersAvailable != this.totalNumBuffers - this.numWriteBehindBuffers) {
            throw new RuntimeException(
                    "Hash Join bug in memory management: Memory buffers leaked.");
        }

        // 表示这些记录要消耗多少bucket
        long numBuckets = p.getBuildSideRecordCount() / NUM_ENTRIES_PER_BUCKET + 1;

        // we need to consider the worst case where everything hashes to one bucket which needs to
        // overflow by the same
        // number of total buckets again. Also, one buffer needs to remain for the probing
        // 需要消耗的segment
        final long totalBuffersNeeded =
                2 * (numBuckets / (this.bucketsPerSegmentMask + 1))
                        + p.getBuildSideBlockCount()
                        + 2;

        // 此时segment足够
        if (totalBuffersNeeded < totalBuffersAvailable) {
            // we are guaranteed to stay in memory
            ensureNumBuffersReturned(p.getBuildSideBlockCount());

            // first read the partition in
            // 该reader用于读取之前写入channel的数据   初始化后就会立即发出read请求 将数据加载到 availableMemory
            final BulkBlockChannelReader reader =
                    this.ioManager.createBulkBlockChannelReader(
                            p.getBuildSideChannel().getChannelID(),
                            this.availableMemory,
                            p.getBuildSideBlockCount());
            // call waits until all is read
            // 表示要保留分区数据   调用close/closeAndDelete 会等待所有req处理完 也就是已经完成了数据加载
            if (keepBuildSidePartitions && p.recursionLevel == 0) {
                reader.close(); // keep the partitions
            } else {
                // 删除下层数据
                reader.closeAndDelete();
            }

            final List<MemorySegment> partitionBuffers = reader.getFullSegments();

            // 重新构建分区对象
            final HashPartition<BT, PT> newPart =
                    new HashPartition<BT, PT>(
                            this.buildSideSerializer,
                            this.probeSideSerializer,
                            0,
                            nextRecursionLevel,
                            partitionBuffers,
                            p.getBuildSideRecordCount(),
                            this.segmentSize,
                            p.getLastSegmentLimit());

            this.partitionsBeingBuilt.add(newPart);

            // erect the buckets
            // 为该分区分配bucket
            initTable((int) numBuckets, (byte) 1);

            // now, index the partition through a hash table
            final HashPartition<BT, PT>.PartitionIterator pIter =
                    newPart.getPartitionIterator(this.buildSideComparator);
            BT record = this.buildSideSerializer.createInstance();

            // 迭代分区数据
            while ((record = pIter.next(record)) != null) {
                final int hashCode = hash(pIter.getCurrentHashCode(), nextRecursionLevel);
                final int posHashCode = hashCode % this.numBuckets;
                final long pointer = pIter.getPointer();

                // get the bucket for the given hash code
                final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
                final int bucketInSegmentPos =
                        (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
                final MemorySegment bucket = this.buckets[bucketArrayPos];

                // 这里是补充bucket信息
                insertBucketEntry(newPart, bucket, bucketInSegmentPos, hashCode, pointer, false);
            }
        } else {
            // we need to partition and partially spill
            final int avgRecordLenPartition =
                    (int)
                            (((long) p.getBuildSideBlockCount())
                                    * this.segmentSize
                                    / p.getBuildSideRecordCount());

            final int bucketCount =
                    getInitialTableSize(
                            totalBuffersAvailable,
                            this.segmentSize,
                            getPartitioningFanOutNoEstimates(totalBuffersAvailable),
                            avgRecordLenPartition);

            // compute in how many splits, we'd need to partition the result
            final int splits = (int) (totalBuffersNeeded / totalBuffersAvailable) + 1;

            // 需要拆分成多个分区
            final int partitionFanOut =
                    Math.min(10 * splits /* being conservative */, MAX_NUM_PARTITIONS);

            createPartitions(partitionFanOut, nextRecursionLevel);

            // set up the table structure. the write behind buffers are taken away, as are one
            // buffer per partition
            initTable(bucketCount, (byte) partitionFanOut);

            // go over the complete input and insert every element into the hash table
            // first set up the reader with some memory.
            final List<MemorySegment> segments = new ArrayList<MemorySegment>(2);
            segments.add(getNextBuffer());
            segments.add(getNextBuffer());

            // 用于读取之前的分区数据
            final BlockChannelReader<MemorySegment> inReader =
                    this.ioManager.createBlockChannelReader(p.getBuildSideChannel().getChannelID());
            final ChannelReaderInputView inView =
                    new HeaderlessChannelReaderInputView(
                            inReader,
                            segments,
                            p.getBuildSideBlockCount(),
                            p.getLastSegmentLimit(),
                            false);
            final ChannelReaderInputViewIterator<BT> inIter =
                    new ChannelReaderInputViewIterator<BT>(
                            inView, this.availableMemory, this.buildSideSerializer);
            final TypeComparator<BT> btComparator = this.buildSideComparator;
            BT rec = this.buildSideSerializer.createInstance();
            while ((rec = inIter.next(rec)) != null) {
                // 将数据重新插入各分区  同时设置bucket
                final int hashCode = hash(btComparator.hash(rec), nextRecursionLevel);
                insertIntoTable(rec, hashCode);
            }

            if (keepBuildSidePartitions && p.recursionLevel == 0) {
                inReader.close(); // keep the partitions
            } else {
                inReader.closeAndDelete();
            }

            // finalize the partitions
            // 结束构建阶段
            for (int i = 0; i < this.partitionsBeingBuilt.size(); i++) {
                HashPartition<BT, PT> part = this.partitionsBeingBuilt.get(i);
                part.finalizeBuildPhase(
                        this.ioManager, this.currentEnumerator, this.writeBehindBuffers);
            }
        }
    }

    /**
     * 将数据插入table
     * @param record   记录本身
     * @param hashCode    记录对应的hash值
     * @throws IOException
     */
    protected final void insertIntoTable(final BT record, final int hashCode) throws IOException {
        final int posHashCode = hashCode % this.numBuckets;

        // get the bucket for the given hash code
        // 找到所在的segment
        final int bucketArrayPos = posHashCode >> this.bucketsPerSegmentBits;
        // 找到bucket的起始位置
        final int bucketInSegmentPos =
                (posHashCode & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
        final MemorySegment bucket = this.buckets[bucketArrayPos];

        // get the basic characteristics of the bucket
        final int partitionNumber = bucket.get(bucketInSegmentPos + HEADER_PARTITION_OFFSET);

        // get the partition descriptor for the bucket
        if (partitionNumber < 0 || partitionNumber >= this.partitionsBeingBuilt.size()) {
            throw new RuntimeException(
                    "Error: Hash structures in Hash-Join are corrupt. Invalid partition number for bucket.");
        }

        // 找到存储分区数据的对象
        final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(partitionNumber);

        // --------- Step 1: Get the partition for this pair and put the pair into the buffer
        // ---------

        // 往分区中插入数据
        long pointer = p.insertIntoBuildBuffer(record);
        if (pointer != -1) {
            // record was inserted into an in-memory partition. a pointer must be inserted into the
            // buckets
            // 将pointer信息保存在bucket中  如果bucket存满了 就利用overflow
            insertBucketEntry(p, bucket, bucketInSegmentPos, hashCode, pointer, true);
        } else {
            // 当数据被直接写入channel时 返回-1 进入该分支

            byte status = bucket.get(bucketInSegmentPos + HEADER_STATUS_OFFSET);
            if (status == BUCKET_STATUS_IN_FILTER) {
                // While partition has been spilled, relocation bloom filter bits for current
                // bucket,
                // and build bloom filter with hashcode.
                // 加入到布隆过滤器
                this.bloomFilter.setBitsLocation(bucket, bucketInSegmentPos + BUCKET_HEADER_LENGTH);
                this.bloomFilter.addHash(hashCode);
            }
        }
    }

    /**
     * 把某条记录的pointer写入bucket
     * @param p
     * @param bucket
     * @param bucketInSegmentPos
     * @param hashCode
     * @param pointer
     * @param spillingAllowed
     * @throws IOException
     */
    final void insertBucketEntry(
            final HashPartition<BT, PT> p,
            final MemorySegment bucket,
            final int bucketInSegmentPos,
            final int hashCode,
            final long pointer,
            final boolean spillingAllowed)
            throws IOException {
        // find the position to put the hash code and pointer
        final int count = bucket.getShort(bucketInSegmentPos + HEADER_COUNT_OFFSET);
        // 还没有达到单个bucket的上限  直接添加
        if (count < NUM_ENTRIES_PER_BUCKET) {
            // we are good in our current bucket, put the values
            bucket.putInt(
                    bucketInSegmentPos + BUCKET_HEADER_LENGTH + (count * HASH_CODE_LEN),
                    hashCode); // hash code
            bucket.putLong(
                    bucketInSegmentPos + BUCKET_POINTER_START_OFFSET + (count * POINTER_LEN),
                    pointer); // pointer
            bucket.putShort(
                    bucketInSegmentPos + HEADER_COUNT_OFFSET, (short) (count + 1)); // update count
        } else {
            // we need to go to the overflow buckets
            final long originalForwardPointer =
                    bucket.getLong(bucketInSegmentPos + HEADER_FORWARD_OFFSET);
            final long forwardForNewBucket;

            if (originalForwardPointer != BUCKET_FORWARD_POINTER_NOT_SET) {

                // forward pointer set
                final int overflowSegNum = (int) (originalForwardPointer >>> 32);
                final int segOffset = (int) originalForwardPointer;
                final MemorySegment seg = p.overflowSegments[overflowSegNum];

                final short obCount = seg.getShort(segOffset + HEADER_COUNT_OFFSET);

                // check if there is space in this overflow bucket
                if (obCount < NUM_ENTRIES_PER_BUCKET) {
                    // space in this bucket and we are done
                    seg.putInt(
                            segOffset + BUCKET_HEADER_LENGTH + (obCount * HASH_CODE_LEN),
                            hashCode); // hash code
                    seg.putLong(
                            segOffset + BUCKET_POINTER_START_OFFSET + (obCount * POINTER_LEN),
                            pointer); // pointer
                    seg.putShort(
                            segOffset + HEADER_COUNT_OFFSET, (short) (obCount + 1)); // update count
                    return;
                } else {
                    // no space here, we need a new bucket. this current overflow bucket will be the
                    // target of the new overflow bucket
                    forwardForNewBucket = originalForwardPointer;
                }
            } else {
                // no overflow bucket yet, so we need a first one
                forwardForNewBucket = BUCKET_FORWARD_POINTER_NOT_SET;
            }

            // we need a new overflow bucket
            MemorySegment overflowSeg;
            final int overflowBucketNum;
            final int overflowBucketOffset;

            // first, see if there is space for an overflow bucket remaining in the last overflow
            // segment
            if (p.nextOverflowBucket == 0) {
                // no space left in last bucket, or no bucket yet, so create an overflow segment
                overflowSeg = getNextBuffer();

                // 这里表示申请不到segment了
                if (overflowSeg == null) {
                    // no memory available to create overflow bucket. we need to spill a partition
                    if (!spillingAllowed) {
                        throw new IOException(
                                "Hashtable memory ran out in a non-spillable situation. "
                                        + "This is probably related to wrong size calculations.");
                    }
                    // 尝试释放内存
                    final int spilledPart = spillPartition();
                    // 数据已经不再维护在内存中了  也就不需要维护overflow了
                    if (spilledPart == p.getPartitionNumber()) {
                        // this bucket is no longer in-memory
                        return;
                    }
                    overflowSeg = getNextBuffer();
                    if (overflowSeg == null) {
                        throw new RuntimeException(
                                "Bug in HybridHashJoin: No memory became available after spilling a partition.");
                    }
                }
                overflowBucketOffset = 0;
                overflowBucketNum = p.numOverflowSegments;

                // add the new overflow segment
                if (p.overflowSegments.length <= p.numOverflowSegments) {
                    MemorySegment[] newSegsArray = new MemorySegment[p.overflowSegments.length * 2];
                    System.arraycopy(
                            p.overflowSegments, 0, newSegsArray, 0, p.overflowSegments.length);
                    p.overflowSegments = newSegsArray;
                }
                p.overflowSegments[p.numOverflowSegments] = overflowSeg;
                p.numOverflowSegments++;
            } else {
                // there is space in the last overflow bucket
                overflowBucketNum = p.numOverflowSegments - 1;
                overflowSeg = p.overflowSegments[overflowBucketNum];
                overflowBucketOffset = p.nextOverflowBucket << NUM_INTRA_BUCKET_BITS;
            }

            // next overflow bucket is one ahead. if the segment is full, the next will be at the
            // beginning
            // of a new segment
            p.nextOverflowBucket =
                    (p.nextOverflowBucket == this.bucketsPerSegmentMask
                            ? 0
                            : p.nextOverflowBucket + 1);

            // insert the new overflow bucket in the chain of buckets
            // 1) set the old forward pointer
            // 2) let the bucket in the main table point to this one
            overflowSeg.putLong(overflowBucketOffset + HEADER_FORWARD_OFFSET, forwardForNewBucket);
            final long pointerToNewBucket =
                    (((long) overflowBucketNum) << 32) | ((long) overflowBucketOffset);
            bucket.putLong(bucketInSegmentPos + HEADER_FORWARD_OFFSET, pointerToNewBucket);

            // finally, insert the values into the overflow buckets
            overflowSeg.putInt(overflowBucketOffset + BUCKET_HEADER_LENGTH, hashCode); // hash code
            overflowSeg.putLong(
                    overflowBucketOffset + BUCKET_POINTER_START_OFFSET, pointer); // pointer

            // set the count to one
            overflowSeg.putShort(overflowBucketOffset + HEADER_COUNT_OFFSET, (short) 1);

            // initiate the probed bitset to 0.
            overflowSeg.putShort(overflowBucketOffset + HEADER_PROBED_FLAGS_OFFSET, (short) 0);
        }
    }

    // --------------------------------------------------------------------------------------------
    //                          Setup and Tear Down of Structures
    // --------------------------------------------------------------------------------------------

    /**
     * Returns a new inMemoryPartition object. This is required as a plug for
     * ReOpenableMutableHashTable.
     * 创建分区对象 存储数据
     */
    protected HashPartition<BT, PT> getNewInMemoryPartition(int number, int recursionLevel) {
        return new HashPartition<BT, PT>(
                this.buildSideSerializer,
                this.probeSideSerializer,
                number,
                recursionLevel,
                this.availableMemory.remove(this.availableMemory.size() - 1),
                this,
                this.segmentSize);
    }

    /**
     * 创建分区
     * @param numPartitions
     * @param recursionLevel  递归层数  最外层是0
     */
    protected void createPartitions(int numPartitions, int recursionLevel) {
        // sanity check
        ensureNumBuffersReturned(numPartitions);

        // 该对象可以遍历一组目录 每个目录对应一条线程
        this.currentEnumerator = this.ioManager.createChannelEnumerator();

        // 清空原来的分区
        this.partitionsBeingBuilt.clear();
        for (int i = 0; i < numPartitions; i++) {
            HashPartition<BT, PT> p = getNewInMemoryPartition(i, recursionLevel);
            this.partitionsBeingBuilt.add(p);
        }
    }

    /**
     * This method clears all partitions currently residing (partially) in memory. It releases all
     * memory and deletes all spilled partitions.
     *
     * <p>This method is intended for a hard cleanup in the case that the join is aborted.
     * 清除所有分区的数据
     */
    protected void clearPartitions() {
        for (int i = this.partitionsBeingBuilt.size() - 1; i >= 0; --i) {
            final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(i);
            try {
                p.clearAllMemory(this.availableMemory);
            } catch (Exception e) {
                LOG.error("Error during partition cleanup.", e);
            }
        }
        this.partitionsBeingBuilt.clear();
    }

    /**
     * 初始化table
     * @param numBuckets   总计应当有多少bucket
     * @param numPartitions   有多少个分区
     */
    protected void initTable(int numBuckets, byte numPartitions) {
        final int bucketsPerSegment = this.bucketsPerSegmentMask + 1;
        final int numSegs =
                (numBuckets >>> this.bucketsPerSegmentBits)
                        + ((numBuckets & this.bucketsPerSegmentMask) == 0 ? 0 : 1);

        // 表示总计需要这么多个segment来存储bucket
        final MemorySegment[] table = new MemorySegment[numSegs];

        ensureNumBuffersReturned(numSegs);

        // go over all segments that are part of the table
        // 开始进行初始化操作
        for (int i = 0, bucket = 0; i < numSegs && bucket < numBuckets; i++) {
            final MemorySegment seg = getNextBuffer();

            // go over all buckets in the segment
            // 这个操作跟 CompactingHashTable 初始化bucket是一样的
            for (int k = 0; k < bucketsPerSegment && bucket < numBuckets; k++, bucket++) {
                final int bucketOffset = k * HASH_BUCKET_SIZE;

                // compute the partition that the bucket corresponds to
                final byte partition = assignPartition(bucket, numPartitions);

                // initialize the header fields
                seg.put(bucketOffset + HEADER_PARTITION_OFFSET, partition);
                // 初始状态  该bucket所指向的数据会在内存中
                seg.put(bucketOffset + HEADER_STATUS_OFFSET, BUCKET_STATUS_IN_MEMORY);
                seg.putShort(bucketOffset + HEADER_COUNT_OFFSET, (short) 0);
                seg.putLong(bucketOffset + HEADER_FORWARD_OFFSET, BUCKET_FORWARD_POINTER_NOT_SET);
                seg.putShort(bucketOffset + HEADER_PROBED_FLAGS_OFFSET, (short) 0);
            }

            table[i] = seg;
        }
        this.buckets = table;
        this.numBuckets = numBuckets;

        if (useBloomFilters) {
            initBloomFilter(numBuckets);
        }
    }

    /**
     * Releases the table (the array of buckets) and returns the occupied memory segments to the
     * list of free segments.
     * 释放bucket数据
     */
    protected void releaseTable() {
        // set the counters back
        this.numBuckets = 0;

        if (this.buckets != null) {
            for (MemorySegment bucket : this.buckets) {
                this.availableMemory.add(bucket);
            }
            this.buckets = null;
        }
    }

    // --------------------------------------------------------------------------------------------
    //                                    Memory Handling
    // --------------------------------------------------------------------------------------------

    /**
     * Selects a partition and spills it. The number of the spilled partition is returned.
     *
     * @return The number of the spilled partition.
     * 将分区内存数据倾泻到channel
     */
    protected int spillPartition() throws IOException {
        // find the largest partition
        ArrayList<HashPartition<BT, PT>> partitions = this.partitionsBeingBuilt;
        int largestNumBlocks = 0;
        int largestPartNum = -1;

        // 找到占用内存最多的分区  且此时还没有设置channel
        for (int i = 0; i < partitions.size(); i++) {
            HashPartition<BT, PT> p = partitions.get(i);
            if (p.isInMemory() && p.getNumOccupiedMemorySegments() > largestNumBlocks) {
                largestNumBlocks = p.getNumOccupiedMemorySegments();
                largestPartNum = i;
            }
        }
        final HashPartition<BT, PT> p = partitions.get(largestPartNum);

        // 当某个分区的数据要进入channel后 需要使用分区数据构建布隆过滤器
        if (useBloomFilters) {
            buildBloomFilterForBucketsInPartition(largestPartNum, p);
        }

        // spill the partition
        // 这样操作 将会丢失该分区相关的overflow队列
        int numBuffersFreed =
                p.spillPartition(
                        this.availableMemory,
                        this.ioManager,
                        this.currentEnumerator.next(),
                        this.writeBehindBuffers);
        // 返回的代表释放的内存数量
        this.writeBehindBuffersAvailable += numBuffersFreed;
        // grab as many buffers as are available directly
        MemorySegment currBuff;
        while (this.writeBehindBuffersAvailable > 0
                && (currBuff = this.writeBehindBuffers.poll()) != null) {
            this.availableMemory.add(currBuff);
            this.writeBehindBuffersAvailable--;
        }
        return largestPartNum;
    }

    /**
     * 当分区数据要进入channel时 就要使用数据来填充布隆过滤器
     * @param partNum
     * @param partition
     */
    protected final void buildBloomFilterForBucketsInPartition(
            int partNum, HashPartition<BT, PT> partition) {
        // Find all the buckets which belongs to this partition, and build bloom filter for each
        // bucket(include its overflow buckets).
        final int bucketsPerSegment = this.bucketsPerSegmentMask + 1;

        int numSegs = this.buckets.length;
        // go over all segments that are part of the table
        // 外层遍历segment
        for (int i = 0, bucket = 0; i < numSegs && bucket < numBuckets; i++) {
            final MemorySegment segment = this.buckets[i];
            // go over all buckets in the segment
            // 内层遍历该分区的每个bucket
            for (int k = 0; k < bucketsPerSegment && bucket < numBuckets; k++, bucket++) {
                // 找到该bucket在segment的位置
                final int bucketInSegmentOffset = k * HASH_BUCKET_SIZE;
                byte partitionNumber = segment.get(bucketInSegmentOffset + HEADER_PARTITION_OFFSET);
                if (partitionNumber == partNum) {
                    byte status = segment.get(bucketInSegmentOffset + HEADER_STATUS_OFFSET);
                    // 默认就是该状态 现在要开始处理
                    if (status == BUCKET_STATUS_IN_MEMORY) {
                        buildBloomFilterForBucket(bucketInSegmentOffset, segment, partition);
                    }
                }
            }
        }
    }

    /**
     * Set all the bucket memory except bucket header as the bit set of bloom filter, and use hash
     * code of build records to build bloom filter.
     * 使用某个bucket的数据来构建布隆过滤器
     */
    final void buildBloomFilterForBucket(
            int bucketInSegmentPos, MemorySegment bucket, HashPartition<BT, PT> p) {
        final int count = bucket.getShort(bucketInSegmentPos + HEADER_COUNT_OFFSET);
        // 表示该bucket下没有entry信息 不需要处理
        if (count <= 0) {
            return;
        }

        int[] hashCodes = new int[count];
        // As the hashcode and bloom filter occupy same bytes, so we read all hashcode out at first
        // and then write back to bloom filter.
        // 把落在该bucket的hash值加载出来
        for (int i = 0; i < count; i++) {
            hashCodes[i] =
                    bucket.getInt(bucketInSegmentPos + BUCKET_HEADER_LENGTH + i * HASH_CODE_LEN);
        }
        // 借助该对象为bucket设置特殊值
        this.bloomFilter.setBitsLocation(bucket, bucketInSegmentPos + BUCKET_HEADER_LENGTH);
        for (int hashCode : hashCodes) {
            // 将hash转换成一位
            this.bloomFilter.addHash(hashCode);
        }
        // 开始处理overflow的数据
        buildBloomFilterForExtraOverflowSegments(bucketInSegmentPos, bucket, p);
    }

    /**
     * 将 overflow里的hash值 转换成位图中的bit
     * @param bucketInSegmentPos
     * @param bucket
     * @param p
     */
    private void buildBloomFilterForExtraOverflowSegments(
            int bucketInSegmentPos, MemorySegment bucket, HashPartition<BT, PT> p) {
        int totalCount = 0;
        boolean skip = false;
        long forwardPointer = bucket.getLong(bucketInSegmentPos + HEADER_FORWARD_OFFSET);

        // 开始遍历overflow的entry
        while (forwardPointer != BUCKET_FORWARD_POINTER_NOT_SET) {
            final int overflowSegNum = (int) (forwardPointer >>> 32);
            if (overflowSegNum < 0 || overflowSegNum >= p.numOverflowSegments) {
                skip = true;
                break;
            }
            MemorySegment overflowSegment = p.overflowSegments[overflowSegNum];
            int bucketInOverflowSegmentOffset = (int) forwardPointer;

            final int count =
                    overflowSegment.getShort(bucketInOverflowSegmentOffset + HEADER_COUNT_OFFSET);
            totalCount += count;
            // The bits size of bloom filter per bucket is 112 * 8, while expected input entries is
            // greater than 2048, the fpp would higher than 0.9,
            // which make the bloom filter an overhead instead of optimization.
            // 当数量过多时  放弃维护索引数据
            if (totalCount > 2048) {
                skip = true;
                break;
            }

            for (int i = 0; i < count; i++) {
                int hashCode =
                        overflowSegment.getInt(
                                bucketInOverflowSegmentOffset
                                        + BUCKET_HEADER_LENGTH
                                        + i * HASH_CODE_LEN);
                // 将hash值转换成bit
                this.bloomFilter.addHash(hashCode);
            }

            forwardPointer =
                    overflowSegment.getLong(bucketInOverflowSegmentOffset + HEADER_FORWARD_OFFSET);
        }

        // 表示bucket有关entry的信息已经转移到布隆过滤器里了 并且只有当分区数据不再维护在内存中时才会这样
        if (!skip) {
            bucket.put(bucketInSegmentPos + HEADER_STATUS_OFFSET, BUCKET_STATUS_IN_FILTER);
        }
    }

    /**
     * This method makes sure that at least a certain number of memory segments is in the list of
     * free segments. Free memory can be in the list of free segments, or in the return-queue where
     * segments used to write behind are put. The number of segments that are in that return-queue,
     * but are actually reclaimable is tracked. This method makes sure at least a certain number of
     * buffers is reclaimed.
     *
     * @param minRequiredAvailable The minimum number of buffers that needs to be reclaimed.
     *                             检查能否提供这么多数量的buffer
     */
    final void ensureNumBuffersReturned(final int minRequiredAvailable) {
        if (minRequiredAvailable > this.availableMemory.size() + this.writeBehindBuffersAvailable) {
            throw new IllegalArgumentException(
                    "More buffers requested available than totally available.");
        }

        try {
            while (this.availableMemory.size() < minRequiredAvailable) {
                this.availableMemory.add(this.writeBehindBuffers.take());
                this.writeBehindBuffersAvailable--;
            }
        } catch (InterruptedException iex) {
            throw new RuntimeException("Hash Join was interrupted.");
        }
    }

    /**
     * Gets the next buffer to be used with the hash-table, either for an in-memory partition, or
     * for the table buckets. This method returns <tt>null</tt>, if no more buffer is available.
     * Spilling a partition may free new buffers then.
     *
     * @return The next buffer to be used by the hash-table, or null, if no buffer remains.
     * 获取一个内存块
     */
    final MemorySegment getNextBuffer() {
        // check if the list directly offers memory
        int s = this.availableMemory.size();
        if (s > 0) {
            return this.availableMemory.remove(s - 1);
        }

        // check if there are write behind buffers that actually are to be used for the hash table
        // 使用后备buffer
        if (this.writeBehindBuffersAvailable > 0) {
            // grab at least one, no matter what
            MemorySegment toReturn;
            try {
                toReturn = this.writeBehindBuffers.take();
            } catch (InterruptedException iex) {
                throw new RuntimeException(
                        "Hybrid Hash Join was interrupted while taking a buffer.");
            }
            this.writeBehindBuffersAvailable--;

            // grab as many more buffers as are available directly
            MemorySegment currBuff;
            while (this.writeBehindBuffersAvailable > 0
                    && (currBuff = this.writeBehindBuffers.poll()) != null) {
                this.availableMemory.add(currBuff);
                this.writeBehindBuffersAvailable--;
            }
            return toReturn;
        } else {
            // no memory available
            return null;
        }
    }

    /**
     * This is the method called by the partitions to request memory to serialize records. It
     * automatically spills partitions, if memory runs out.
     *
     * @return The next available memory segment.
     */
    @Override
    public MemorySegment nextSegment() {
        final MemorySegment seg = getNextBuffer();
        if (seg != null) {
            return seg;
        } else {
            try {
                // 当内存块不足时  为各分区设置channel  并将囤积的数据写入 以便释放内存
                spillPartition();
            } catch (IOException ioex) {
                throw new RuntimeException(
                        "Error spilling Hash Join Partition"
                                + (ioex.getMessage() == null ? "." : ": " + ioex.getMessage()),
                        ioex);
            }

            MemorySegment fromSpill = getNextBuffer();
            if (fromSpill == null) {
                throw new RuntimeException(
                        "BUG in Hybrid Hash Join: Spilling did not free a buffer.");
            } else {
                return fromSpill;
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //                             Utility Computational Functions
    // --------------------------------------------------------------------------------------------

    /**
     * Determines the number of buffers to be used for asynchronous write behind. It is currently
     * computed as the logarithm of the number of buffers to the base 4, rounded up, minus 2. The
     * upper limit for the number of write behind buffers is however set to six.
     *
     * @param numBuffers The number of available buffers.
     * @return The number
     */
    public static int getNumWriteBehindBuffers(int numBuffers) {
        int numIOBufs = (int) (Math.log(numBuffers) / Math.log(4) - 1.5);
        return numIOBufs > 6 ? 6 : numIOBufs;
    }

    /**
     * Gets the number of partitions to be used for an initial hash-table, when no estimates are
     * available.
     *
     * <p>The current logic makes sure that there are always between 10 and 127 partitions, and
     * close to 0.1 of the number of buffers.
     *
     * @param numBuffers The number of buffers available.
     * @return The number of partitions to use.
     */
    public static int getPartitioningFanOutNoEstimates(int numBuffers) {
        return Math.max(10, Math.min(numBuffers / 10, MAX_NUM_PARTITIONS));
    }

    public static int getInitialTableSize(
            int numBuffers, int bufferSize, int numPartitions, int recordLenBytes) {

        // ----------------------------------------------------------------------------------------
        // the following observations hold:
        // 1) If the records are assumed to be very large, then many buffers need to go to the
        // partitions
        //    and fewer to the table
        // 2) If the records are small, then comparatively many have to go to the buckets, and fewer
        // to the
        //    partitions
        // 3) If the bucket-table is chosen too small, we will eventually get many collisions and
        // will grow the
        //    hash table, incrementally adding buffers.
        // 4) If the bucket-table is chosen to be large and we actually need more buffers for the
        // partitions, we
        //    cannot subtract them afterwards from the table
        //
        // ==> We start with a comparatively small hash-table. We aim for a 200% utilization of the
        // bucket table
        //     when all the partition buffers are full. Most likely, that will cause some buckets to
        // be re-hashed
        //     and grab additional buffers away from the partitions.
        // NOTE: This decision may be subject to changes after conclusive experiments!
        // ----------------------------------------------------------------------------------------

        final long totalSize = ((long) bufferSize) * numBuffers;
        final long numRecordsStorable = totalSize / (recordLenBytes + RECORD_TABLE_BYTES);
        final long bucketBytes = numRecordsStorable * RECORD_TABLE_BYTES;
        final long numBuckets = bucketBytes / (2 * HASH_BUCKET_SIZE) + 1;

        return numBuckets > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) numBuckets;
    }

    /**
     * Assigns a partition to a bucket.
     *
     * @param bucket The bucket to get the partition for.
     * @param numPartitions The number of partitions.
     * @return The partition for the bucket.
     */
    public static byte assignPartition(int bucket, byte numPartitions) {
        return (byte) (bucket % numPartitions);
    }

    /**
     * The level parameter is needed so that we can have different hash functions when we
     * recursively apply the partitioning, so that the working set eventually fits into memory.
     */
    public static int hash(int code, int level) {
        final int rotation = level * 11;

        code = Integer.rotateLeft(code, rotation);

        return MathUtils.jenkinsHash(code);
    }

    public TypeComparator<PT> getProbeSideComparator() {
        return this.probeSideComparator;
    }

    // ======================================================================================================

    /**
     * 借助bucket查找 hash和equals都匹配的数据
     * */
    public static class HashBucketIterator<BT, PT> implements MutableObjectIterator<BT> {

        private final TypeSerializer<BT> accessor;

        private final TypePairComparator<PT, BT> comparator;

        private MemorySegment bucket;

        private MemorySegment[] overflowSegments;

        private HashPartition<BT, PT> partition;

        private int bucketInSegmentOffset;

        private int searchHashCode;

        private int posInSegment;

        private int countInSegment;

        private int numInSegment;

        private int originalBucketInSegmentOffset;

        private MemorySegment originalBucket;

        private long lastPointer;

        private BitSet probedSet;

        private boolean isBuildOuterJoin = false;

        HashBucketIterator(
                TypeSerializer<BT> accessor,
                TypePairComparator<PT, BT> comparator,
                BitSet probedSet,
                boolean isBuildOuterJoin) {
            this.accessor = accessor;
            this.comparator = comparator;
            this.probedSet = probedSet;
            this.isBuildOuterJoin = isBuildOuterJoin;
        }

        /**
         * 在每次迭代前 要先设置一些属性
         * @param bucket   本次扫描的bucket
         * @param overflowSegments
         * @param partition  本次处理的分区
         * @param searchHashCode   本次要检索的hash
         * @param bucketInSegmentOffset
         */
        void set(
                MemorySegment bucket,
                MemorySegment[] overflowSegments,
                HashPartition<BT, PT> partition,
                int searchHashCode,
                int bucketInSegmentOffset) {
            this.bucket = bucket;
            this.originalBucket = bucket;
            this.overflowSegments = overflowSegments;
            this.partition = partition;
            this.searchHashCode = searchHashCode;
            this.bucketInSegmentOffset = bucketInSegmentOffset;
            this.originalBucketInSegmentOffset = bucketInSegmentOffset;
            this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
            this.countInSegment = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
            this.numInSegment = 0;
        }

        /**
         * 获取下一条记录
         * @param reuse The target object into which to place next element if E is mutable.
         * @return
         */
        public BT next(BT reuse) {
            // loop over all segments that are involved in the bucket (original bucket plus overflow
            // buckets)
            while (true) {
                // 设置位图此时处理的segment
                probedSet.setMemorySegment(
                        bucket, this.bucketInSegmentOffset + HEADER_PROBED_FLAGS_OFFSET);

                // 在set()时 已经确定了bucket的位置
                while (this.numInSegment < this.countInSegment) {

                    final int thisCode = this.bucket.getInt(this.posInSegment);
                    this.posInSegment += HASH_CODE_LEN;

                    // check if the hash code matches
                    // hash匹配 然后获取在partition数据块中的pointer
                    if (thisCode == this.searchHashCode) {
                        // get the pointer to the pair
                        final long pointer =
                                this.bucket.getLong(
                                        this.bucketInSegmentOffset
                                                + BUCKET_POINTER_START_OFFSET
                                                + (this.numInSegment * POINTER_LEN));
                        this.numInSegment++;

                        // deserialize the key to check whether it is really equal, or whether we
                        // had only a hash collision
                        try {
                            this.partition.setReadPosition(pointer);
                            // 从分区对象中解析数据
                            reuse = this.accessor.deserialize(reuse, this.partition);
                            // 在检索前 会先给comparator设置待比较对象  此时表示hash和equal都匹配成功了
                            if (this.comparator.equalToReference(reuse)) {
                                if (isBuildOuterJoin) {
                                    // 给bucket/overflow的entry打上标记
                                    probedSet.set(numInSegment - 1);
                                }
                                this.lastPointer = pointer;
                                return reuse;
                            }
                        } catch (IOException ioex) {
                            throw new RuntimeException(
                                    "Error deserializing key or value from the hashtable: "
                                            + ioex.getMessage(),
                                    ioex);
                        }
                    } else {
                        this.numInSegment++;
                    }
                }

                // this segment is done. check if there is another chained bucket
                // 当bucket内的entry没有找到满足条件的数据  则从overflow中查找
                final long forwardPointer =
                        this.bucket.getLong(this.bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
                if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
                    return null;
                }

                final int overflowSegNum = (int) (forwardPointer >>> 32);
                this.bucket = this.overflowSegments[overflowSegNum];
                this.bucketInSegmentOffset = (int) forwardPointer;
                this.countInSegment =
                        this.bucket.getShort(this.bucketInSegmentOffset + HEADER_COUNT_OFFSET);
                this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
                this.numInSegment = 0;
            }
        }

        /**
         * 读取匹配set hash和equals的记录   与上面操作相同
         * @return
         */
        public BT next() {
            // loop over all segments that are involved in the bucket (original bucket plus overflow
            // buckets)
            while (true) {
                probedSet.setMemorySegment(
                        bucket, this.bucketInSegmentOffset + HEADER_PROBED_FLAGS_OFFSET);
                while (this.numInSegment < this.countInSegment) {

                    final int thisCode = this.bucket.getInt(this.posInSegment);
                    this.posInSegment += HASH_CODE_LEN;

                    // check if the hash code matches
                    if (thisCode == this.searchHashCode) {
                        // get the pointer to the pair
                        final long pointer =
                                this.bucket.getLong(
                                        this.bucketInSegmentOffset
                                                + BUCKET_POINTER_START_OFFSET
                                                + (this.numInSegment * POINTER_LEN));
                        this.numInSegment++;

                        // deserialize the key to check whether it is really equal, or whether we
                        // had only a hash collision
                        try {
                            this.partition.setReadPosition(pointer);
                            BT result = this.accessor.deserialize(this.partition);
                            if (this.comparator.equalToReference(result)) {
                                if (isBuildOuterJoin) {
                                    probedSet.set(numInSegment - 1);
                                }
                                this.lastPointer = pointer;
                                return result;
                            }
                        } catch (IOException ioex) {
                            throw new RuntimeException(
                                    "Error deserializing key or value from the hashtable: "
                                            + ioex.getMessage(),
                                    ioex);
                        }
                    } else {
                        this.numInSegment++;
                    }
                }

                // this segment is done. check if there is another chained bucket
                final long forwardPointer =
                        this.bucket.getLong(this.bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
                if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
                    return null;
                }

                final int overflowSegNum = (int) (forwardPointer >>> 32);
                this.bucket = this.overflowSegments[overflowSegNum];
                this.bucketInSegmentOffset = (int) forwardPointer;
                this.countInSegment =
                        this.bucket.getShort(this.bucketInSegmentOffset + HEADER_COUNT_OFFSET);
                this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
                this.numInSegment = 0;
            }
        }

        /**
         * 写入数据
         * @param value
         * @throws IOException
         */
        public void writeBack(BT value) throws IOException {
            // 该视图下是 存储分区数据的内存块
            final SeekableDataOutputView outView = this.partition.getWriteView();
            // 找到上次next匹配到的位置  并进行覆盖
            outView.setWritePosition(this.lastPointer);
            this.accessor.serialize(value, outView);
        }

        /**
         * 恢复到set()时的状态
         */
        public void reset() {
            this.bucket = this.originalBucket;
            this.bucketInSegmentOffset = this.originalBucketInSegmentOffset;

            this.posInSegment = this.bucketInSegmentOffset + BUCKET_HEADER_LENGTH;
            this.countInSegment = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
            this.numInSegment = 0;
        }
    } // end HashBucketIterator

    /** Iterate all the elements in memory which has not been probed during probe phase.
     * 生成一个迭代器  内部的数据还没有探测过
     * */
    public static class UnmatchedBuildIterator<BT, PT> implements MutableObjectIterator<BT> {

        private final TypeSerializer<BT> accessor;

        private final long totalBucketNumber;

        private final int bucketsPerSegmentBits;

        private final int bucketsPerSegmentMask;

        private final MemorySegment[] buckets;

        private final ArrayList<HashPartition<BT, PT>> partitionsBeingBuilt;

        private final BitSet probedSet;

        private MemorySegment bucketSegment;

        private MemorySegment[] overflowSegments;

        private HashPartition<BT, PT> partition;

        private int scanCount;

        private int bucketInSegmentOffset;

        private int countInSegment;

        private int numInSegment;

        UnmatchedBuildIterator(
                TypeSerializer<BT> accessor,
                long totalBucketNumber,
                int bucketsPerSegmentBits,
                int bucketsPerSegmentMask,
                MemorySegment[] buckets,
                ArrayList<HashPartition<BT, PT>> partitionsBeingBuilt,
                BitSet probedSet) {

            this.accessor = accessor;
            this.totalBucketNumber = totalBucketNumber;
            this.bucketsPerSegmentBits = bucketsPerSegmentBits;
            this.bucketsPerSegmentMask = bucketsPerSegmentMask;
            this.buckets = buckets;
            this.partitionsBeingBuilt = partitionsBeingBuilt;
            this.probedSet = probedSet;
            init();
        }

        private void init() {
            scanCount = -1;
            // 找到首个可用的bucket后 既返回  (如果分区已经使用了channel 就会忽略  也就是说进了磁盘的分区数据无法直接通过bucket来检索了)
            while (!moveToNextBucket()) {
                if (scanCount >= totalBucketNumber) {
                    break;
                }
            }
        }

        /**
         * 返回数据
         * @param reuse The target object into which to place next element if E is mutable.
         * @return
         */
        public BT next(BT reuse) {
            // search unprobed record in bucket, while none found move to next bucket and search.
            while (true) {
                BT result = nextInBucket(reuse);
                if (result == null) {
                    // return null while there are no more buckets.
                    if (!moveToNextOnHeapBucket()) {
                        return null;
                    }
                } else {
                    return result;
                }
            }
        }

        public BT next() {
            // search unprobed record in bucket, while none found move to next bucket and search.
            while (true) {
                BT result = nextInBucket();
                // 表示该bucket中已经没有数据了
                if (result == null) {
                    // return null while there are no more buckets.
                    // 切换到下个bucket
                    if (!moveToNextOnHeapBucket()) {
                        return null;
                    }
                } else {
                    return result;
                }
            }
        }

        /**
         * Loop to make sure that it would move to next on heap bucket, return true while move to a
         * on heap bucket, return false if there is no more bucket.
         */
        private boolean moveToNextOnHeapBucket() {
            while (!moveToNextBucket()) {
                if (scanCount >= totalBucketNumber) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Move to next bucket, return true while move to a on heap bucket, return false while move
         * to a spilled bucket or there is no more bucket.
         * 挨个检索bucket
         */
        private boolean moveToNextBucket() {
            scanCount++;
            if (scanCount > totalBucketNumber - 1) {
                return false;
            }
            // move to next bucket, update all the current bucket status with new bucket
            // information.
            final int bucketArrayPos = scanCount >> this.bucketsPerSegmentBits;
            final int currentBucketInSegmentOffset =
                    (scanCount & this.bucketsPerSegmentMask) << NUM_INTRA_BUCKET_BITS;
            MemorySegment currentBucket = this.buckets[bucketArrayPos];
            final int partitionNumber =
                    currentBucket.get(currentBucketInSegmentOffset + HEADER_PARTITION_OFFSET);
            final HashPartition<BT, PT> p = this.partitionsBeingBuilt.get(partitionNumber);

            // 以上是常规操作  要求分区数据还在内存中
            if (p.isInMemory()) {
                setBucket(currentBucket, p.overflowSegments, p, currentBucketInSegmentOffset);
                return true;
            } else {
                return false;
            }
        }

        // update current bucket status.
        // 设置当前在使用的bucket
        private void setBucket(
                MemorySegment bucket,
                MemorySegment[] overflowSegments,
                HashPartition<BT, PT> partition,
                int bucketInSegmentOffset) {
            this.bucketSegment = bucket;
            this.overflowSegments = overflowSegments;
            this.partition = partition;
            this.bucketInSegmentOffset = bucketInSegmentOffset;
            this.countInSegment = bucket.getShort(bucketInSegmentOffset + HEADER_COUNT_OFFSET);
            this.numInSegment = 0;
            // reset probedSet with probedFlags offset in this bucket.
            this.probedSet.setMemorySegment(
                    bucketSegment, this.bucketInSegmentOffset + HEADER_PROBED_FLAGS_OFFSET);
        }

        /**
         * 返回当前bucket的下个数据
         * @param reuse
         * @return
         */
        private BT nextInBucket(BT reuse) {
            // loop over all segments that are involved in the bucket (original bucket plus overflow
            // buckets)
            while (true) {
                while (this.numInSegment < this.countInSegment) {
                    // 如果布隆过滤器没值 表示直接读bucket即可
                    boolean probed = probedSet.get(numInSegment);
                    if (!probed) {
                        final long pointer =
                                this.bucketSegment.getLong(
                                        this.bucketInSegmentOffset
                                                + BUCKET_POINTER_START_OFFSET
                                                + (this.numInSegment * POINTER_LEN));
                        try {
                            this.partition.setReadPosition(pointer);
                            reuse = this.accessor.deserialize(reuse, this.partition);
                            this.numInSegment++;
                            return reuse;
                        } catch (IOException ioex) {
                            throw new RuntimeException(
                                    "Error deserializing key or value from the hashtable: "
                                            + ioex.getMessage(),
                                    ioex);
                        }
                    } else {
                        this.numInSegment++;
                    }
                }

                // 表示要借助 overflow

                // if all buckets were spilled out of memory
                if (this.bucketSegment == null) {
                    return null;
                }

                // this segment is done. check if there is another chained bucket
                final long forwardPointer =
                        this.bucketSegment.getLong(
                                this.bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
                if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
                    return null;
                }

                final int overflowSegNum = (int) (forwardPointer >>> 32);
                this.bucketSegment = this.overflowSegments[overflowSegNum];
                this.bucketInSegmentOffset = (int) forwardPointer;
                this.countInSegment =
                        this.bucketSegment.getShort(
                                this.bucketInSegmentOffset + HEADER_COUNT_OFFSET);
                this.numInSegment = 0;
                // reset probedSet with probedFlags offset in this bucket.
                this.probedSet.setMemorySegment(
                        bucketSegment, this.bucketInSegmentOffset + HEADER_PROBED_FLAGS_OFFSET);
            }
        }

        /**
         * 同上
         * @return
         */
        private BT nextInBucket() {
            // loop over all segments that are involved in the bucket (original bucket plus overflow
            // buckets)
            while (true) {
                while (this.numInSegment < this.countInSegment) {
                    boolean probed = probedSet.get(numInSegment);
                    if (!probed) {
                        final long pointer =
                                this.bucketSegment.getLong(
                                        this.bucketInSegmentOffset
                                                + BUCKET_POINTER_START_OFFSET
                                                + (this.numInSegment * POINTER_LEN));
                        try {
                            this.partition.setReadPosition(pointer);
                            BT result = this.accessor.deserialize(this.partition);
                            this.numInSegment++;
                            return result;
                        } catch (IOException ioex) {
                            throw new RuntimeException(
                                    "Error deserializing key or value from the hashtable: "
                                            + ioex.getMessage(),
                                    ioex);
                        }
                    } else {
                        this.numInSegment++;
                    }
                }

                // if all buckets were spilled out of memory
                if (this.bucketSegment == null) {
                    return null;
                }

                // this segment is done. check if there is another chained bucket
                final long forwardPointer =
                        this.bucketSegment.getLong(
                                this.bucketInSegmentOffset + HEADER_FORWARD_OFFSET);
                if (forwardPointer == BUCKET_FORWARD_POINTER_NOT_SET) {
                    return null;
                }

                final int overflowSegNum = (int) (forwardPointer >>> 32);
                this.bucketSegment = this.overflowSegments[overflowSegNum];
                this.bucketInSegmentOffset = (int) forwardPointer;
                this.countInSegment =
                        this.bucketSegment.getShort(
                                this.bucketInSegmentOffset + HEADER_COUNT_OFFSET);
                this.numInSegment = 0;
                // reset probedSet with probedFlags offset in this bucket.
                this.probedSet.setMemorySegment(
                        bucketSegment, this.bucketInSegmentOffset + HEADER_PROBED_FLAGS_OFFSET);
            }
        }

        public void back() {
            this.numInSegment--;
        }
    }

    // ======================================================================================================

    /**
     * 探测迭代器  内部包含一个 MutableObjectIterator
     * @param <PT>
     */
    public static final class ProbeIterator<PT> {

        private MutableObjectIterator<PT> source;

        /**
         * 该实例可以重复使用
         */
        private PT instance;

        ProbeIterator(MutableObjectIterator<PT> source, PT instance) {
            this.instance = instance;
            set(source);
        }

        void set(MutableObjectIterator<PT> source) {
            this.source = source;
        }

        public PT next() throws IOException {
            PT retVal = this.source.next(this.instance);
            if (retVal != null) {
                this.instance = retVal;
                return retVal;
            } else {
                return null;
            }
        }

        public PT getCurrent() {
            return this.instance;
        }
    }
}
