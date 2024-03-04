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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.FileChannelInputView;
import org.apache.flink.runtime.io.disk.FileChannelOutputView;
import org.apache.flink.runtime.io.disk.InputViewIterator;
import org.apache.flink.runtime.io.disk.SeekableFileChannelInputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.types.NullKeyFieldException;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 大记录处理器
 * record 会专门抽取出key 额外存储一份keys数据
 * @param <T>
 */
public class LargeRecordHandler<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LargeRecordHandler.class);

    private static final int MIN_SEGMENTS_FOR_KEY_SPILLING = 1;

    private static final int MAX_SEGMENTS_FOR_KEY_SPILLING = 4;

    // --------------------------------------------------------------------------------------------

    private final TypeSerializer<T> serializer;

    private final TypeComparator<T> comparator;

    /**
     * 针对元祖类型的序列化对象
     */
    private TupleSerializer<Tuple> keySerializer;

    /**
     * 针对元祖元素类型的比较对象
     */
    private TupleComparator<Tuple> keyComparator;

    private FileChannelOutputView recordsOutFile;

    private FileChannelOutputView keysOutFile;

    private Tuple keyTuple;

    private FileChannelInputView keysReader;

    private SeekableFileChannelInputView recordsReader;

    private FileIOChannel.ID recordsChannel;

    private FileIOChannel.ID keysChannel;

    private final IOManager ioManager;

    private final MemoryManager memManager;

    private final List<MemorySegment> memory;

    /**
     * 该对象可以进行排序
     */
    private Sorter<Tuple> keySorter;

    private final TaskInvokable memoryOwner;

    private long recordCounter;

    private int numKeyFields;

    private final int maxFilehandles;

    private volatile boolean closed;

    private final ExecutionConfig executionConfig;

    // --------------------------------------------------------------------------------------------

    public LargeRecordHandler(
            TypeSerializer<T> serializer,
            TypeComparator<T> comparator,
            IOManager ioManager,
            MemoryManager memManager,
            List<MemorySegment> memory,
            TaskInvokable memoryOwner,
            int maxFilehandles,
            ExecutionConfig executionConfig) {
        this.serializer = checkNotNull(serializer);
        this.comparator = checkNotNull(comparator);
        this.ioManager = checkNotNull(ioManager);
        this.memManager = checkNotNull(memManager);
        this.memory = checkNotNull(memory);
        this.memoryOwner = checkNotNull(memoryOwner);
        this.maxFilehandles = maxFilehandles;
        this.executionConfig = checkNotNull(executionConfig);

        checkArgument(maxFilehandles >= 2);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 添加一条记录
     * @param record
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public long addRecord(T record) throws IOException {

        // 一开始文件还未初始化
        if (recordsOutFile == null) {

            if (closed) {
                throw new IllegalStateException("The large record handler has been closed.");
            }
            if (recordsReader != null) {
                throw new IllegalStateException("The handler has already switched to sorting.");
            }

            LOG.debug("Initializing the large record spilling...");

            // initialize the utilities
            {
                // 获取该类型的key字段
                final TypeComparator<?>[] keyComps = comparator.getFlatComparators();
                numKeyFields = keyComps.length;
                Object[] keyHolder = new Object[numKeyFields];

                // 从记录中抽取出关键字段
                comparator.extractKeys(record, keyHolder, 0);

                TypeSerializer<?>[] keySers = new TypeSerializer<?>[numKeyFields];
                TypeSerializer<?>[] tupleSers = new TypeSerializer<?>[numKeyFields + 1];

                int[] keyPos = new int[numKeyFields];

                for (int i = 0; i < numKeyFields; i++) {
                    keyPos[i] = i;
                    keySers[i] = createSerializer(keyHolder[i], i);
                    tupleSers[i] = keySers[i];
                }
                // add the long serializer for the offset
                // tupleSers 多维护一个存储offset的字段
                tupleSers[numKeyFields] = LongSerializer.INSTANCE;

                keySerializer =
                        new TupleSerializer<>(
                                (Class<Tuple>) Tuple.getTupleClass(numKeyFields + 1), tupleSers);
                keyComparator = new TupleComparator<>(keyPos, keyComps, keySers);

                // 只由关键字段组成的tuple
                keyTuple = keySerializer.createInstance();
            }

            // initialize the spilling
            final int totalNumSegments = memory.size();
            final int segmentsForKeys =
                    (totalNumSegments >= 2 * MAX_SEGMENTS_FOR_KEY_SPILLING)
                            ? MAX_SEGMENTS_FOR_KEY_SPILLING
                            : Math.max(
                                    MIN_SEGMENTS_FOR_KEY_SPILLING,
                                    totalNumSegments - MAX_SEGMENTS_FOR_KEY_SPILLING);

            List<MemorySegment> recordsMemory = new ArrayList<MemorySegment>();
            List<MemorySegment> keysMemory = new ArrayList<MemorySegment>();

            // 2个内存块列表分别存储 keys/records
            for (int i = 0; i < segmentsForKeys; i++) {
                keysMemory.add(memory.get(i));
            }
            for (int i = segmentsForKeys; i < totalNumSegments; i++) {
                recordsMemory.add(memory.get(i));
            }

            recordsChannel = ioManager.createChannel();
            keysChannel = ioManager.createChannel();

            // 创建文件存储数据

            recordsOutFile =
                    new FileChannelOutputView(
                            ioManager.createBlockChannelWriter(recordsChannel),
                            memManager,
                            recordsMemory,
                            memManager.getPageSize());

            keysOutFile =
                    new FileChannelOutputView(
                            ioManager.createBlockChannelWriter(keysChannel),
                            memManager,
                            keysMemory,
                            memManager.getPageSize());
        }

        final long offset = recordsOutFile.getWriteOffset();
        if (offset < 0) {
            throw new RuntimeException("wrong offset");
        }

        Object[] keyHolder = new Object[numKeyFields];

        // 每次用新record的字段来填充 keyTuple
        comparator.extractKeys(record, keyHolder, 0);
        for (int i = 0; i < numKeyFields; i++) {
            keyTuple.setField(keyHolder[i], i);
        }
        // 额外追加一个偏移量
        keyTuple.setField(offset, numKeyFields);


        // 多写入一条记录
        keySerializer.serialize(keyTuple, keysOutFile);
        serializer.serialize(record, recordsOutFile);

        recordCounter++;

        return offset;
    }

    /**
     * 表示写入完成了  准备对keys进行排序  因为之前写入时 数据是无序的
     * @param memory
     * @return
     * @throws IOException
     */
    public MutableObjectIterator<T> finishWriteAndSortKeys(List<MemorySegment> memory)
            throws IOException {
        if (recordsOutFile == null || keysOutFile == null) {
            throw new IllegalStateException("The LargeRecordHandler has not spilled any records");
        }

        // close the writers and
        final int lastBlockBytesKeys;
        final int lastBlockBytesRecords;

        recordsOutFile.close();
        keysOutFile.close();
        lastBlockBytesKeys = keysOutFile.getBytesInLatestSegment();
        lastBlockBytesRecords = recordsOutFile.getBytesInLatestSegment();
        recordsOutFile = null;
        keysOutFile = null;

        final int pagesForReaders =
                Math.max(
                        3 * MIN_SEGMENTS_FOR_KEY_SPILLING,
                        Math.min(2 * MAX_SEGMENTS_FOR_KEY_SPILLING, memory.size() / 50));
        final int pagesForKeyReader =
                Math.min(
                        pagesForReaders - MIN_SEGMENTS_FOR_KEY_SPILLING,
                        MAX_SEGMENTS_FOR_KEY_SPILLING);
        final int pagesForRecordReader = pagesForReaders - pagesForKeyReader;

        // grab memory for the record reader
        ArrayList<MemorySegment> memForRecordReader = new ArrayList<MemorySegment>();
        ArrayList<MemorySegment> memForKeysReader = new ArrayList<MemorySegment>();

        // 为读取数据分配内存块
        for (int i = 0; i < pagesForRecordReader; i++) {
            memForRecordReader.add(memory.remove(memory.size() - 1));
        }
        for (int i = 0; i < pagesForKeyReader; i++) {
            memForKeysReader.add(memory.remove(memory.size() - 1));
        }

        keysReader =
                new FileChannelInputView(
                        ioManager.createBlockChannelReader(keysChannel),
                        memManager,
                        memForKeysReader,
                        lastBlockBytesKeys);
        // 该对象可以将字节流反序列化  得到tuple对象
        InputViewIterator<Tuple> keyIterator =
                new InputViewIterator<Tuple>(keysReader, keySerializer);

        try {
            // 生成排序对象
            keySorter =
                    ExternalSorter.newBuilder(
                                    memManager,
                                    memoryOwner,
                                    keySerializer,
                                    keyComparator,
                                    executionConfig)
                            .maxNumFileHandles(maxFilehandles)
                            .sortBuffers(1)
                            .enableSpilling(ioManager, 1.0f)
                            .memory(memory)
                            .objectReuse(this.executionConfig.isObjectReuseEnabled())
                            .largeRecords(false)
                            .build(keyIterator);
        } catch (MemoryAllocationException e) {
            throw new IllegalStateException(
                    "We should not try allocating memory. Instead the sorter should use the provided memory.",
                    e);
        }

        // wait for the sorter to sort the keys
        MutableObjectIterator<Tuple> result;
        try {
            result = keySorter.getIterator();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        // keys被排序了 并且在写入时携带了offset 所以可以利用它来读取records
        recordsReader =
                new SeekableFileChannelInputView(
                        ioManager,
                        recordsChannel,
                        memManager,
                        memForRecordReader,
                        lastBlockBytesRecords);

        return new FetchingIterator<T>(
                serializer, result, recordsReader, keySerializer, numKeyFields);
    }

    /**
     * Closes all structures and deletes all temporary files. Even in the presence of failures, this
     * method will try and continue closing files and deleting temporary files.
     *
     * @throws IOException Thrown if an error occurred while closing/deleting the files.
     * 关闭本对象
     */
    public void close() throws IOException {

        // we go on closing and deleting files in the presence of failures.
        // we remember the first exception to occur and re-throw it later
        Throwable ex = null;

        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;

            // close the writers
            if (recordsOutFile != null) {
                try {
                    recordsOutFile.close();
                    recordsOutFile = null;
                } catch (Throwable t) {
                    LOG.error("Cannot close the large records spill file.", t);
                    ex = t;
                }
            }
            if (keysOutFile != null) {
                try {
                    keysOutFile.close();
                    keysOutFile = null;
                } catch (Throwable t) {
                    LOG.error("Cannot close the large records key spill file.", t);
                    ex = ex == null ? t : ex;
                }
            }

            // close the readers
            if (recordsReader != null) {
                try {
                    recordsReader.close();
                    recordsReader = null;
                } catch (Throwable t) {
                    LOG.error("Cannot close the large records reader.", t);
                    ex = ex == null ? t : ex;
                }
            }
            if (keysReader != null) {
                try {
                    keysReader.close();
                    keysReader = null;
                } catch (Throwable t) {
                    LOG.error("Cannot close the large records key reader.", t);
                    ex = ex == null ? t : ex;
                }
            }

            // delete the spill files
            if (recordsChannel != null) {
                try {
                    ioManager.deleteChannel(recordsChannel);
                    recordsChannel = null;
                } catch (Throwable t) {
                    LOG.error("Cannot delete the large records spill file.", t);
                    ex = ex == null ? t : ex;
                }
            }
            if (keysChannel != null) {
                try {
                    ioManager.deleteChannel(keysChannel);
                    keysChannel = null;
                } catch (Throwable t) {
                    LOG.error("Cannot delete the large records key spill file.", t);
                    ex = ex == null ? t : ex;
                }
            }

            // close the key sorter
            if (keySorter != null) {
                try {
                    keySorter.close();
                    keySorter = null;
                } catch (Throwable t) {
                    LOG.error(
                            "Cannot properly dispose the key sorter and clean up its temporary files.",
                            t);
                    ex = ex == null ? t : ex;
                }
            }

            memManager.release(memory);

            recordCounter = 0;
        }

        // re-throw the exception, if necessary
        if (ex != null) {
            throw new IOException(
                    "An error occurred cleaning up spill files in the large record handler.", ex);
        }
    }

    // --------------------------------------------------------------------------------------------

    public boolean hasData() {
        return recordCounter > 0;
    }

    // --------------------------------------------------------------------------------------------

    private TypeSerializer<Object> createSerializer(Object key, int pos) {
        if (key == null) {
            throw new NullKeyFieldException(pos);
        }
        try {
            TypeInformation<Object> info = TypeExtractor.getForObject(key);
            return info.createSerializer(executionConfig);
        } catch (Throwable t) {
            throw new RuntimeException("Could not create key serializer for type " + key);
        }
    }

    /**
     * 使用该对象来读取数据
     * @param <T>
     */
    private static final class FetchingIterator<T> implements MutableObjectIterator<T> {

        private final TypeSerializer<T> serializer;

        private final MutableObjectIterator<Tuple> tupleInput;

        private final SeekableFileChannelInputView recordsInputs;

        /**
         * 暂存当前使用的keys
         */
        private Tuple value;

        private final int pointerPos;

        /**
         *
         * @param serializer
         * @param tupleInput  排序后的 keys
         * @param recordsInputs   对应写入的记录
         * @param tupleSerializer
         * @param pointerPos  指针字段的下标
         */
        public FetchingIterator(
                TypeSerializer<T> serializer,
                MutableObjectIterator<Tuple> tupleInput,
                SeekableFileChannelInputView recordsInputs,
                TypeSerializer<Tuple> tupleSerializer,
                int pointerPos) {
            this.serializer = serializer;
            this.tupleInput = tupleInput;
            this.recordsInputs = recordsInputs;
            this.pointerPos = pointerPos;

            this.value = tupleSerializer.createInstance();
        }

        @Override
        public T next(T reuse) throws IOException {
            return next();
        }

        @Override
        public T next() throws IOException {
            // 读取keys
            Tuple value = tupleInput.next(this.value);
            if (value != null) {
                this.value = value;
                // 获取指针字段
                long pointer = value.<Long>getField(pointerPos);

                // 使用指针寻址
                recordsInputs.seek(pointer);
                return serializer.deserialize(recordsInputs);
            } else {
                return null;
            }
        }
    }
}
