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
import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.runtime.operators.sort.CircularElement.EOF_MARKER;
import static org.apache.flink.runtime.operators.sort.CircularElement.SPILLING_MARKER;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The thread that handles the spilling of intermediate results and sets up the merging. It also
 * merges the channels until sufficiently few channels remain to perform the final streamed merge.
 * 表示在后台触发数据倾泻的线程
 */
final class SpillingThread<E> extends ThreadBase<E> {
    /** An interface for injecting custom behaviour for spilling and merging phases.
     *
     * */
    interface SpillingBehaviour<E> {
        default void open() {}

        default void close() {}

        /**
         * A method that allows adjusting the spilling phase. We can inject e.g. combining the
         * elements while spilling.
         * @param largeRecordHandler 在数据倾泻时调用
         */
        void spillBuffer(
                CircularElement<E> element,
                ChannelWriterOutputView output,
                LargeRecordHandler<E> largeRecordHandler)
                throws IOException;

        /**
         * A method that allows adjusting the merging phase. We can inject e.g. combining the
         * spilled elements.
         *
         * @param mergeIterator  在数据合并时调用
         * @param output
         */
        void mergeRecords(MergeIterator<E> mergeIterator, ChannelWriterOutputView output)
                throws IOException;
    }

    /** Logging. */
    private static final Logger LOG = LoggerFactory.getLogger(SpillingThread.class);

    private final MemoryManager memManager; // memory manager to release memory

    private final IOManager ioManager; // I/O manager to create channels

    private final TypeSerializer<E> serializer; // The serializer for the data type

    private final TypeComparator<E>
            comparator; // The comparator that establishes the order relation.

    private final List<MemorySegment> writeMemory; // memory segments for writing

    private final List<MemorySegment> mergeReadMemory; // memory segments for sorting/reading

    private final int maxFanIn;

    /**
     * 该对象就是用来管理文件的
     */
    private final SpillChannelManager spillChannelManager;

    /**
     * 使用该对象写入数据 会自动存一份keys 并可以通过keys检索record
     */
    private final LargeRecordHandler<E> largeRecordHandler;

    /**
     * 可以进行数据倾泻
     * 也可以进行数据合并
     */
    private final SpillingBehaviour<E> spillingBehaviour;

    private volatile boolean spillingBehaviourOpened = false;

    private final int minNumWriteBuffers;

    private final int maxNumWriteBuffers;

    /**
     *
     * @param exceptionHandler
     * @param dispatcher  存储数据用于分发
     * @param memManager  内存池
     * @param ioManager
     * @param serializer
     * @param comparator
     * @param sortReadMemory
     * @param writeMemory
     * @param maxNumFileHandles
     * @param spillingChannelManager
     * @param largeRecordHandler
     * @param spillingBehaviour
     * @param minNumWriteBuffers
     * @param maxNumWriteBuffers
     */
    SpillingThread(
            @Nullable ExceptionHandler<IOException> exceptionHandler,
            StageMessageDispatcher<E> dispatcher,
            MemoryManager memManager,
            IOManager ioManager,
            TypeSerializer<E> serializer,
            TypeComparator<E> comparator,
            List<MemorySegment> sortReadMemory,
            List<MemorySegment> writeMemory,
            int maxNumFileHandles,
            SpillChannelManager spillingChannelManager,
            @Nullable LargeRecordHandler<E> largeRecordHandler,
            SpillingBehaviour<E> spillingBehaviour,
            int minNumWriteBuffers,
            int maxNumWriteBuffers) {
        super(exceptionHandler, "SortMerger spilling thread", dispatcher);
        this.memManager = checkNotNull(memManager);
        this.ioManager = checkNotNull(ioManager);
        this.serializer = checkNotNull(serializer);
        this.comparator = checkNotNull(comparator);
        this.mergeReadMemory = checkNotNull(sortReadMemory);
        this.writeMemory = checkNotNull(writeMemory);
        this.maxFanIn = maxNumFileHandles;
        this.spillChannelManager = checkNotNull(spillingChannelManager);
        this.largeRecordHandler = largeRecordHandler;
        this.spillingBehaviour = checkNotNull(spillingBehaviour);
        this.minNumWriteBuffers = minNumWriteBuffers;
        this.maxNumWriteBuffers = maxNumWriteBuffers;
    }

    /** Entry point of the thread.
     * 运行逻辑
     * */
    @Override
    public void go() throws IOException, InterruptedException {

        // ------------------- In-Memory Cache ------------------------
        final Queue<CircularElement<E>> cache = new ArrayDeque<>();

        // 首先从dispatcher读取数据
        // 正常情况数据是直接缓存在内存中即可的   如果以false的方式返回了 一般就是内存不足  需要写入磁盘
        boolean cacheOnly = readCache(cache);

        // check whether the thread was canceled
        // 当线程被暂停时  退出
        if (!isRunning()) {
            return;
        }

        MutableObjectIterator<E> largeRecords = null;

        // check if we can stay in memory with the large record handler
        // 表示largeRecordHandler中有数据  在之前的流程中如果遇到大记录 就会利用该对象存储
        if (cacheOnly && largeRecordHandler != null && largeRecordHandler.hasData()) {
            List<MemorySegment> memoryForLargeRecordSorting = new ArrayList<>();

            CircularElement<E> circElement;

            // 因为数据处理完了 就可以释放内存块了
            while ((circElement = this.dispatcher.poll(SortStage.READ)) != null) {
                circElement.getBuffer().dispose();
                memoryForLargeRecordSorting.addAll(circElement.getMemory());
            }

            if (memoryForLargeRecordSorting.isEmpty()) {
                cacheOnly = false;
                LOG.debug("Going to disk-based merge because of large records.");
            } else {
                LOG.debug("Sorting large records, to add them to in-memory merge.");
                // 将写入的数据以keys/records的形式读取出来
                largeRecords =
                        largeRecordHandler.finishWriteAndSortKeys(memoryForLargeRecordSorting);
            }
        }

        // ------------------- In-Memory Merge ------------------------
        // 将数据加载到内存即可
        if (cacheOnly) {
            mergeInMemory(cache, largeRecords);
            return;
        }

        // 表示数据无法缓存 一般就是之前的流程发现内存不够了

        // ------------------- Spilling Phase ------------------------
        // 进行数据倾泻
        List<ChannelWithBlockCount> channelIDs = startSpilling(cache);

        // ------------------- Merging Phase ------------------------

        // 倾泻完成后 对磁盘数据进行合并
        mergeOnDisk(channelIDs);
    }

    @Override
    public void close() throws InterruptedException {
        super.close();
        if (spillingBehaviourOpened) {
            this.spillingBehaviour.close();
            this.spillingBehaviourOpened = false;
        }
    }

    /**
     *
     * @param cache
     * @return
     * @throws InterruptedException
     */
    private boolean readCache(Queue<CircularElement<E>> cache) throws InterruptedException {
        // fill cache
        while (isRunning()) {
            // take next element from queue
            // 只读取SPILL的数据
            final CircularElement<E> element = this.dispatcher.take(SortStage.SPILL);

            // 遇到标记数据 返回
            if (element == SPILLING_MARKER) {
                // 表示需要倾泻  一般都是内存不足 需要尽快释放内存
                return false;
            } else if (element == EOF_MARKER) {
                // 表示数据已经处理完了
                return true;
            }
            cache.add(element);
        }
        return false;
    }

    /**
     *
     * @param channelIDs  spill阶段写入的文件
     * @throws IOException
     */
    private void mergeOnDisk(List<ChannelWithBlockCount> channelIDs) throws IOException {
        // make sure we have enough memory to merge and for large record handling
        List<MemorySegment> mergeReadMemory;
        MutableObjectIterator<E> largeRecords = null;

        // 顺便处理 largeRecordHandler
        if (largeRecordHandler != null && largeRecordHandler.hasData()) {

            List<MemorySegment> longRecMem;
            if (channelIDs.isEmpty()) {
                // only long records
                longRecMem = this.mergeReadMemory;
                mergeReadMemory = Collections.emptyList();
            } else {
                int maxMergedStreams = Math.min(this.maxFanIn, channelIDs.size());

                int pagesPerStream =
                        Math.max(
                                minNumWriteBuffers,
                                Math.min(
                                        maxNumWriteBuffers,
                                        this.mergeReadMemory.size() / 2 / maxMergedStreams));

                int totalMergeReadMemory = maxMergedStreams * pagesPerStream;

                // grab the merge memory   回收内存
                mergeReadMemory = new ArrayList<>(totalMergeReadMemory);
                for (int i = 0; i < totalMergeReadMemory; i++) {
                    mergeReadMemory.add(this.mergeReadMemory.get(i));
                }

                // the remainder of the memory goes to the long record sorter
                longRecMem = new ArrayList<>();
                for (int i = totalMergeReadMemory; i < this.mergeReadMemory.size(); i++) {
                    longRecMem.add(this.mergeReadMemory.get(i));
                }
            }

            LOG.debug("Sorting keys for large records.");
            // 将数据排序后存入longRecMem 参与之后的merge
            largeRecords = largeRecordHandler.finishWriteAndSortKeys(longRecMem);
        } else {
            mergeReadMemory = this.mergeReadMemory;
        }

        // merge channels until sufficient file handles are available
        // merge前 肯定要把数据先加载到内存 mergeReadMemory 就是为merge存在的临时容器
        while (isRunning() && channelIDs.size() > this.maxFanIn) {
            channelIDs = mergeChannelList(channelIDs, mergeReadMemory, this.writeMemory);
        }

        // from here on, we won't write again
        this.memManager.release(this.writeMemory);
        this.writeMemory.clear();

        // check if we have spilled some data at all  本次没有处理文件数据  且有largeRecords 那么将其作为结果
        if (channelIDs.isEmpty()) {
            if (largeRecords == null) {
                this.dispatcher.sendResult(EmptyMutableObjectIterator.get());
            } else {
                this.dispatcher.sendResult(largeRecords);
            }
        } else {
            LOG.debug("Beginning final merge.");

            // allocate the memory for the final merging step
            List<List<MemorySegment>> readBuffers = new ArrayList<>(channelIDs.size());

            // allocate the read memory and register it to be released
            getSegmentsForReaders(readBuffers, mergeReadMemory, channelIDs.size());

            // get the readers and register them to be released
            this.dispatcher.sendResult(
                    // 虽然数据做了合并  但是一旦小于maxFanIn后 就会停止合并 这里生成迭代器返回
                    getMergingIterator(
                            channelIDs,
                            readBuffers,
                            new ArrayList<>(channelIDs.size()),
                            largeRecords));
        }

        // done
        LOG.debug("Spilling and merging thread done.");
    }

    /**
     * 在内存中做聚合
     * @param cache
     * @param largeRecords  可迭代之前写入的大记录
     * @throws IOException
     */
    private void mergeInMemory(
            Queue<CircularElement<E>> cache, MutableObjectIterator<E> largeRecords)
            throws IOException {
        // operates on in-memory buffers only
        LOG.debug("Initiating in memory merge.");

        List<MutableObjectIterator<E>> iterators = new ArrayList<>(cache.size() + 1);

        // iterate buffers and collect a set of iterators
        for (CircularElement<E> cached : cache) {
            // note: the yielded iterator only operates on the buffer heap (and disregards the
            // stack)
            iterators.add(cached.getBuffer().getIterator());
        }

        if (largeRecords != null) {
            iterators.add(largeRecords);
        }

        // release the remaining sort-buffers
        LOG.debug("Releasing unused sort-buffer memory.");
        // 回收内存
        disposeSortBuffers(true);

        // set lazy iterator
        if (iterators.isEmpty()) {
            this.dispatcher.sendResult(EmptyMutableObjectIterator.get());
        } else if (iterators.size() == 1) {
            this.dispatcher.sendResult(iterators.get(0));
        } else {
            // 合并数据并作为result发送给dispatcher
            this.dispatcher.sendResult(new MergeIterator<>(iterators, this.comparator));
        }
    }

    /**
     * 开始倾泻数据  目前看来就是将element作为参数 触发spill相关的方法 完成spill阶段
     * @param cache  这些数据就是通过 dispatcher.take(SortStage.SPILL) 得到的
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private List<ChannelWithBlockCount> startSpilling(Queue<CircularElement<E>> cache)
            throws IOException, InterruptedException {
        final FileIOChannel.Enumerator enumerator = this.ioManager.createChannelEnumerator();
        List<ChannelWithBlockCount> channelIDs = new ArrayList<>();

        // loop as long as the thread is marked alive and we do not see the final element
        // 先开启倾泻对象
        openSpillingBehaviour();
        while (isRunning()) {
            final CircularElement<E> element =
                    cache.isEmpty() ? this.dispatcher.take(SortStage.SPILL) : cache.poll();

            // check if we are still running
            if (!isRunning()) {
                return Collections.emptyList();
            }
            // check if this is the end-of-work buffer
            // 表示所有数据处理完了
            if (element == EOF_MARKER) {
                break;
            }

            // open next channel
            FileIOChannel.ID channel = enumerator.next();
            // 产生一个新文件
            spillChannelManager.registerChannelToBeRemovedAtShutdown(channel);

            // create writer
            final BlockChannelWriter<MemorySegment> writer =
                    this.ioManager.createBlockChannelWriter(channel);
            spillChannelManager.registerOpenChannelToBeRemovedAtShutdown(writer);

            // 这是一个按page写入数据的对象
            final ChannelWriterOutputView output =
                    new ChannelWriterOutputView(
                            writer, this.writeMemory, this.memManager.getPageSize());

            // write sort-buffer to channel
            LOG.debug("Spilling buffer " + element.getId() + ".");
            // 触发钩子    这个时候认为已经写好了
            spillingBehaviour.spillBuffer(element, output, largeRecordHandler);
            LOG.debug("Spilled buffer " + element.getId() + ".");

            output.close();
            spillChannelManager.unregisterOpenChannelToBeRemovedAtShutdown(writer);

            if (output.getBytesWritten() > 0) {
                // 记录每个channel使用的block数量
                channelIDs.add(new ChannelWithBlockCount(channel, output.getBlockCount()));
            }

            // pass empty sort-buffer to reading thread
            // 已经写好了 就可以释放了
            element.getBuffer().reset();
            // READ目前看来就是用于回收的
            this.dispatcher.send(SortStage.READ, element);
        }

        // done with the spilling
        LOG.debug("Spilling done.");
        LOG.debug("Releasing sort-buffer memory.");

        // clear the sort buffers, but do not return the memory to the manager, as we use it for
        // merging
        disposeSortBuffers(false);
        return channelIDs;
    }

    private void openSpillingBehaviour() {
        if (!spillingBehaviourOpened) {
            this.spillingBehaviour.open();
            this.spillingBehaviourOpened = true;
        }
    }

    /** Releases the memory that is registered for in-memory sorted run generation.
     * 回收内存
     * */
    private void disposeSortBuffers(boolean releaseMemory) {
        CircularElement<E> element;
        while ((element = this.dispatcher.poll(SortStage.READ)) != null) {
            element.getBuffer().dispose();
            if (releaseMemory) {
                this.memManager.release(element.getMemory());
            }
        }
    }

    // ------------------------------------------------------------------------
    //                             Result Merging
    // ------------------------------------------------------------------------

    /**
     * Returns an iterator that iterates over the merged result from all given channels.
     *
     * @param channelIDs The channels that are to be merged and returned.
     * @param inputSegments The buffers to be used for reading. The list contains for each channel
     *     one list of input segments. The size of the <code>inputSegments</code> list must be equal
     *     to that of the <code>channelIDs</code> list.
     * @return An iterator over the merged records of the input channels.
     * @throws IOException Thrown, if the readers encounter an I/O problem.
     * 将文件数据读取出来 变成MergeIterator
     */
    private MergeIterator<E> getMergingIterator(
            final List<ChannelWithBlockCount> channelIDs,
            final List<List<MemorySegment>> inputSegments,
            List<FileIOChannel> readerList,
            MutableObjectIterator<E> largeRecords)
            throws IOException {
        // create one iterator per channel id
        LOG.debug("Performing merge of {} sorted streams.", channelIDs.size());

        final List<MutableObjectIterator<E>> iterators = new ArrayList<>(channelIDs.size() + 1);

        for (int i = 0; i < channelIDs.size(); i++) {
            final ChannelWithBlockCount channel = channelIDs.get(i);
            final List<MemorySegment> segsForChannel = inputSegments.get(i);

            // create a reader. if there are multiple segments for the reader, issue multiple
            // together per I/O request
            final BlockChannelReader<MemorySegment> reader =
                    this.ioManager.createBlockChannelReader(channel.getChannel());

            readerList.add(reader);
            spillChannelManager.registerOpenChannelToBeRemovedAtShutdown(reader);
            spillChannelManager.unregisterChannelToBeRemovedAtShutdown(channel.getChannel());

            // wrap channel reader as a view, to get block spanning record deserialization
            final ChannelReaderInputView inView =
                    new ChannelReaderInputView(
                            reader, segsForChannel, channel.getBlockCount(), false);
            iterators.add(new ChannelReaderInputViewIterator<>(inView, null, this.serializer));
        }

        if (largeRecords != null) {
            iterators.add(largeRecords);
        }

        return new MergeIterator<>(iterators, this.comparator);
    }

    /**
     * Merges the given sorted runs to a smaller number of sorted runs.
     *
     * @param channelIDs The IDs of the sorted runs that need to be merged.
     * @param allReadBuffers  存储数据用的临时容器
     * @param writeBuffers The buffers to be used by the writers.
     * @return A list of the IDs of the merged channels.
     * @throws IOException Thrown, if the readers or writers encountered an I/O problem.
     * 将channel文件对应的数据合并
     */
    private List<ChannelWithBlockCount> mergeChannelList(
            final List<ChannelWithBlockCount> channelIDs,
            final List<MemorySegment> allReadBuffers,
            final List<MemorySegment> writeBuffers)
            throws IOException {
        // A channel list with length maxFanIn<sup>i</sup> can be merged to maxFanIn files in i-1
        // rounds where every merge
        // is a full merge with maxFanIn input channels. A partial round includes merges with fewer
        // than maxFanIn
        // inputs. It is most efficient to perform the partial round first.
        final double scale = Math.ceil(Math.log(channelIDs.size()) / Math.log(this.maxFanIn)) - 1;

        final int numStart = channelIDs.size();
        final int numEnd = (int) Math.pow(this.maxFanIn, scale);

        final int numMerges = (int) Math.ceil((numStart - numEnd) / (double) (this.maxFanIn - 1));

        final int numNotMerged = numEnd - numMerges;
        final int numToMerge = numStart - numNotMerged;

        // unmerged channel IDs are copied directly to the result list
        final List<ChannelWithBlockCount> mergedChannelIDs = new ArrayList<>(numEnd);
        mergedChannelIDs.addAll(channelIDs.subList(0, numNotMerged));

        final int channelsToMergePerStep = (int) Math.ceil(numToMerge / (double) numMerges);

        // allocate the memory for the merging step
        final List<List<MemorySegment>> readBuffers = new ArrayList<>(channelsToMergePerStep);

        // 为内存块分组
        getSegmentsForReaders(readBuffers, allReadBuffers, channelsToMergePerStep);

        final List<ChannelWithBlockCount> channelsToMergeThisStep =
                new ArrayList<>(channelsToMergePerStep);
        int channelNum = numNotMerged;
        while (isRunning() && channelNum < channelIDs.size()) {
            channelsToMergeThisStep.clear();

            // 选择channel进行合并
            for (int i = 0;
                    i < channelsToMergePerStep && channelNum < channelIDs.size();
                    i++, channelNum++) {
                channelsToMergeThisStep.add(channelIDs.get(channelNum));
            }

            mergedChannelIDs.add(mergeChannels(channelsToMergeThisStep, readBuffers, writeBuffers));
        }

        return mergedChannelIDs;
    }

    /**
     * Merges the sorted runs described by the given Channel IDs into a single sorted run. The
     * merging process uses the given read and write buffers.
     *
     * @param channelIDs The IDs of the runs' channels.   参与合并的一组channel
     * @param readBuffers The buffers for the readers that read the sorted runs.   用于存储文件数据的内存块
     * @param writeBuffers The buffers for the writer that writes the merged channel.   写入合并结果的内存块
     * @return The ID and number of blocks of the channel that describes the merged run.  多个文件最终会变成一个文件
     */
    private ChannelWithBlockCount mergeChannels(
            List<ChannelWithBlockCount> channelIDs,
            List<List<MemorySegment>> readBuffers,
            List<MemorySegment> writeBuffers)
            throws IOException {
        // the list with the readers, to be closed at shutdown
        final List<FileIOChannel> channelAccesses = new ArrayList<>(channelIDs.size());

        // the list with the target iterators
        // 借助MergingIterator 实现合并逻辑
        final MergeIterator<E> mergeIterator =
                getMergingIterator(channelIDs, readBuffers, channelAccesses, null);

        // create a new channel writer
        final FileIOChannel.ID mergedChannelID = this.ioManager.createChannel();
        spillChannelManager.registerChannelToBeRemovedAtShutdown(mergedChannelID);
        final BlockChannelWriter<MemorySegment> writer =
                this.ioManager.createBlockChannelWriter(mergedChannelID);
        spillChannelManager.registerOpenChannelToBeRemovedAtShutdown(writer);
        final ChannelWriterOutputView output =
                new ChannelWriterOutputView(writer, writeBuffers, this.memManager.getPageSize());

        openSpillingBehaviour();
        // 进行数据合并写入
        spillingBehaviour.mergeRecords(mergeIterator, output);
        output.close();
        final int numBlocksWritten = output.getBlockCount();

        // register merged result to be removed at shutdown
        spillChannelManager.unregisterOpenChannelToBeRemovedAtShutdown(writer);

        // remove the merged channel readers from the clear-at-shutdown list
        // 删除之前的文件
        for (FileIOChannel access : channelAccesses) {
            access.closeAndDelete();
            spillChannelManager.unregisterOpenChannelToBeRemovedAtShutdown(access);
        }

        // 返回merge后的文件
        return new ChannelWithBlockCount(mergedChannelID, numBlocksWritten);
    }

    /**
     * Divides the given collection of memory buffers among {@code numChannels} sublists.
     *
     * @param target The list into which the lists with buffers for the channels are put.
     * @param memory A list containing the memory buffers to be distributed. The buffers are not
     *     removed from this list.
     * @param numChannels The number of channels for which to allocate buffers. Must not be zero.
     *                    简单来说就是分配内存块
     */
    private void getSegmentsForReaders(
            List<List<MemorySegment>> target, List<MemorySegment> memory, int numChannels) {
        // determine the memory to use per channel and the number of buffers
        final int numBuffers = memory.size();
        // 每个channel可以使用的内存数量
        final int buffersPerChannelLowerBound = numBuffers / numChannels;
        final int numChannelsWithOneMore = numBuffers % numChannels;

        final Iterator<MemorySegment> segments = memory.iterator();

        // collect memory for the channels that get one segment more
        for (int i = 0; i < numChannelsWithOneMore; i++) {
            final ArrayList<MemorySegment> segs = new ArrayList<>(buffersPerChannelLowerBound + 1);
            target.add(segs);
            for (int k = buffersPerChannelLowerBound; k >= 0; k--) {
                segs.add(segments.next());
            }
        }

        // collect memory for the remaining channels
        for (int i = numChannelsWithOneMore; i < numChannels; i++) {
            final ArrayList<MemorySegment> segs = new ArrayList<>(buffersPerChannelLowerBound);
            target.add(segs);
            for (int k = buffersPerChannelLowerBound; k > 0; k--) {
                segs.add(segments.next());
            }
        }
    }
}
