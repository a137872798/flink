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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.HeaderlessChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An output view that buffers written data in memory pages and spills them when they are full.
 * 当内存足够时 暂存在内存  当内存不足时 则先将数据写入磁盘
 * */
public class SpillingBuffer extends AbstractPagedOutputView {

    private final ArrayList<MemorySegment> fullSegments;

    private final MemorySegmentSource memorySource;

    private BlockChannelWriter<MemorySegment> writer;

    /**
     * 表示数据来源于内存数据
     */
    private RandomAccessInputView inMemInView;

    /**
     * 表示数据来源于已经写入的底层文件
     */
    private HeaderlessChannelReaderInputView externalInView;

    private final IOManager ioManager;

    private int blockCount;

    private int numBytesInLastSegment;

    /**
     * 表示待写入的buffer数量
     */
    private int numMemorySegmentsInWriter;

    public SpillingBuffer(IOManager ioManager, MemorySegmentSource memSource, int segmentSize) {
        super(memSource.nextSegment(), segmentSize, 0);

        this.fullSegments = new ArrayList<MemorySegment>(16);
        this.memorySource = memSource;
        this.ioManager = ioManager;
    }

    /**
     * 尝试获取下个数据页时触发该方法
     * @param current The current memory segment
     * @param positionInCurrent The position in the segment, one after the last valid byte.
     * @return
     * @throws IOException
     */
    @Override
    protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
            throws IOException {
        // check if we are still in memory
        // 第一次调用 writer还是null
        if (this.writer == null) {
            // 将使用完的内存块加入 fullSegments
            this.fullSegments.add(current);

            // 生成新的内存块
            final MemorySegment nextSeg = this.memorySource.nextSegment();
            if (nextSeg != null) {
                return nextSeg;
            } else {
                // out of memory, need to spill: create a writer
                // 表示所有内存块都已经使用完毕了  可以开始写数据了 初始化writer
                this.writer =
                        this.ioManager.createBlockChannelWriter(this.ioManager.createChannel());

                // add all segments to the writer
                this.blockCount = this.fullSegments.size();
                this.numMemorySegmentsInWriter = this.blockCount;
                // 一次性全部写入
                for (int i = 0; i < this.fullSegments.size(); i++) {
                    this.writer.writeBlock(this.fullSegments.get(i));
                }
                this.fullSegments.clear();
                // 阻塞等待结果
                final MemorySegment seg = this.writer.getNextReturnedBlock();
                this.numMemorySegmentsInWriter--;
                return seg;
            }
        } else {
            // spilling   一开始累计到一定量后一次性触发全部写请求 然后后面的请求就是直接加入了
            this.writer.writeBlock(current);
            this.blockCount++;
            return this.writer.getNextReturnedBlock();
        }
    }

    /**
     * 将输出流反转成一个输入流
     * @return
     * @throws IOException
     */
    public DataInputView flip() throws IOException {
        // check whether this is the first flip and we need to add the current segment to the full
        // ones
        // 表示当前有数据块
        if (getCurrentSegment() != null) {
            // first flip  表示此时还处于积累阶段  数据还在fullSegments中  直接利用该容器生成RandomAccessInputView
            if (this.writer == null) {
                // in memory
                this.fullSegments.add(getCurrentSegment());
                this.numBytesInLastSegment = getCurrentPositionInSegment();
                this.inMemInView =
                        new RandomAccessInputView(
                                this.fullSegments, this.segmentSize, this.numBytesInLastSegment);
            } else {
                // external: write the last segment and collect the memory back
                this.writer.writeBlock(this.getCurrentSegment());
                this.numMemorySegmentsInWriter++;

                this.numBytesInLastSegment = getCurrentPositionInSegment();
                this.blockCount++;
                this.writer.close();
                // 把内存块再取出来 回填到fullSegments
                for (int i = this.numMemorySegmentsInWriter; i > 0; i--) {
                    this.fullSegments.add(this.writer.getNextReturnedBlock());
                }
                this.numMemorySegmentsInWriter = 0;
            }

            // make sure we cannot write more
            clear();
        }

        if (this.writer == null) {
            // in memory
            this.inMemInView.setReadPosition(0);
            return this.inMemInView;
        } else {
            // recollect memory from a previous view
            if (this.externalInView != null) {
                this.externalInView.close();
            }

            // 此时数据已经写入到writer下的channel了  现在以输入流形式打开底层对象
            final BlockChannelReader<MemorySegment> reader =
                    this.ioManager.createBlockChannelReader(this.writer.getChannelID());

            this.externalInView =
                    new HeaderlessChannelReaderInputView(
                            reader,
                            this.fullSegments,
                            this.blockCount,
                            this.numBytesInLastSegment,
                            false);
            return this.externalInView;
        }
    }

    /**
     * @return A list with all memory segments that have been taken from the memory segment source.
     */
    public List<MemorySegment> close() throws IOException {
        final ArrayList<MemorySegment> segments =
                new ArrayList<MemorySegment>(
                        this.fullSegments.size() + this.numMemorySegmentsInWriter);

        // if the buffer is still being written, clean that up
        if (getCurrentSegment() != null) {
            segments.add(getCurrentSegment());
            clear();
        }

        // 将所有数据移动到临时容器
        moveAll(this.fullSegments, segments);
        this.fullSegments.clear();

        // clean up the writer
        if (this.writer != null) {
            // closing before the first flip, collect the memory in the writer
            this.writer.close();
            for (int i = this.numMemorySegmentsInWriter; i > 0; i--) {
                segments.add(this.writer.getNextReturnedBlock());
            }
            this.writer.closeAndDelete();
            this.writer = null;
        }

        // clean up the views  内存对象 置为null就会自动释放
        if (this.inMemInView != null) {
            this.inMemInView = null;
        }
        if (this.externalInView != null) {
            if (!this.externalInView.isClosed()) {
                this.externalInView.close();
            }
            this.externalInView = null;
        }
        return segments;
    }

    /**
     * Utility method that moves elements. It avoids copying the data into a dedicated array first,
     * as the {@link ArrayList#addAll(java.util.Collection)} method does.
     *
     * @param <E>
     * @param source
     * @param target
     */
    private static final <E> void moveAll(ArrayList<E> source, ArrayList<E> target) {
        target.ensureCapacity(target.size() + source.size());
        for (int i = source.size() - 1; i >= 0; i--) {
            target.add(source.remove(i));
        }
    }
}
