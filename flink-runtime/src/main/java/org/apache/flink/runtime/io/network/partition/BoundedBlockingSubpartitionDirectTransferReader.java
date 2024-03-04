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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The reader (read view) of a BoundedBlockingSubpartition based on {@link
 * org.apache.flink.shaded.netty4.io.netty.channel.FileRegion}.
 * 也是用于读取子分区数据的
 */
public class BoundedBlockingSubpartitionDirectTransferReader implements ResultSubpartitionView {

    /** The result subpartition that we read.
     * 该reader读取的子分区
     * */
    private final BoundedBlockingSubpartition parent;

    /** The reader/decoder to the file region with the data we currently read from.
     * 读取子分区数据的reader
     * */
    private final BoundedData.Reader dataReader;

    /** The remaining number of data buffers (not events) in the result.
     * 剩余多少 data buffer
     * */
    private int numDataBuffers;

    /** The remaining number of data buffers and events in the result. */
    private int numDataAndEventBuffers;

    /** Flag whether this reader is released. */
    private boolean isReleased;

    private int sequenceNumber;

    BoundedBlockingSubpartitionDirectTransferReader(
            BoundedBlockingSubpartition parent,
            Path filePath,
            int numDataBuffers,
            int numDataAndEventBuffers)
            throws IOException {
        int numEvents = numDataAndEventBuffers - numDataBuffers;
        checkArgument(
                numEvents == 1
                        || numEvents
                                == 2 /* EndOfData might not be generated e.g. in DataSet API */,
                "Too many event buffers.");
        this.parent = checkNotNull(parent);

        checkNotNull(filePath);

        // 直接从文件读取数据的reader对象
        this.dataReader = new FileRegionReader(filePath);

        checkArgument(numDataBuffers >= 0);
        this.numDataBuffers = numDataBuffers;

        checkArgument(numDataAndEventBuffers >= 0);
        this.numDataAndEventBuffers = numDataAndEventBuffers;
    }

    /**
     * 获取下个数据块 (其实就是一条记录)
     * @return
     * @throws IOException
     */
    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        if (isReleased) {
            return null;
        }

        Buffer current = dataReader.nextBuffer();
        if (current == null) {
            // as per contract, we must return null when the reader is empty,
            // but also in case the reader is disposed (rather than throwing an exception)
            return null;
        }

        // 更新统计信息  就是更新data buffer数量
        updateStatistics(current);

        // We simply assume all the data except for the last one (EndOfPartitionEvent)
        // are non-events for batch jobs to avoid pre-fetching the next header
        // 使用猜测的方式 ?
        Buffer.DataType nextDataType = Buffer.DataType.NONE;
        if (numDataBuffers > 0) {
            nextDataType = Buffer.DataType.DATA_BUFFER;
        } else if (numDataAndEventBuffers > 0) {
            nextDataType = Buffer.DataType.EVENT_BUFFER;
        }
        return BufferAndBacklog.fromBufferAndLookahead(
                current, nextDataType, numDataBuffers, sequenceNumber++);
    }

    private void updateStatistics(Buffer buffer) {
        if (buffer.isBuffer()) {
            numDataBuffers--;
        }
        numDataAndEventBuffers--;
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        // We simply assume there are no events except EndOfPartitionEvent for bath jobs,
        // then it has no essential effect to ignore the judgement of next event buffer.
        return new AvailabilityWithBacklog(
                (numCreditsAvailable > 0 || numDataBuffers == 0) && numDataAndEventBuffers > 0,
                numDataBuffers);
    }

    /**
     * 释放相关资源
     * @throws IOException
     */
    @Override
    public void releaseAllResources() throws IOException {
        // it is not a problem if this method executes multiple times
        isReleased = true;

        IOUtils.closeQuietly(dataReader);

        // Notify the parent that this one is released. This allows the parent to
        // eventually release all resources (when all readers are done and the
        // parent is disposed).
        parent.releaseReaderReference(this);
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public Throwable getFailureCause() {
        // we can never throw an error after this was created
        return null;
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return parent.unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return parent.getNumberOfQueuedBuffers();
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        parent.bufferSize(newBufferSize);
    }

    @Override
    public void notifyDataAvailable() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        // in case of bounded partitions there is no upstream to acknowledge, we simply ignore
        // the ack, as there are no checkpoints
    }

    @Override
    public String toString() {
        return String.format(
                "Blocking Subpartition Reader: ID=%s, index=%d",
                parent.parent.getPartitionId(), parent.getSubPartitionIndex());
    }

    /**
     * The reader to read from {@link BoundedBlockingSubpartition} and return the wrapped {@link
     * org.apache.flink.shaded.netty4.io.netty.channel.FileRegion} based buffer.
     * 直接从文件读取数据
     */
    static final class FileRegionReader implements BoundedData.Reader {

        private final FileChannel fileChannel;

        private final ByteBuffer headerBuffer;

        FileRegionReader(Path filePath) throws IOException {
            this.fileChannel = FileChannel.open(filePath, StandardOpenOption.READ);
            this.headerBuffer = BufferReaderWriterUtil.allocatedHeaderBuffer();
        }

        @Nullable
        @Override
        public Buffer nextBuffer() throws IOException {
            return BufferReaderWriterUtil.readFileRegionFromByteChannel(fileChannel, headerBuffer);
        }

        @Override
        public void close() throws IOException {
            fileChannel.close();
        }
    }
}
