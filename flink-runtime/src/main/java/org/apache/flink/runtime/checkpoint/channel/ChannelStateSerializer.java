/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static java.lang.Math.addExact;
import static java.lang.Math.min;

/**
 * channel 对应分区    该对象则是用于将分区中的数据(或者立即为状态) 进行序列化
 */
interface ChannelStateSerializer {

    // 状态分为头部和数据

    void writeHeader(DataOutputStream dataStream) throws IOException;

    void writeData(DataOutputStream stream, Buffer... flinkBuffers) throws IOException;

    void readHeader(InputStream stream) throws IOException;

    int readLength(InputStream stream) throws IOException;

    int readData(InputStream stream, ChannelStateByteBuffer buffer, int bytes) throws IOException;

    /**
     * 根据offsets 抽取一组数据放置在一起
     * @param bytes
     * @param offsets
     * @return
     * @throws IOException
     */
    byte[] extractAndMerge(byte[] bytes, List<Long> offsets) throws IOException;

    long getHeaderLength();
}

/** Wrapper around various buffers to receive channel state data.
 * 存储状态的缓冲区
 * */
@Internal
@NotThreadSafe
interface ChannelStateByteBuffer extends AutoCloseable {

    /**
     * 判断buffer当前是否可写入数据
     * @return
     */
    boolean isWritable();

    @Override
    void close();

    /**
     * Read up to <code>bytesToRead</code> bytes into this buffer from the given {@link
     * InputStream}.
     *
     * @return the total number of bytes read into this buffer.
     * 将inputStream中的数据 读取到buffer中
     */
    int writeBytes(InputStream input, int bytesToRead) throws IOException;

    static ChannelStateByteBuffer wrap(Buffer buffer) {
        return new ChannelStateByteBuffer() {

            private final ByteBuf byteBuf = buffer.asByteBuf();

            @Override
            public boolean isWritable() {
                return byteBuf.isWritable();
            }

            @Override
            public void close() {
                buffer.recycleBuffer();
            }

            @Override
            public int writeBytes(InputStream input, int bytesToRead) throws IOException {
                return byteBuf.writeBytes(input, Math.min(bytesToRead, byteBuf.writableBytes()));
            }
        };
    }

    static ChannelStateByteBuffer wrap(BufferBuilder bufferBuilder) {
        final byte[] buf = new byte[1024];
        return new ChannelStateByteBuffer() {
            @Override
            public boolean isWritable() {
                return !bufferBuilder.isFull();
            }

            @Override
            public void close() {
                bufferBuilder.close();
            }

            @Override
            public int writeBytes(InputStream input, int bytesToRead) throws IOException {
                int left = bytesToRead;
                for (int toRead = getToRead(left); toRead > 0; toRead = getToRead(left)) {
                    int read = input.read(buf, 0, toRead);
                    int copied = bufferBuilder.append(java.nio.ByteBuffer.wrap(buf, 0, read));
                    Preconditions.checkState(copied == read);
                    left -= read;
                }
                bufferBuilder.commit();
                return bytesToRead - left;
            }

            private int getToRead(int bytesToRead) {
                return min(bytesToRead, min(buf.length, bufferBuilder.getWritableBytes()));
            }
        };
    }

    static ChannelStateByteBuffer wrap(byte[] bytes) {
        return new ChannelStateByteBuffer() {
            private int written = 0;

            @Override
            public boolean isWritable() {
                return written < bytes.length;
            }

            @Override
            public void close() {}

            @Override
            public int writeBytes(InputStream input, int bytesToRead) throws IOException {
                final int bytesRead = input.read(bytes, written, bytes.length - written);
                written += bytesRead;
                return bytesRead;
            }
        };
    }
}

/**
 * 该对象描述如何将状态持久化
 */
class ChannelStateSerializerImpl implements ChannelStateSerializer {
    private static final int SERIALIZATION_VERSION = 0;

    /**
     * 目前头部信息就是一个版本号 是为了以后做兼容的
     * @param dataStream
     * @throws IOException
     */
    @Override
    public void writeHeader(DataOutputStream dataStream) throws IOException {
        dataStream.writeInt(SERIALIZATION_VERSION);
    }

    @Override
    public void writeData(DataOutputStream stream, Buffer... flinkBuffers) throws IOException {
        // 计算数据的总长度 并写入
        stream.writeInt(getSize(flinkBuffers));
        for (Buffer buffer : flinkBuffers) {
            ByteBuf nettyByteBuf = buffer.asByteBuf();
            nettyByteBuf.getBytes(nettyByteBuf.readerIndex(), stream, nettyByteBuf.readableBytes());
        }
    }

    private int getSize(Buffer[] buffers) {
        int len = 0;
        for (Buffer buffer : buffers) {
            len = addExact(len, buffer.readableBytes());
        }
        return len;
    }

    @Override
    public void readHeader(InputStream stream) throws IOException {
        int version = readInt(stream);
        // 确保头部的version匹配
        Preconditions.checkArgument(
                version == SERIALIZATION_VERSION, "unsupported version: " + version);
    }

    @Override
    public int readLength(InputStream stream) throws IOException {
        int len = readInt(stream);
        Preconditions.checkArgument(len >= 0, "negative state size");
        return len;
    }

    @Override
    public int readData(InputStream stream, ChannelStateByteBuffer buffer, int bytes)
            throws IOException {
        return buffer.writeBytes(stream, bytes);
    }

    private static int readInt(InputStream stream) throws IOException {
        return new DataInputStream(stream).readInt();
    }

    @Override
    public byte[] extractAndMerge(byte[] bytes, List<Long> offsets) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        // 把 bytes 各offset的数据都读取出来了
        byte[] merged = extractByOffsets(bytes, offsets);
        writeHeader(dataOutputStream);
        // 再写入
        dataOutputStream.writeInt(merged.length);
        dataOutputStream.write(merged, 0, merged.length);
        dataOutputStream.close();
        return out.toByteArray();
    }

    /**
     *
     * @param data  存储原始数据的容器
     * @param offsets
     * @return
     * @throws IOException
     */
    private byte[] extractByOffsets(byte[] data, List<Long> offsets) throws IOException {
        DataInputStream lengthReadingStream =
                new DataInputStream(new ByteArrayInputStream(data, 0, data.length));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long prevOffset = 0;
        for (long offset : offsets) {
            lengthReadingStream.skipBytes((int) (offset - prevOffset));
            // 读取长度信息
            int dataWithLengthOffset = (int) offset + Integer.BYTES;
            // 写入output
            out.write(data, dataWithLengthOffset, lengthReadingStream.readInt());
            prevOffset = dataWithLengthOffset;
        }
        return out.toByteArray();
    }

    @Override
    public long getHeaderLength() {
        return Integer.BYTES;
    }
}
