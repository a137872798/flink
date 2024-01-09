/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;

/**
 * Un-synchronized stream similar to Java's ByteArrayInputStream that also exposes the current
 * position.
 * 使用字节数组 作为内存块
 */
@Internal
public class ByteArrayInputStreamWithPos extends MemorySegmentInputStreamWithPos {

    private static final byte[] EMPTY = new byte[0];

    public ByteArrayInputStreamWithPos() {
        this(EMPTY);
    }

    public ByteArrayInputStreamWithPos(byte[] buffer) {
        this(buffer, 0, buffer.length);
    }

    public ByteArrayInputStreamWithPos(byte[] buffer, int offset, int length) {
        super(MemorySegmentFactory.wrap(buffer), offset, length);
    }

    public void setBuffer(byte[] buffer, int off, int len) {
        setSegment(MemorySegmentFactory.wrap(buffer), off, len);
    }
}
