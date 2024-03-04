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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;

/** Interface for turning sequences of memory segments into records.
 * 该对象可以反序列化 record
 * */
public interface RecordDeserializer<T extends IOReadableWritable> {

    /** Status of the deserialization result. */
    enum DeserializationResult {

        /**
         * 表示消耗了buffer的数据 但是未产生完整的record
         */
        PARTIAL_RECORD(false, true),

        /**
         * 表示消耗了部分buffer数据 并且产生了一个完整的record
         */
        INTERMEDIATE_RECORD_FROM_BUFFER(true, false),

        /**
         * 消费完数据 产生record
         */
        LAST_RECORD_FROM_BUFFER(true, true);

        private final boolean isFullRecord;

        private final boolean isBufferConsumed;

        private DeserializationResult(boolean isFullRecord, boolean isBufferConsumed) {
            this.isFullRecord = isFullRecord;
            this.isBufferConsumed = isBufferConsumed;
        }

        public boolean isFullRecord() {
            return this.isFullRecord;
        }

        public boolean isBufferConsumed() {
            return this.isBufferConsumed;
        }
    }

    DeserializationResult getNextRecord(T target) throws IOException;

    /**
     * 使用buffer中的数据 以便产生新的record
     * @param buffer
     * @throws IOException
     */
    void setNextBuffer(Buffer buffer) throws IOException;

    /**
     * 清除buffer数据
     */
    void clear();

    /**
     * Gets the unconsumed buffer which needs to be persisted in unaligned checkpoint scenario.
     *
     * <p>Note that the unconsumed buffer might be null if the whole buffer was already consumed
     * before and there are no partial length or data remained in the end of buffer.
     *
     * 返回所有未消费的buffer
     */
    CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException;
}
