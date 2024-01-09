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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

/** Serializer for serializing/deserializing BigDecimal values including null values.
 * 包含BigDecimal 序列化/反序列化相关的逻辑
 * */
@Internal
public final class BigDecSerializer extends TypeSerializerSingleton<BigDecimal> {

    private static final long serialVersionUID = 1L;

    public static final BigDecSerializer INSTANCE = new BigDecSerializer();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public BigDecimal createInstance() {
        return BigDecimal.ZERO;
    }

    @Override
    public BigDecimal copy(BigDecimal from) {
        return from;
    }

    @Override
    public BigDecimal copy(BigDecimal from, BigDecimal reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    /**
     * 这些类型 DataOutputView就没有现成的api可以直接使用了
     * @param record The record to serialize.
     * @param target The output view to write the serialized data to.
     * @throws IOException
     */
    @Override
    public void serialize(BigDecimal record, DataOutputView target) throws IOException {
        // null value support
        // 可以看到都是转而委托给 BigInteger的序列化对象
        if (record == null) {
            BigIntSerializer.writeBigInteger(null, target);
            return;
        }
        // fast paths for 0, 1, 10
        // only reference equality is checked because equals would be too expensive
        // 这里多写入一个0  代表小数部分为0
        else if (record == BigDecimal.ZERO) {
            BigIntSerializer.writeBigInteger(BigInteger.ZERO, target);
            target.writeInt(0);
            return;
        } else if (record == BigDecimal.ONE) {
            BigIntSerializer.writeBigInteger(BigInteger.ONE, target);
            target.writeInt(0);
            return;
        } else if (record == BigDecimal.TEN) {
            BigIntSerializer.writeBigInteger(BigInteger.TEN, target);
            target.writeInt(0);
            return;
        }
        // default
        // 应该是分别写入 整数 和 小数的部分
        BigIntSerializer.writeBigInteger(record.unscaledValue(), target);
        target.writeInt(record.scale());
    }

    @Override
    public BigDecimal deserialize(DataInputView source) throws IOException {
        return readBigDecimal(source);
    }

    @Override
    public BigDecimal deserialize(BigDecimal reuse, DataInputView source) throws IOException {
        return readBigDecimal(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        final boolean isNull = BigIntSerializer.copyBigInteger(source, target);

        // 读取一个小数部分
        if (!isNull) {
            final int scale = source.readInt();
            target.writeInt(scale);
        }
    }

    // --------------------------------------------------------------------------------------------
    //                           Static Helpers for BigInteger Serialization
    // --------------------------------------------------------------------------------------------

    /**
     * 还原 BigDecimal
     * @param source
     * @return
     * @throws IOException
     */
    public static BigDecimal readBigDecimal(DataInputView source) throws IOException {
        final BigInteger unscaledValue = BigIntSerializer.readBigInteger(source);
        if (unscaledValue == null) {
            return null;
        }
        final int scale = source.readInt();
        // fast-path for 0, 1, 10
        // 小数为0
        if (scale == 0) {
            if (unscaledValue == BigInteger.ZERO) {
                return BigDecimal.ZERO;
            } else if (unscaledValue == BigInteger.ONE) {
                return BigDecimal.ONE;
            } else if (unscaledValue == BigInteger.TEN) {
                return BigDecimal.TEN;
            }
        }
        // default
        // 把整数和小数 + 在一起
        return new BigDecimal(unscaledValue, scale);
    }

    @Override
    public TypeSerializerSnapshot<BigDecimal> snapshotConfiguration() {
        return new BigDecSerializerSnapshot();
    }

    // ------------------------------------------------------------------------

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class BigDecSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<BigDecimal> {

        public BigDecSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
