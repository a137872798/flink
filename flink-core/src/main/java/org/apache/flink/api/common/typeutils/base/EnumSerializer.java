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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** {@link TypeSerializer} for Java enums.
 * 有关java枚举的序列化对象
 * */
@Internal
public final class EnumSerializer<T extends Enum<T>> extends TypeSerializer<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> enumClass;

    /**
     * Maintain our own map of enum value to their ordinal, instead of directly using {@link
     * Enum#ordinal()}. This allows us to maintain backwards compatibility for previous serialized
     * data in the case that the order of enum constants was changed or new constants were added.
     *
     * <p>On a fresh start with no reconfiguration, the ordinals would simply be identical to the
     * enum constants actual ordinals. Ordinals may change after reconfiguration.
     * 每个枚举值 以及他们的序号
     */
    private Map<T, Integer> valueToOrdinal;

    /**
     * Array of enum constants with their indexes identical to their ordinals in the {@link
     * #valueToOrdinal} map. Serves as a bidirectional map to have fast access from ordinal to
     * value. May be reordered after reconfiguration.
     * 每个枚举值
     */
    private T[] values;

    public EnumSerializer(Class<T> enumClass) {
        this(enumClass, enumClass.getEnumConstants());
    }

    private EnumSerializer(Class<T> enumClass, T[] enumValues) {
        this.enumClass = checkNotNull(enumClass);
        this.values = checkNotNull(enumValues);
        checkArgument(Enum.class.isAssignableFrom(enumClass), "not an enum");

        checkArgument(this.values.length > 0, "cannot use an empty enum");

        this.valueToOrdinal = new EnumMap<>(this.enumClass);
        int i = 0;
        for (T value : values) {
            this.valueToOrdinal.put(value, i++);
        }
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public EnumSerializer<T> duplicate() {
        return this;
    }

    /**
     * 实例化时 只返回第一个枚举
     * @return
     */
    @Override
    public T createInstance() {
        checkState(values != null);
        return values[0];
    }

    @Override
    public T copy(T from) {
        return from;
    }

    @Override
    public T copy(T from, T reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return 4;
    }

    /**
     * 序列化时  只写入编号
     * @param record The record to serialize.
     * @param target The output view to write the serialized data to.
     * @throws IOException
     */
    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        // use our own maintained ordinals instead of the actual enum ordinal
        target.writeInt(valueToOrdinal.get(record));
    }

    /**
     * 反序列化时   因为value还是存在的 用序号查看即可
     * @param source The input view from which to read the data.
     * @return
     * @throws IOException
     */
    @Override
    public T deserialize(DataInputView source) throws IOException {
        return values[source.readInt()];
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return values[source.readInt()];
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.write(source, 4);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EnumSerializer) {
            EnumSerializer<?> other = (EnumSerializer<?>) obj;

            return other.enumClass == this.enumClass && Arrays.equals(values, other.values);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return enumClass.hashCode();
    }

    // --------------------------------------------------------------------------------------------

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        // may be null if this serializer was deserialized from an older version
        if (this.values == null) {
            this.values = enumClass.getEnumConstants();

            this.valueToOrdinal = new EnumMap<>(this.enumClass);
            int i = 0;
            for (T value : values) {
                this.valueToOrdinal.put(value, i++);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshotting & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public EnumSerializerSnapshot<T> snapshotConfiguration() {
        return new EnumSerializerSnapshot<>(enumClass, values);
    }

    // 一些简单的基础类型 都是不支持存储/读取快照的

    /** {@link TypeSerializerSnapshot} for {@link EnumSerializer}. */
    public static final class EnumSerializerSnapshot<T extends Enum<T>>
            implements TypeSerializerSnapshot<T> {
        private static final int CURRENT_VERSION = 3;

        private T[] previousEnums;
        private Class<T> enumClass;

        @SuppressWarnings("unused")
        public EnumSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        EnumSerializerSnapshot(Class<T> enumClass, T[] enums) {
            this.enumClass = checkNotNull(enumClass);
            this.previousEnums = checkNotNull(enums);
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        /**
         * 写入枚举信息  就是枚举类名 + 枚举值数量 + 每个枚举值的名字
         * @param out the {@link DataOutputView} to write the snapshot to.
         * @throws IOException
         */
        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            checkState(enumClass != null, "Enum class can not be null.");
            out.writeUTF(enumClass.getName());
            out.writeInt(previousEnums.length);
            for (T enumConstant : previousEnums) {
                out.writeUTF(enumConstant.name());
            }
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            // 通过类加载器 读取枚举类
            enumClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);

            int numEnumConstants = in.readInt();

            @SuppressWarnings("unchecked")
            T[] previousEnums = (T[]) Array.newInstance(enumClass, numEnumConstants);
            for (int i = 0; i < numEnumConstants; i++) {
                String enumName = in.readUTF();
                try {
                    // 通过api 可以得到具体的枚举值
                    previousEnums[i] = Enum.valueOf(enumClass, enumName);
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException(
                            "Could not create a restore serializer for enum "
                                    + enumClass
                                    + ". Probably because an enum value was removed.");
                }
            }

            this.previousEnums = previousEnums;
        }

        @Override
        public TypeSerializer<T> restoreSerializer() {
            checkState(enumClass != null, "Enum class can not be null.");

            return new EnumSerializer<>(enumClass, previousEnums);
        }

        /**
         * 检查2个序列化对象是否兼容  要检查前后枚举是否变化
         * @param newSerializer the new serializer to check.
         * @return
         */
        @Override
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializer<T> newSerializer) {
            if (!(newSerializer instanceof EnumSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            EnumSerializer<T> newEnumSerializer = (EnumSerializer<T>) newSerializer;
            if (!enumClass.equals(newEnumSerializer.enumClass)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            T[] currentEnums = enumClass.getEnumConstants();

            if (Arrays.equals(previousEnums, currentEnums)) {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            }

            Set<T> reconfiguredEnumSet = new LinkedHashSet<>(Arrays.asList(previousEnums));
            reconfiguredEnumSet.addAll(Arrays.asList(currentEnums));

            @SuppressWarnings("unchecked")
            T[] reconfiguredEnums =
                    reconfiguredEnumSet.toArray(
                            (T[]) Array.newInstance(enumClass, reconfiguredEnumSet.size()));

            EnumSerializer<T> reconfiguredSerializer =
                    new EnumSerializer<>(enumClass, reconfiguredEnums);
            return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                    reconfiguredSerializer);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------

    @VisibleForTesting
    T[] getValues() {
        return values;
    }

    @VisibleForTesting
    Map<T, Integer> getValueToOrdinal() {
        return valueToOrdinal;
    }

    @Override
    public String toString() {
        return "EnumSerializer{"
                + "enumClass="
                + enumClass
                + ", values="
                + Arrays.toString(values)
                + '}';
    }
}
