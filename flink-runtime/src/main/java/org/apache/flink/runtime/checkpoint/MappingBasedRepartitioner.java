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

package org.apache.flink.runtime.checkpoint;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A repartitioner that assigns the same channel state to multiple subtasks according to some
 * mapping.
 *
 * <p>The replicated data will then be filtered before processing the record.
 *
 * <p>Note that channel mappings are cached for the same parallelism changes.
 * 利用 RescaleMappings 对已有的状态进行重分区
 */
@NotThreadSafe
public class MappingBasedRepartitioner<T> implements OperatorStateRepartitioner<T> {
    private final RescaleMappings newToOldSubtasksMapping;

    public MappingBasedRepartitioner(RescaleMappings newToOldSubtasksMapping) {
        this.newToOldSubtasksMapping = newToOldSubtasksMapping;
    }

    /**
     *
     * @param previousParallelSubtaskStates  所有数据
     * @param oldIndexes  被选中的旧分区
     * @param <T>
     * @return
     */
    private static <T> List<T> extractOldState(
            List<List<T>> previousParallelSubtaskStates, int[] oldIndexes) {
        switch (oldIndexes.length) {
            case 0:
                return Collections.emptyList();
            case 1:
                return previousParallelSubtaskStates.get(oldIndexes[0]);
            default:
                return Arrays.stream(oldIndexes)
                        .boxed()
                        .flatMap(oldIndex -> previousParallelSubtaskStates.get(oldIndex).stream())
                        .collect(Collectors.toList());
        }
    }

    @Override
    public List<List<T>> repartitionState(
            List<List<T>> previousParallelSubtaskStates, int oldParallelism, int newParallelism) {
        List<List<T>> repartitioned = new ArrayList<>();
        // 将state按照新分区排序
        for (int newIndex = 0; newIndex < newParallelism; newIndex++) {
            repartitioned.add(
                    extractOldState(
                            previousParallelSubtaskStates,
                            newToOldSubtasksMapping.getMappedIndexes(newIndex)));
        }
        return repartitioned;
    }
}
