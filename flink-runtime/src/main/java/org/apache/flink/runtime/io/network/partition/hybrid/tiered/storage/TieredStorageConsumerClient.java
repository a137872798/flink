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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** {@link TieredStorageConsumerClient} is used to read buffer from tiered store.
 * 消费数据的客户端
 * */
public class TieredStorageConsumerClient {

    private final List<TierFactory> tierFactories;

    /**
     * 这个对象只是管理了一些reader/writer
     */
    private final TieredStorageNettyService nettyService;

    /**
     * agent包含拉取数据的api
     */
    private final List<TierConsumerAgent> tierConsumerAgents;

    /**
     * This map is used to record the consumer agent being used and the id of segment being read for
     * each data source, which is represented by {@link TieredStoragePartitionId} and {@link
     * TieredStorageSubpartitionId}.
     */
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, Tuple2<TierConsumerAgent, Integer>>>
            currentConsumerAgentAndSegmentIds = new HashMap<>();

    public TieredStorageConsumerClient(
            List<TierFactory> tierFactories,
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageNettyService nettyService) {
        this.tierFactories = tierFactories;
        this.nettyService = nettyService;
        // 每个工厂生成一个agent
        this.tierConsumerAgents = createTierConsumerAgents(tieredStorageConsumerSpecs);
    }

    /**
     * 用这个对象统一管理所有探针
     */
    public void start() {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.start();
        }
    }

    /**
     *
     * @param partitionId
     * @param subpartitionId
     * @return
     */
    public Optional<Buffer> getNextBuffer(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {

        // 找到该子分区相关的 consumerAgent
        Tuple2<TierConsumerAgent, Integer> currentConsumerAgentAndSegmentId =
                currentConsumerAgentAndSegmentIds
                        .computeIfAbsent(partitionId, ignore -> new HashMap<>())
                        .getOrDefault(subpartitionId, Tuple2.of(null, 0));
        Optional<Buffer> buffer = Optional.empty();
        if (currentConsumerAgentAndSegmentId.f0 == null) {

            // 尝试用任意一个agent消费
            for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
                buffer =
                        tierConsumerAgent.getNextBuffer(
                                partitionId, subpartitionId, currentConsumerAgentAndSegmentId.f1);

                // 只要一个有数据就可以了
                if (buffer.isPresent()) {
                    // 采集到结果就可以加入list了
                    currentConsumerAgentAndSegmentIds
                            .get(partitionId)
                            .put(
                                    subpartitionId,
                                    Tuple2.of(
                                            tierConsumerAgent,
                                            currentConsumerAgentAndSegmentId.f1));
                    break;
                }
            }
        } else {
            // 已经存在就继续用该对象读取
            buffer =
                    currentConsumerAgentAndSegmentId.f0.getNextBuffer(
                            partitionId, subpartitionId, currentConsumerAgentAndSegmentId.f1);
        }

        // 表示已经没数据了
        if (!buffer.isPresent()) {
            return Optional.empty();
        }
        Buffer bufferData = buffer.get();
        // 表示数据读完了
        if (bufferData.getDataType() == Buffer.DataType.END_OF_SEGMENT) {
            currentConsumerAgentAndSegmentIds
                    .get(partitionId)
                    .put(subpartitionId, Tuple2.of(null, currentConsumerAgentAndSegmentId.f1 + 1));
            bufferData.recycleBuffer();
            // 这里会使用下个agent
            return getNextBuffer(partitionId, subpartitionId);
        }
        return Optional.of(bufferData);
    }

    public void registerAvailabilityNotifier(AvailabilityNotifier notifier) {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.registerAvailabilityNotifier(notifier);
        }
    }

    public void close() throws IOException {
        for (TierConsumerAgent tierConsumerAgent : tierConsumerAgents) {
            tierConsumerAgent.close();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Internal methods
    // --------------------------------------------------------------------------------------------

    /**
     *
     * @param tieredStorageConsumerSpecs
     * @return
     */
    private List<TierConsumerAgent> createTierConsumerAgents(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs) {
        ArrayList<TierConsumerAgent> tierConsumerAgents = new ArrayList<>();
        // 每个工厂都要创建agent对象
        for (TierFactory tierFactory : tierFactories) {
            tierConsumerAgents.add(
                    tierFactory.createConsumerAgent(tieredStorageConsumerSpecs, nettyService));
        }
        return tierConsumerAgents;
    }
}
