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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartition;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The default implementation of {@link TieredStorageNettyService}.
 * 该对象用于管理生产者/消费者
 * */
public class TieredStorageNettyServiceImpl implements TieredStorageNettyService {

    // ------------------------------------
    //          For producer side
    // ------------------------------------

    private final Map<TieredStoragePartitionId, List<NettyServiceProducer>>
            registeredServiceProducers = new ConcurrentHashMap<>();

    // ------------------------------------
    //          For consumer side
    // ------------------------------------

    /** The initialization in consumer side is thread-safe.
     * 细化到子分区级别
     * */
    private final Map<
                    TieredStoragePartitionId,
                    Map<TieredStorageSubpartitionId, List<NettyConnectionReaderRegistration>>>
            nettyConnectionReaderRegistrations = new HashMap<>();

    /**
     * 用于管理维护资源
     */
    private final TieredStorageResourceRegistry resourceRegistry;

    public TieredStorageNettyServiceImpl(TieredStorageResourceRegistry resourceRegistry) {
        this.resourceRegistry = resourceRegistry;
    }

    /**
     * 注册数据生产者
     */
    @Override
    public void registerProducer(
            TieredStoragePartitionId partitionId, NettyServiceProducer serviceProducer) {
        registeredServiceProducers
                .computeIfAbsent(
                        partitionId,
                        ignore -> {
                            final TieredStoragePartitionId id = partitionId;
                            resourceRegistry.registerResource(
                                    // 这里只注册了一个释放函数
                                    id, () -> registeredServiceProducers.remove(id));
                            return new ArrayList<>();
                        })
                .add(serviceProducer);
    }

    /**
     * 注册消费者
     * @param partitionId partition id indicates the unique id of {@link TieredResultPartition}.
     * @param subpartitionId subpartition id indicates the unique id of subpartition.
     * @return
     */
    @Override
    public CompletableFuture<NettyConnectionReader> registerConsumer(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {

        // 找到子分区对应的一组reader
        List<NettyConnectionReaderRegistration> registrations =
                getReaderRegistration(partitionId, subpartitionId);
        for (NettyConnectionReaderRegistration registration : registrations) {
            Optional<CompletableFuture<NettyConnectionReader>> futureOpt =
                    registration.trySetConsumer();
            // 只要一个reader可用即可
            if (futureOpt.isPresent()) {
                return futureOpt.get();
            }
        }

        // 此时都没有可用的reader
        NettyConnectionReaderRegistration registration = new NettyConnectionReaderRegistration();
        registrations.add(registration);
        // 等待reader被设置
        return registration.trySetConsumer().get();
    }

    /**
     * Create a {@link ResultSubpartitionView} for the netty server.
     *
     * @param partitionId partition id indicates the unique id of {@link TieredResultPartition}.
     * @param subpartitionId subpartition id indicates the unique id of subpartition.
     * @param availabilityListener listener is used to listen the available status of data.
     * @return the {@link TieredStorageResultSubpartitionView}.
     * 产生读取数据的视图对象
     */
    public ResultSubpartitionView createResultSubpartitionView(
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            BufferAvailabilityListener availabilityListener) {
        List<NettyServiceProducer> serviceProducers = registeredServiceProducers.get(partitionId);

        // 没有生产者 产生一个空对象
        if (serviceProducers == null) {
            return new TieredStorageResultSubpartitionView(
                    availabilityListener, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        }
        List<NettyPayloadManager> nettyPayloadManagers = new ArrayList<>();
        List<NettyConnectionId> nettyConnectionIds = new ArrayList<>();

        // 此时已经注册了一些生产者
        for (NettyServiceProducer serviceProducer : serviceProducers) {
            // 每个生产者对应一个manager
            NettyPayloadManager nettyPayloadManager = new NettyPayloadManager();
            NettyConnectionWriterImpl writer =
                    new NettyConnectionWriterImpl(nettyPayloadManager, availabilityListener);
            // 使用writer建立连接
            serviceProducer.connectionEstablished(subpartitionId, writer);
            // 每个生产者对应一条连接
            nettyConnectionIds.add(writer.getNettyConnectionId());
            nettyPayloadManagers.add(nettyPayloadManager);
        }

        // 使用多个manager/connectionIds 来进行初始化
        return new TieredStorageResultSubpartitionView(
                availabilityListener,
                nettyPayloadManagers,
                nettyConnectionIds,
                registeredServiceProducers.get(partitionId));
    }

    /**
     * Set up input channels in {@link SingleInputGate}. The method will be invoked by the pekko rpc
     * thread at first, and then the method {@link
     * TieredStorageNettyService#registerConsumer(TieredStoragePartitionId,
     * TieredStorageSubpartitionId)} will be invoked by the same thread sequentially, which ensures
     * thread safety.
     *
     * @param tieredStorageConsumerSpecs specs indicates {@link TieredResultPartition} and {@link
     *     TieredStorageSubpartitionId}.
     * @param inputChannelProviders it provides input channels for subpartitions.
     *
     *                              按照channel 用于读取数据
     */
    public void setupInputChannels(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            List<Supplier<InputChannel>> inputChannelProviders) {
        checkState(tieredStorageConsumerSpecs.size() == inputChannelProviders.size());
        for (int index = 0; index < tieredStorageConsumerSpecs.size(); ++index) {
            setupInputChannel(
                    index,
                    tieredStorageConsumerSpecs.get(index).getPartitionId(),
                    tieredStorageConsumerSpecs.get(index).getSubpartitionId(),
                    inputChannelProviders.get(index));
        }
    }

    /**
     * 根据提示信息设置channel
     * @param index
     * @param partitionId
     * @param subpartitionId
     * @param inputChannelProvider
     */
    private void setupInputChannel(
            int index,
            TieredStoragePartitionId partitionId,
            TieredStorageSubpartitionId subpartitionId,
            Supplier<InputChannel> inputChannelProvider) {

        // 找到相关的一组reader
        List<NettyConnectionReaderRegistration> registrations =
                getReaderRegistration(partitionId, subpartitionId);
        boolean hasSetChannel = false;
        // 给所有reader注册
        for (NettyConnectionReaderRegistration registration : registrations) {
            if (registration.trySetChannel(index, inputChannelProvider)) {
                hasSetChannel = true;
            }
        }

        // 只要有一个注册成功  就不再维护该分区的reader信息
        if (hasSetChannel) {
            removeRegistration(partitionId, subpartitionId);
            return;
        }

        // 都没有注册成功的情况下 新建并追加一个
        NettyConnectionReaderRegistration registration = new NettyConnectionReaderRegistration();
        registration.trySetChannel(index, inputChannelProvider);
        registrations.add(registration);
    }

    /**
     * 移除该子分区下所有reader
     * @param partitionId
     * @param subpartitionId
     */
    private void removeRegistration(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        Map<TieredStorageSubpartitionId, List<NettyConnectionReaderRegistration>>
                subpartitionRegistrations = nettyConnectionReaderRegistrations.get(partitionId);
        subpartitionRegistrations.remove(subpartitionId);
        if (subpartitionRegistrations.isEmpty()) {
            nettyConnectionReaderRegistrations.remove(partitionId);
        }
    }

    private List<NettyConnectionReaderRegistration> getReaderRegistration(
            TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
        return nettyConnectionReaderRegistrations
                .computeIfAbsent(partitionId, (ignore) -> new HashMap<>())
                .computeIfAbsent(subpartitionId, (ignore) -> new ArrayList<>());
    }

    /**
     * This class is used for pairing input channels with data consumers. Each registration consists
     * of exactly 1 input channel and exactly 1 data consumer. Upon both input channel and data
     * consumer are set, it creates a {@link NettyConnectionReader} through with the data consumer
     * reads data from the input channel.
     */
    private static class NettyConnectionReaderRegistration {

        /** This can be negative iff channel is not yet set. */
        private int channelIndex = -1;

        /** This can be null iff channel is not yet set.
         * reader读取的channel
         * */
        @Nullable private Supplier<InputChannel> channelSupplier;

        /** This can be null iff reader is not yet set.
         * NettyConnectionReader 用于从InputChannel读取数据
         * */
        @Nullable private CompletableFuture<NettyConnectionReader> readerFuture;

        /**
         * Try to set input channel.
         *
         * @param channelIndex the index of channel.
         * @param channelSupplier supplier to provide channel.
         * @return true if the channel is successfully set, or false if the registration already has
         *     an input channel.
         *     设置channel
         */
        public boolean trySetChannel(int channelIndex, Supplier<InputChannel> channelSupplier) {
            if (isChannelSet()) {
                return false;
            }

            checkArgument(channelIndex >= 0);
            this.channelIndex = channelIndex;
            this.channelSupplier = checkNotNull(channelSupplier);

            tryCreateNettyConnectionReader();

            return true;
        }

        /**
         * Try to set data consumer.
         *
         * @return a future that provides the netty connection reader upon its created, or empty if
         *     the registration already has a consumer.
         *     设置reader
         */
        public Optional<CompletableFuture<NettyConnectionReader>> trySetConsumer() {
            if (!isReaderSet()) {
                this.readerFuture = new CompletableFuture<>();
                return Optional.of(readerFuture);
            }

            tryCreateNettyConnectionReader();

            return Optional.empty();
        }

        /**
         * 2个同时设置时  为future设置 NettyConnectionReaderImpl
         */
        void tryCreateNettyConnectionReader() {
            if (isChannelSet() && isReaderSet()) {
                readerFuture.complete(new NettyConnectionReaderImpl(channelSupplier));
            }
        }

        private boolean isChannelSet() {
            return channelIndex >= 0;
        }

        private boolean isReaderSet() {
            return readerFuture != null;
        }
    }
}
