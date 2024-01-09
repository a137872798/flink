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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.CombinedWatermarkStatus.PartialWatermark;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link WatermarkOutputMultiplexer} combines the watermark (and idleness) updates of multiple
 * partitions/shards/splits into one combined watermark update and forwards it to an underlying
 * {@link WatermarkOutput}.
 *
 * <p>A multiplexed output can either be immediate or deferred. Watermark updates on an immediate
 * output will potentially directly affect the combined watermark state, which will be forwarded to
 * the underlying output immediately. Watermark updates on a deferred output will only update an
 * internal state but not directly update the combined watermark state. Only when {@link
 * #onPeriodicEmit()} is called will the deferred updates be combined and forwarded to the
 * underlying output.
 *
 * <p>For registering a new multiplexed output, you must first call {@link
 * #registerNewOutput(String)} and then call {@link #getImmediateOutput(String)} or {@link
 * #getDeferredOutput(String)} with the output ID you get from that. You can get both an immediate
 * and deferred output for a given output ID, you can also call the getters multiple times.
 *
 * <p><b>WARNING:</b>This class is not thread safe.
 * 该对象同时管理多个 WatermarkOutput
 */
@Internal
public class WatermarkOutputMultiplexer {
    /** A callback for propagating changes to split based watermarks. */
    @FunctionalInterface
    @Internal
    public interface WatermarkUpdateListener {
        /** Called when the watermark increases. */
        void onWatermarkUpdate(long watermark);
    }

    /**
     * The {@link WatermarkOutput} that we use to emit our multiplexed watermark updates. We assume
     * that outside code holds a coordinating lock so we don't lock in this class when accessing
     * this {@link WatermarkOutput}.
     * 多个output作用后的结果 通过该对象发射
     */
    private final WatermarkOutput underlyingOutput;

    /**
     * Map view, to allow finding them when requesting the {@link WatermarkOutput} for a given id.
     * 每个水位关联一个 output
     */
    private final Map<String, PartialWatermark> watermarkPerOutputId;

    /**
     * 用于管理一组PartialWatermark
     */
    private final CombinedWatermarkStatus combinedWatermarkStatus;

    /**
     * Creates a new {@link WatermarkOutputMultiplexer} that emits combined updates to the given
     * {@link WatermarkOutput}.
     */
    public WatermarkOutputMultiplexer(WatermarkOutput underlyingOutput) {
        this.underlyingOutput = underlyingOutput;
        this.watermarkPerOutputId = new HashMap<>();
        this.combinedWatermarkStatus = new CombinedWatermarkStatus();
    }

    /**
     * Registers a new multiplexed output, which creates internal states for that output and returns
     * an output ID that can be used to get a deferred or immediate {@link WatermarkOutput} for that
     * output.
     * 注册一个新的output对象
     */
    public void registerNewOutput(String id, WatermarkUpdateListener onWatermarkUpdate) {
        final PartialWatermark outputState = new PartialWatermark(onWatermarkUpdate);

        final PartialWatermark previouslyRegistered =
                watermarkPerOutputId.putIfAbsent(id, outputState);
        checkState(previouslyRegistered == null, "Already contains an output for ID %s", id);

        combinedWatermarkStatus.add(outputState);
    }

    public boolean unregisterOutput(String id) {
        final PartialWatermark output = watermarkPerOutputId.remove(id);
        if (output != null) {
            combinedWatermarkStatus.remove(output);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns an immediate {@link WatermarkOutput} for the given output ID.
     *
     * <p>>See {@link WatermarkOutputMultiplexer} for a description of immediate and deferred
     * outputs.
     */
    public WatermarkOutput getImmediateOutput(String outputId) {
        final PartialWatermark outputState = watermarkPerOutputId.get(outputId);
        Preconditions.checkArgument(
                outputState != null, "no output registered under id %s", outputId);

        // 将 PartialWatermark 包装后得到 ImmediateOutput 可以直接获取水位值
        return new ImmediateOutput(outputState);
    }

    /**
     * Returns a deferred {@link WatermarkOutput} for the given output ID.
     *
     * <p>>See {@link WatermarkOutputMultiplexer} for a description of immediate and deferred
     * outputs.
     */
    public WatermarkOutput getDeferredOutput(String outputId) {
        final PartialWatermark outputState = watermarkPerOutputId.get(outputId);
        Preconditions.checkArgument(
                outputState != null, "no output registered under id %s", outputId);
        // 获得一个延期水位对象
        return new DeferredOutput(outputState);
    }

    /**
     * Tells the {@link WatermarkOutputMultiplexer} to combine all outstanding deferred watermark
     * updates and possibly emit a new update to the underlying {@link WatermarkOutput}.
     */
    public void onPeriodicEmit() {
        updateCombinedWatermark();
    }

    /**
     * Checks whether we need to update the combined watermark. Should be called when a newly
     * emitted per-output watermark is higher than the max so far or if we need to combined the
     * deferred per-output updates.
     */
    private void updateCombinedWatermark() {
        if (combinedWatermarkStatus.updateCombinedWatermark()) {
            // 发射更新后的水位
            underlyingOutput.emitWatermark(
                    new Watermark(combinedWatermarkStatus.getCombinedWatermark()));
            // 如果上游处于空闲状态  将下游也标记成空闲
        } else if (combinedWatermarkStatus.isIdle()) {
            underlyingOutput.markIdle();
        }
    }

    /**
     * Updating the state of an immediate output can possible lead to a combined watermark update to
     * the underlying {@link WatermarkOutput}.
     * 将PartialWatermark 包装成了output对象   往该对象发射有效水位时 下游会立刻观测到
     */
    private class ImmediateOutput implements WatermarkOutput {

        private final PartialWatermark state;

        public ImmediateOutput(PartialWatermark state) {
            this.state = state;
        }

        /**
         * 通过该对象发射水位
         * @param watermark
         */
        @Override
        public void emitWatermark(Watermark watermark) {
            long timestamp = watermark.getTimestamp();
            // 更新水位
            boolean wasUpdated = state.setWatermark(timestamp);

            // if it's higher than the max watermark so far we might have to update the
            // combined watermark
            if (wasUpdated && timestamp > combinedWatermarkStatus.getCombinedWatermark()) {
                // 因为某个output的水位发生了变化  尝试更新combinedWatermarkStatus的水位  并向下游发射水位
                updateCombinedWatermark();
            }
        }

        /**
         * 标记成空闲状态
         */
        @Override
        public void markIdle() {
            state.setIdle(true);

            // this can always lead to an advancing watermark. We don't know if this output
            // was holding back the watermark or not.
            // 有新的水位处于空闲状态 combinedWatermark需要重新计算
            updateCombinedWatermark();
        }

        @Override
        public void markActive() {
            state.setIdle(false);

            // stop potentially automatic advancing of the watermark
            updateCombinedWatermark();
        }
    }

    /**
     * Updating the state of a deferred output will never lead to a combined watermark update. Only
     * when {@link WatermarkOutputMultiplexer#onPeriodicEmit()} is called will the deferred updates
     * be combined into a potential combined update of the underlying {@link WatermarkOutput}.
     * 延迟对象的区别是 接收到水位时 不会直接作用到combinedWatermark上 也不会往下游发射   只有当主动调用onPeriodicEmit时 才会发射
     */
    private static class DeferredOutput implements WatermarkOutput {

        private final PartialWatermark state;

        public DeferredOutput(PartialWatermark state) {
            this.state = state;
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            state.setWatermark(watermark.getTimestamp());
        }

        @Override
        public void markIdle() {
            state.setIdle(true);
        }

        @Override
        public void markActive() {
            state.setIdle(false);
        }
    }
}
