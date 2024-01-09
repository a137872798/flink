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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Internal;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link CombinedWatermarkStatus} combines the watermark (and idleness) updates of multiple
 * partitions/shards/splits into one combined watermark.
 * 维护一组水位状态
 */
@Internal
final class CombinedWatermarkStatus {

    /** List of all watermark outputs, for efficient access. */
    private final List<PartialWatermark> partialWatermarks = new ArrayList<>();

    /** The combined watermark over the per-output watermarks. 将多个水位合并后的结果 也是这些watermarks中最小的水位 */
    private long combinedWatermark = Long.MIN_VALUE;

    private boolean idle = false;

    public long getCombinedWatermark() {
        return combinedWatermark;
    }

    public boolean isIdle() {
        return idle;
    }

    // 添加和移除某水位
    public boolean remove(PartialWatermark o) {
        return partialWatermarks.remove(o);
    }
    public void add(PartialWatermark element) {
        partialWatermarks.add(element);
    }

    /**
     * Checks whether we need to update the combined watermark.
     *
     * <p><b>NOTE:</b>It can update {@link #isIdle()} status.
     *
     * @return true, if the combined watermark changed
     * combinedWatermark 需要根据当前所有水位进行计算
     */
    public boolean updateCombinedWatermark() {
        long minimumOverAllOutputs = Long.MAX_VALUE;

        // if we don't have any outputs minimumOverAllOutputs is not valid, it's still
        // at its initial Long.MAX_VALUE state and we must not emit that
        // 没有任何用于参考的水位
        if (partialWatermarks.isEmpty()) {
            return false;
        }

        boolean allIdle = true;
        for (PartialWatermark partialWatermark : partialWatermarks) {
            if (!partialWatermark.isIdle()) {
                minimumOverAllOutputs =
                        Math.min(minimumOverAllOutputs, partialWatermark.getWatermark());
                allIdle = false;
            }
        }

        this.idle = allIdle;

        // 只有当至少有一个非空闲 才能计算  反正就是更新最小的水位
        if (!allIdle && minimumOverAllOutputs > combinedWatermark) {
            combinedWatermark = minimumOverAllOutputs;
            return true;
        }

        return false;
    }

    /** Per-output watermark state. 每个发射出去的水位状态 */
    static class PartialWatermark {

        /**
         * 水位值 (时间戳)
         */
        private long watermark = Long.MIN_VALUE;
        /**
         * 水位有一个是否空闲属性  非空闲才会参与计算
         */
        private boolean idle = false;

        /**
         * 每个对象可以关联一个 update对象
         */
        private final WatermarkOutputMultiplexer.WatermarkUpdateListener onWatermarkUpdate;

        public PartialWatermark(
                WatermarkOutputMultiplexer.WatermarkUpdateListener onWatermarkUpdate) {
            this.onWatermarkUpdate = onWatermarkUpdate;
        }

        /**
         * Returns the current watermark timestamp. This will throw {@link IllegalStateException} if
         * the output is currently idle.
         */
        private long getWatermark() {
            checkState(!idle, "Output is idle.");
            return watermark;
        }

        /**
         * Returns true if the watermark was advanced, that is if the new watermark is larger than
         * the previous one.
         *
         * <p>Setting a watermark will clear the idleness flag.
         */
        public boolean setWatermark(long watermark) {
            // 当更新水位时 代表不再处于idle状态
            this.idle = false;
            final boolean updated = watermark > this.watermark;
            if (updated) {
                // 触发监听器 以及更新水位
                this.onWatermarkUpdate.onWatermarkUpdate(watermark);
                this.watermark = Math.max(watermark, this.watermark);
            }
            return updated;
        }

        private boolean isIdle() {
            return idle;
        }

        public void setIdle(boolean idle) {
            this.idle = idle;
        }
    }
}
