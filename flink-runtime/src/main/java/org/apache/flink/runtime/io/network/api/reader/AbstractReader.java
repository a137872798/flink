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

package org.apache.flink.runtime.io.network.api.reader;

import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.TaskEventHandler;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/** A basic reader implementation, which wraps an input gate and handles events.
 * 基础的reader骨架类
 * */
public abstract class AbstractReader implements ReaderBase {

    /** The input gate to read from.
     * 从gate中读取数据
     * */
    protected final InputGate inputGate;

    /** The task event handler to manage task event subscriptions.
     * 通过该对象维护事件监听器  以及发送事件
     * */
    private final TaskEventHandler taskEventHandler = new TaskEventHandler();

    /** Flag indicating whether this reader allows iteration events.
     * 表示是否允许迭代事件
     * */
    private boolean isIterative;

    /**
     * The current number of end of superstep events (reset for each superstep). A superstep is
     * finished after an end of superstep event has been received for each input channel.
     * 表示此时收到了多少 end of superstep events 事件
     */
    private int currentNumberOfEndOfSuperstepEvents;

    protected AbstractReader(InputGate inputGate) {
        this.inputGate = inputGate;
    }

    @Override
    public boolean isFinished() {
        return inputGate.isFinished();
    }

    // ------------------------------------------------------------------------
    // Events
    // ------------------------------------------------------------------------

    @Override
    public void registerTaskEventListener(
            EventListener<TaskEvent> listener, Class<? extends TaskEvent> eventType) {
        taskEventHandler.subscribe(listener, eventType);
    }

    @Override
    public void sendTaskEvent(TaskEvent event) throws IOException {
        inputGate.sendTaskEvent(event);
    }

    /**
     * Handles the event and returns whether the reader reached an end-of-stream event (either the
     * end of the whole stream or the end of an superstep).
     * 这个事件不同于 taskEvent
     */
    protected boolean handleEvent(AbstractEvent event) throws IOException {
        final Class<?> eventType = event.getClass();

        try {
            // ------------------------------------------------------------
            // Runtime events
            // ------------------------------------------------------------

            // This event is also checked at the (single) input gate to release the respective
            // channel, at which it was received.
            // 分区end事件不处理
            if (eventType == EndOfPartitionEvent.class) {
                return true;

                // 表示某个superstep结束了
            } else if (eventType == EndOfSuperstepEvent.class) {
                return incrementEndOfSuperstepEventAndCheck();
            }

            // ------------------------------------------------------------
            // Task events (user)
            // ------------------------------------------------------------
            // 任务事件 转给handler处理
            else if (event instanceof TaskEvent) {
                taskEventHandler.publish((TaskEvent) event);

                return false;
            } else {
                throw new IllegalStateException(
                        "Received unexpected event of type " + eventType + " at reader.");
            }
        } catch (Throwable t) {
            throw new IOException(
                    "Error while handling event of type " + eventType + ": " + t.getMessage(), t);
        }
    }

    public void publish(TaskEvent event) {
        taskEventHandler.publish(event);
    }

    // ------------------------------------------------------------------------
    // Iterations
    // ------------------------------------------------------------------------

    @Override
    public void setIterativeReader() {
        isIterative = true;
    }

    @Override
    public void startNextSuperstep() {
        checkState(isIterative, "Tried to start next superstep in a non-iterative reader.");
        checkState(
                currentNumberOfEndOfSuperstepEvents == inputGate.getNumberOfInputChannels(),
                "Tried to start next superstep before reaching end of previous superstep.");

        // 相当于重置了  需要等superstepEvent数量达到一定值 才能进入下一阶段
        currentNumberOfEndOfSuperstepEvents = 0;
    }

    @Override
    public boolean hasReachedEndOfSuperstep() {
        return isIterative
                && currentNumberOfEndOfSuperstepEvents == inputGate.getNumberOfInputChannels();
    }

    private boolean incrementEndOfSuperstepEventAndCheck() {
        checkState(isIterative, "Tried to increment superstep count in a non-iterative reader.");
        checkState(
                currentNumberOfEndOfSuperstepEvents + 1 <= inputGate.getNumberOfInputChannels(),
                "Received too many ("
                        + currentNumberOfEndOfSuperstepEvents
                        + ") end of superstep events.");

        return ++currentNumberOfEndOfSuperstepEvents == inputGate.getNumberOfInputChannels();
    }
}
