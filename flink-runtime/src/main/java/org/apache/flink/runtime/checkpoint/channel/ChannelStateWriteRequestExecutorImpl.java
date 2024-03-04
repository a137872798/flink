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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Executes {@link ChannelStateWriteRequest}s in a separate thread. Any exception occurred during
 * execution causes this thread to stop and the exception to be re-thrown on any subsequent call.
 * 该对象作为门面对象 可以接收检查点相关的请求
 */
@ThreadSafe
class ChannelStateWriteRequestExecutorImpl implements ChannelStateWriteRequestExecutor {

    private static final Logger LOG =
            LoggerFactory.getLogger(ChannelStateWriteRequestExecutorImpl.class);

    private final Object lock = new Object();

    /**
     * 该对象底层对接 writer对象 包含了写入数据流的逻辑
     */
    private final ChannelStateWriteRequestDispatcher dispatcher;
    private final Thread thread;

    private final int maxSubtasksPerChannelStateFile;

    /**
     * 待处理的请求
     */
    @GuardedBy("lock")
    private final Deque<ChannelStateWriteRequest> deque;

    @GuardedBy("lock")
    private Exception thrown = null;

    @GuardedBy("lock")
    private boolean wasClosed = false;

    @GuardedBy("lock")
    private final Map<SubtaskID, Queue<ChannelStateWriteRequest>> unreadyQueues = new HashMap<>();

    /**
     * 一开始为空
     */
    @GuardedBy("lock")
    private final Set<SubtaskID> subtasks;

    /** Lock this before the {@link #lock} to avoid the deadlock. */
    private final Object registerLock;

    @GuardedBy("registerLock")
    private boolean isRegistering = true;

    @GuardedBy("registerLock")
    private final Consumer<ChannelStateWriteRequestExecutor> onRegistered;

    ChannelStateWriteRequestExecutorImpl(
            ChannelStateWriteRequestDispatcher dispatcher,
            int maxSubtasksPerChannelStateFile,
            Consumer<ChannelStateWriteRequestExecutor> onRegistered,
            Object registerLock) {
        this(
                dispatcher,
                new ArrayDeque<>(),
                maxSubtasksPerChannelStateFile,
                registerLock,
                onRegistered);
    }

    ChannelStateWriteRequestExecutorImpl(
            ChannelStateWriteRequestDispatcher dispatcher,
            Deque<ChannelStateWriteRequest> deque,
            int maxSubtasksPerChannelStateFile,
            Object registerLock,
            Consumer<ChannelStateWriteRequestExecutor> onRegistered) {
        this.dispatcher = dispatcher;
        this.deque = deque;
        this.maxSubtasksPerChannelStateFile = maxSubtasksPerChannelStateFile;
        this.registerLock = registerLock;
        this.onRegistered = onRegistered;
        this.thread = new Thread(this::run, "Channel state writer ");
        this.subtasks = new HashSet<>();
        this.thread.setDaemon(true);
    }

    @VisibleForTesting
    void run() {
        try {
            // 该对象维护避免资源泄漏 可以先忽略
            FileSystemSafetyNet.initializeSafetyNetForThread();
            loop();
        } catch (Exception ex) {
            thrown = ex;
        } finally {
            try {
                closeAll(
                        this::cleanupRequests,
                        () -> {
                            Throwable cause;
                            synchronized (lock) {
                                cause = thrown == null ? new CancellationException() : thrown;
                            }
                            dispatcher.fail(cause);
                        });
            } catch (Exception e) {
                synchronized (lock) {
                    //noinspection NonAtomicOperationOnVolatileField
                    thrown = ExceptionUtils.firstOrSuppressed(e, thrown);
                }
            }
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        }
        LOG.debug("loop terminated");
    }

    /**
     * 执行器 就是不断循环
     * @throws Exception
     */
    private void loop() throws Exception {
        while (true) {
            try {
                ChannelStateWriteRequest request;
                synchronized (lock) {
                    request = waitAndTakeUnsafe();
                    // 表示被关闭了
                    if (request == null) {
                        // The executor is closed, so return directly.
                        return;
                    }
                }
                // The executor will end the registration, when the start request comes.
                // Because the checkpoint can be started after all tasks are initiated.
                if (request instanceof CheckpointStartRequest) {
                    synchronized (registerLock) {
                        if (completeRegister()) {
                            // 首次启动检查点 触发钩子
                            onRegistered.accept(this);
                        }
                    }
                }

                // 分发请求
                dispatcher.dispatch(request);
            } catch (InterruptedException e) {
                synchronized (lock) {
                    if (!wasClosed) {
                        LOG.debug(
                                "Channel state executor is interrupted while waiting for a request (continue waiting)",
                                e);
                    } else {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    /**
     * Retrieves and removes the head request of the {@link #deque}, waiting if necessary until an
     * element becomes available.
     *
     * @return The head request, it can be null when the executor is closed.
     */
    @Nullable
    private ChannelStateWriteRequest waitAndTakeUnsafe() throws InterruptedException {
        ChannelStateWriteRequest request;
        while (!wasClosed) {
            request = deque.pollFirst();
            if (request == null) {
                lock.wait();
            } else {
                return request;
            }
        }
        return null;
    }

    /**
     * 取消所有req
     * @throws Exception
     */
    private void cleanupRequests() throws Exception {
        List<ChannelStateWriteRequest> drained;
        Throwable cause;
        synchronized (lock) {
            cause = thrown == null ? new CancellationException() : thrown;
            drained = new ArrayList<>(deque);
            deque.clear();
            for (Queue<ChannelStateWriteRequest> unreadyQueue : unreadyQueues.values()) {
                while (!unreadyQueue.isEmpty()) {
                    drained.add(unreadyQueue.poll());
                }
            }
        }
        LOG.info("discarding {} drained requests", drained.size());
        closeAll(
                drained.stream()
                        .<AutoCloseable>map(request -> () -> request.cancel(cause))
                        .collect(Collectors.toList()));
    }

    @Override
    public void start() throws IllegalStateException {
        this.thread.start();
    }

    /**
     *
     * @param request
     * @throws Exception
     */
    @Override
    public void submit(ChannelStateWriteRequest request) throws Exception {
        synchronized (lock) {
            Queue<ChannelStateWriteRequest> unreadyQueue =
                    unreadyQueues.get(
                            SubtaskID.of(request.getJobVertexID(), request.getSubtaskIndex()));
            checkArgument(unreadyQueue != null, "The subtask %s is not yet registered.");
            submitInternal(
                    request,
                    // 这个是处理逻辑
                    () -> {
                        // 1. unreadyQueue isn't empty, the new request must keep the order, so add
                        // the new request to the unreadyQueue tail.
                        // 需要按照顺序发送  如果前面有未准备好的req 就加入到等待队列
                        if (!unreadyQueue.isEmpty()) {
                            unreadyQueue.add(request);
                            return;
                        }
                        // 2. unreadyQueue is empty, and new request is ready, so add it to the
                        // readyQueue directly.
                        // unreadyQueue为空 并且该请求可以直接发送  就直接写入队列
                        if (request.getReadyFuture().isDone()) {
                            deque.add(request);
                            lock.notifyAll();  // 有req了 唤醒阻塞线程
                            return;
                        }
                        // 3. unreadyQueue is empty, and new request isn't ready, so add it to the
                        // unreadyQueue, and register it as the first request.
                        // 当前请求还没准备好  加入unreadyQueue
                        unreadyQueue.add(request);
                        registerFirstRequestFuture(request, unreadyQueue);
                    });
        }
    }

    /**
     * 表示收到第一个加入该队列的req   在req准备好后 要自动将队列移动到 deque中
     * @param firstRequest
     * @param unreadyQueue
     */
    private void registerFirstRequestFuture(
            @Nonnull ChannelStateWriteRequest firstRequest,
            Queue<ChannelStateWriteRequest> unreadyQueue) {
        assert Thread.holdsLock(lock);
        checkState(firstRequest == unreadyQueue.peek(), "The request isn't the first request.");

        firstRequest
                .getReadyFuture()
                .thenAccept(
                        o -> {
                            synchronized (lock) {
                                moveReadyRequestToReadyQueue(unreadyQueue, firstRequest);
                            }
                        })
                .exceptionally(
                        throwable -> {
                            // When dataFuture is completed, just move the request to readyQueue.
                            // And the throwable doesn't need to be handled here, it will be handled
                            // in channel state writer thread later.
                            synchronized (lock) {
                                moveReadyRequestToReadyQueue(unreadyQueue, firstRequest);
                            }
                            return null;
                        });
    }

    private void moveReadyRequestToReadyQueue(
            Queue<ChannelStateWriteRequest> unreadyQueue, ChannelStateWriteRequest firstRequest) {
        assert Thread.holdsLock(lock);
        checkState(firstRequest == unreadyQueue.peek());
        while (!unreadyQueue.isEmpty()) {
            ChannelStateWriteRequest req = unreadyQueue.peek();
            if (!req.getReadyFuture().isDone()) {
                registerFirstRequestFuture(req, unreadyQueue);
                return;
            }
            deque.add(Objects.requireNonNull(unreadyQueue.poll()));
            lock.notifyAll();
        }
    }

    /**
     * 发送高优先级请求
     * @param request
     * @throws Exception
     */
    @Override
    public void submitPriority(ChannelStateWriteRequest request) throws Exception {
        synchronized (lock) {
            checkArgument(
                    unreadyQueues.containsKey(
                            SubtaskID.of(request.getJobVertexID(), request.getSubtaskIndex())),
                    "The subtask %s is not yet registered.");
            checkState(request.getReadyFuture().isDone(), "The priority request must be ready.");
            submitInternal(
                    request,
                    () -> {
                        // 注意直接加入到队首
                        deque.addFirst(request);
                        lock.notifyAll();
                    });
        }
    }

    private void submitInternal(ChannelStateWriteRequest request, RunnableWithException action)
            throws Exception {
        try {
            action.run();
        } catch (Exception ex) {
            request.cancel(ex);
            throw ex;
        }
        ensureRunning();
    }

    private void ensureRunning() throws Exception {
        assert Thread.holdsLock(lock);
        // this check should be performed *at least after* enqueuing a request
        // checking before is not enough because (check + enqueue) is not atomic
        if (wasClosed || !thread.isAlive()) {
            cleanupRequests();
            IllegalStateException exception = new IllegalStateException("not running");
            if (thrown != null) {
                exception.addSuppressed(thrown);
            }
            throw exception;
        }
    }

    /**
     * 一开始subtasks为空 需要注册才能知道参与本次检查点的子任务
     * @param jobVertexID
     * @param subtaskIndex
     */
    @Override
    public void registerSubtask(JobVertexID jobVertexID, int subtaskIndex) {
        assert Thread.holdsLock(registerLock);

        SubtaskID subtaskID = SubtaskID.of(jobVertexID, subtaskIndex);
        synchronized (lock) {
            checkState(isRegistering(), "This executor has been registered.");
            checkState(
                    !subtasks.contains(subtaskID),
                    String.format("This subtask[%s] has already registered.", subtaskID));
            subtasks.add(subtaskID);
            deque.add(
                    ChannelStateWriteRequest.registerSubtask(
                            subtaskID.getJobVertexID(), subtaskID.getSubtaskIndex()));
            lock.notifyAll();
            unreadyQueues.put(subtaskID, new ArrayDeque<>());
            // 注册的子任务达到最大值  强制启动(或者理解为结束准备阶段)
            if (subtasks.size() == maxSubtasksPerChannelStateFile && completeRegister()) {
                onRegistered.accept(this);
            }
        }
    }

    @VisibleForTesting
    public boolean isRegistering() {
        synchronized (registerLock) {
            return isRegistering;
        }
    }

    private boolean completeRegister() {
        assert Thread.holdsLock(registerLock);
        if (isRegistering) {
            isRegistering = false;
            return true;
        }
        return false;
    }

    @Override
    public void releaseSubtask(JobVertexID jobVertexID, int subtaskIndex) throws IOException {
        synchronized (registerLock) {
            synchronized (lock) {
                if (completeRegister()) {
                    onRegistered.accept(this);
                }
                subtasks.remove(SubtaskID.of(jobVertexID, subtaskIndex));
                if (!subtasks.isEmpty()) {
                    return;
                }
                // 当所有注册的子任务都被释放时  本对象被关闭
                wasClosed = true;
                lock.notifyAll();
            }
        }
        while (thread.isAlive()) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                if (!thread.isAlive()) {
                    Thread.currentThread().interrupt();
                }
                LOG.debug(
                        "Channel state executor is interrupted while waiting for the writer thread to die",
                        e);
            }
        }
        synchronized (lock) {
            if (thrown != null) {
                throw new IOException(thrown);
            }
        }
    }

    @VisibleForTesting
    Thread getThread() {
        return thread;
    }
}
