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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.LocatableInputSplit;
import org.apache.flink.util.NetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The locatable input split assigner assigns to each host splits that are local, before assigning
 * splits that are not local.
 * 该对象管理一组 split
 */
@Public
public final class LocatableInputSplitAssigner implements InputSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(LocatableInputSplitAssigner.class);

    // unassigned input splits   维护还未分配的split
    private final Set<LocatableInputSplitWithCount> unassigned =
            new HashSet<LocatableInputSplitWithCount>();

    // input splits indexed by host for local assignment
    private final ConcurrentHashMap<String, LocatableInputSplitChooser> localPerHost =
            new ConcurrentHashMap<String, LocatableInputSplitChooser>();

    // unassigned splits for remote assignment
    private final LocatableInputSplitChooser remoteSplitChooser;

    private int localAssignments; // lock protected by the unassigned set lock

    private int remoteAssignments; // lock protected by the unassigned set lock

    // --------------------------------------------------------------------------------------------

    /**
     * 本对象使用一组split进行初始化
     * @param splits
     */
    public LocatableInputSplitAssigner(Collection<LocatableInputSplit> splits) {
        for (LocatableInputSplit split : splits) {
            this.unassigned.add(new LocatableInputSplitWithCount(split));
        }
        // 通过chooser可以得到远端 localCount最小的split
        this.remoteSplitChooser = new LocatableInputSplitChooser(unassigned);
    }

    public LocatableInputSplitAssigner(LocatableInputSplit[] splits) {
        for (LocatableInputSplit split : splits) {
            this.unassigned.add(new LocatableInputSplitWithCount(split));
        }
        this.remoteSplitChooser = new LocatableInputSplitChooser(unassigned);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 获取下个要被使用的 split
     * @param host The host address of split requesting task.
     * @param taskId The id of the split requesting task.
     * @return
     */
    @Override
    public LocatableInputSplit getNextInputSplit(String host, int taskId) {

        // for a null host, we return a remote split
        if (host == null) {
            synchronized (this.remoteSplitChooser) {
                synchronized (this.unassigned) {
                    LocatableInputSplitWithCount split =
                            this.remoteSplitChooser.getNextUnassignedMinLocalCountSplit(
                                    this.unassigned);

                    // 找到了下个要使用的split 它的localCount是最小的
                    if (split != null) {
                        // got a split to assign. Double check that it hasn't been assigned before.
                        if (this.unassigned.remove(split)) {
                            if (LOG.isInfoEnabled()) {
                                LOG.info("Assigning split to null host (random assignment).");
                            }

                            remoteAssignments++;
                            return split.getSplit();
                        } else {
                            throw new IllegalStateException(
                                    "Chosen InputSplit has already been assigned. This should not happen!");
                        }
                    } else {
                        // all splits consumed  在未指定host的情况下 就是返回这样的split 没有就是null
                        return null;
                    }
                }
            }
        }

        host = host.toLowerCase(Locale.US);

        // for any non-null host, we take the list of non-null splits
        LocatableInputSplitChooser localSplits = this.localPerHost.get(host);

        // if we have no list for this host yet, create one
        if (localSplits == null) {
            localSplits = new LocatableInputSplitChooser();

            // lock the list, to be sure that others have to wait for that host's local list
            synchronized (localSplits) {
                LocatableInputSplitChooser prior = this.localPerHost.putIfAbsent(host, localSplits);

                // if someone else beat us in the case to create this list, then we do not populate
                // this one, but
                // simply work with that other list
                if (prior == null) {
                    // we are the first, we populate

                    // first, copy the remaining splits to release the lock on the set early
                    // because that is shared among threads
                    LocatableInputSplitWithCount[] remaining;
                    synchronized (this.unassigned) {
                        remaining =
                                this.unassigned.toArray(
                                        new LocatableInputSplitWithCount[this.unassigned.size()]);
                    }

                    for (LocatableInputSplitWithCount isw : remaining) {
                        // 发现是本地的  加入到LocatableInputSplitChooser中进行排序
                        if (isLocal(host, isw.getSplit().getHostnames())) {
                            // Split is local on host.
                            // Increment local count
                            isw.incrementLocalCount();
                            // and add to local split list
                            localSplits.addInputSplit(isw);
                        }
                    }

                } else {
                    // someone else was faster
                    localSplits = prior;
                }
            }
        }

        // at this point, we have a list of local splits (possibly empty)
        // we need to make sure no one else operates in the current list (that protects against
        // list creation races) and that the unassigned set is consistent
        // NOTE: we need to obtain the locks in this order, strictly!!!
        // 找到host相关的split对象 并选择localCount最小的split
        synchronized (localSplits) {
            synchronized (this.unassigned) {
                LocatableInputSplitWithCount split =
                        localSplits.getNextUnassignedMinLocalCountSplit(this.unassigned);

                if (split != null) {
                    // found a valid split. Double check that it hasn't been assigned before.
                    if (this.unassigned.remove(split)) {
                        if (LOG.isInfoEnabled()) {
                            LOG.info("Assigning local split to host " + host);
                        }

                        localAssignments++;
                        return split.getSplit();
                    } else {
                        throw new IllegalStateException(
                                "Chosen InputSplit has already been assigned. This should not happen!");
                    }
                }
            }
        }

        // we did not find a local split, return a remote split
        // 本地未找到的情况下 从remote获取
        synchronized (this.remoteSplitChooser) {
            synchronized (this.unassigned) {
                LocatableInputSplitWithCount split =
                        this.remoteSplitChooser.getNextUnassignedMinLocalCountSplit(
                                this.unassigned);

                if (split != null) {
                    // found a valid split. Double check that it hasn't been assigned yet.
                    if (this.unassigned.remove(split)) {
                        if (LOG.isInfoEnabled()) {
                            LOG.info("Assigning remote split to host " + host);
                        }

                        remoteAssignments++;
                        return split.getSplit();
                    } else {
                        throw new IllegalStateException(
                                "Chosen InputSplit has already been assigned. This should not happen!");
                    }
                } else {
                    // all splits consumed
                    return null;
                }
            }
        }
    }

    /**
     * 将split重新加入到unassigned中
     * @param splits The list of input splits to be returned.
     * @param taskId The id of the task that failed to process the input splits.
     */
    @Override
    public void returnInputSplit(List<InputSplit> splits, int taskId) {
        synchronized (this.unassigned) {
            for (InputSplit split : splits) {
                LocatableInputSplitWithCount lisw =
                        new LocatableInputSplitWithCount((LocatableInputSplit) split);
                this.remoteSplitChooser.addInputSplit(lisw);
                this.unassigned.add(lisw);
            }
        }
    }

    private static final boolean isLocal(String flinkHost, String[] hosts) {
        if (flinkHost == null || hosts == null) {
            return false;
        }
        for (String h : hosts) {
            if (h != null && NetUtils.getHostnameFromFQDN(h.toLowerCase()).equals(flinkHost)) {
                return true;
            }
        }

        return false;
    }

    public int getNumberOfLocalAssignments() {
        return localAssignments;
    }

    public int getNumberOfRemoteAssignments() {
        return remoteAssignments;
    }

    /**
     * Wraps a LocatableInputSplit and adds a count for the number of observed hosts that can access
     * the split locally.
     */
    private static class LocatableInputSplitWithCount {

        private final LocatableInputSplit split;
        private int localCount;

        public LocatableInputSplitWithCount(LocatableInputSplit split) {
            this.split = split;
            this.localCount = 0;
        }

        public void incrementLocalCount() {
            this.localCount++;
        }

        public int getLocalCount() {
            return this.localCount;
        }

        public LocatableInputSplit getSplit() {
            return this.split;
        }
    }

    /**
     * Holds a list of LocatableInputSplits and returns the split with the lowest local count. The
     * rational is that splits which are local on few hosts should be preferred over others which
     * have more degrees of freedom for local assignment.
     *
     * <p>Internally, the splits are stored in a linked list. Sorting the list is not a good
     * solution, as local counts are updated whenever a previously unseen host requests a split.
     * Instead, we track the minimum local count and iteratively look for splits with that minimum
     * count.
     */
    private static class LocatableInputSplitChooser {

        // list of input splits
        private final LinkedList<LocatableInputSplitWithCount> splits;

        // the current minimum local count. We look for splits with this local count.
        private int minLocalCount = -1;
        // the second smallest count observed so far.  倒数第二小的
        private int nextMinLocalCount = -1;
        // number of elements we need to inspect for the minimum local count.
        // 代表要检查几个元素
        private int elementCycleCount = 0;

        public LocatableInputSplitChooser() {
            this.splits = new LinkedList<LocatableInputSplitWithCount>();
        }

        /**
         * 该对象初始化时 使用的是一组空的LocatableInputSplitWithCount对象
         * @param splits
         */
        public LocatableInputSplitChooser(Collection<LocatableInputSplitWithCount> splits) {
            this.splits = new LinkedList<LocatableInputSplitWithCount>();
            for (LocatableInputSplitWithCount isw : splits) {
                this.addInputSplit(isw);
            }
        }

        /**
         * Adds a single input split
         *
         * @param split The input split to add
         */
        public void addInputSplit(LocatableInputSplitWithCount split) {
            // split此时的计数值
            int localCount = split.getLocalCount();

            // 首次添加 该值为-1
            if (minLocalCount == -1) {
                // first split to add
                this.minLocalCount = localCount;
                this.elementCycleCount = 1;
                this.splits.offerFirst(split);
            } else if (localCount < minLocalCount) {
                // split with new min local count
                this.nextMinLocalCount = this.minLocalCount;
                this.minLocalCount = localCount;
                // all other splits have more local host than this one
                this.elementCycleCount = 1;
                // 小的放在前面
                splits.offerFirst(split);
            } else if (localCount == minLocalCount) {
                this.elementCycleCount++;
                this.splits.offerFirst(split);
            } else {
                // 夹在1，2之间
                if (localCount < nextMinLocalCount) {
                    nextMinLocalCount = localCount;
                }
                splits.offerLast(split);
            }
        }

        /**
         * Retrieves a LocatableInputSplit with minimum local count. InputSplits which have already
         * been assigned (i.e., which are not contained in the provided set) are filtered out. The
         * returned input split is NOT removed from the provided set.
         *
         * @param unassignedSplits Set of unassigned input splits.
         * @return An input split with minimum local count or null if all splits have been assigned.
         * 获取下一个localCount最小的split  并且它还得是未分配的
         */
        public LocatableInputSplitWithCount getNextUnassignedMinLocalCountSplit(
                Set<LocatableInputSplitWithCount> unassignedSplits) {

            if (splits.size() == 0) {
                return null;
            }

            do {
                // 代表这几个localCount是一样的
                elementCycleCount--;
                // take first split of the list
                LocatableInputSplitWithCount split = splits.pollFirst();
                // 只考虑还未分配的
                if (unassignedSplits.contains(split)) {
                    int localCount = split.getLocalCount();
                    // still unassigned, check local count  表示此时该split应该要换位置了
                    if (localCount > minLocalCount) {
                        // re-insert at end of the list and continue to look for split with smaller
                        // local count
                        splits.offerLast(split);
                        // check and update second smallest local count
                        if (nextMinLocalCount == -1 || split.getLocalCount() < nextMinLocalCount) {
                            nextMinLocalCount = split.getLocalCount();
                        }
                        split = null;
                    }
                } else {
                    // split was already assigned
                    split = null;
                }
                if (elementCycleCount == 0) {
                    // one full cycle, but no split with min local count found
                    // update minLocalCnt and element cycle count for next pass over the splits
                    minLocalCount = nextMinLocalCount;
                    nextMinLocalCount = -1;
                    elementCycleCount = splits.size();
                }
                if (split != null) {
                    // found a split to assign
                    return split;
                }
            } while (elementCycleCount > 0);

            // no split left
            return null;
        }
    }
}
