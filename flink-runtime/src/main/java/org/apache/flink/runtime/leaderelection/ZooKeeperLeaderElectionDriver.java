/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperLeaderElectionHaServices;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.TreeCacheSelector;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.ConnectionState;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.ConnectionStateListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/** ZooKeeper based {@link LeaderElectionDriver} implementation.
 * 作为驱动 与 zk通信以获得选举能力
 * */
public class ZooKeeperLeaderElectionDriver implements LeaderElectionDriver, LeaderLatchListener {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderElectionDriver.class);

    /**
     * zk客户端
     */
    private final CuratorFramework curatorFramework;

    /**
     * 监听选举关系的变化
     */
    private final LeaderElectionDriver.Listener leaderElectionListener;

    private final String leaderLatchPath;

    /**
     * 这个是封装好的选举对象
     */
    private final LeaderLatch leaderLatch;

    private final TreeCache treeCache;

    private final ConnectionStateListener listener =
            (client, newState) -> handleStateChange(newState);

    private AtomicBoolean running = new AtomicBoolean(true);

    public ZooKeeperLeaderElectionDriver(
            CuratorFramework curatorFramework, LeaderElectionDriver.Listener leaderElectionListener)
            throws Exception {
        this.curatorFramework = Preconditions.checkNotNull(curatorFramework);
        this.leaderElectionListener = Preconditions.checkNotNull(leaderElectionListener);

        // 创建维护选举信息相关的路径
        this.leaderLatchPath =
                ZooKeeperUtils.generateLeaderLatchPath(curatorFramework.getNamespace());

        // 封装选举对象
        this.leaderLatch = new LeaderLatch(curatorFramework, ZooKeeperUtils.getLeaderLatchPath());
        this.treeCache =
                ZooKeeperUtils.createTreeCache(
                        curatorFramework,
                        "/",
                        new ZooKeeperLeaderElectionDriver.ConnectionInfoNodeSelector());

        treeCache
                .getListenable()
                .addListener(
                        (client, event) -> {
                            switch (event.getType()) {
                                case NODE_ADDED:
                                    // 监听路径下数据的变化或者删除
                                case NODE_UPDATED:
                                    Preconditions.checkNotNull(
                                            event.getData(),
                                            "The ZooKeeper event data must not be null.");
                                    handleChangedLeaderInformation(event.getData());
                                    break;
                                case NODE_REMOVED:
                                    Preconditions.checkNotNull(
                                            event.getData(),
                                            "The ZooKeeper event data must not be null.");
                                    handleRemovedLeaderInformation(event.getData().getPath());
                                    break;
                            }
                        });

        leaderLatch.addListener(this);
        curatorFramework.getConnectionStateListenable().addListener(listener);
        leaderLatch.start();
        treeCache.start();
    }

    @Override
    public void close() throws Exception {
        if (running.compareAndSet(true, false)) {
            LOG.info("Closing {}.", this);

            curatorFramework.getConnectionStateListenable().removeListener(listener);

            Exception exception = null;

            try {
                treeCache.close();
            } catch (Exception e) {
                exception = e;
            }

            try {
                leaderLatch.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            ExceptionUtils.tryRethrowException(exception);
        }
    }

    @Override
    public boolean hasLeadership() {
        return leaderLatch.hasLeadership();
    }

    /**
     * 作为领导可以推送信息  其实该节点相关的所有组件都自动变成领导了
     * @param componentId identifying the component for which to publish the leader information
     * @param leaderInformation leader information of the respective component
     */
    @Override
    public void publishLeaderInformation(String componentId, LeaderInformation leaderInformation) {
        Preconditions.checkState(running.get());

        // 此时不是leader 无法发布消息
        if (!leaderLatch.hasLeadership()) {
            return;
        }

        final String connectionInformationPath =
                ZooKeeperUtils.generateConnectionInformationPath(componentId);

        LOG.debug(
                "Write leader information {} for component '{}' to {}.",
                leaderInformation,
                componentId,
                ZooKeeperUtils.generateZookeeperPath(
                        curatorFramework.getNamespace(), connectionInformationPath));

        try {
            // 将信息写入指定路径
            ZooKeeperUtils.writeLeaderInformationToZooKeeper(
                    leaderInformation,
                    curatorFramework,
                    leaderLatch::hasLeadership,
                    connectionInformationPath);
        } catch (Exception e) {
            leaderElectionListener.onError(e);
        }
    }

    /**
     * 作为leader 删除发布的消息
     * @param componentId identifying the component for which to delete the leader information
     */
    @Override
    public void deleteLeaderInformation(String componentId) {
        try {
            ZooKeeperUtils.deleteZNode(
                    curatorFramework, ZooKeeperUtils.generateZookeeperPath(componentId));
        } catch (Exception e) {
            leaderElectionListener.onError(e);
        }
    }

    private void handleStateChange(ConnectionState newState) {
        switch (newState) {
            case CONNECTED:
                LOG.debug("Connected to ZooKeeper quorum. Leader election can start.");
                break;
            case SUSPENDED:
                LOG.warn("Connection to ZooKeeper suspended, waiting for reconnection.");
                break;
            case RECONNECTED:
                LOG.info(
                        "Connection to ZooKeeper was reconnected. Leader election can be restarted.");
                break;
            case LOST:
                // Maybe we have to throw an exception here to terminate the JobManager
                LOG.warn(
                        "Connection to ZooKeeper lost. None of the contenders participates in the leader election anymore.");
                break;
        }
    }

    @Override
    public void isLeader() {
        final UUID leaderSessionID = UUID.randomUUID();
        LOG.debug("{} obtained the leadership with session ID {}.", this, leaderSessionID);
        // 表示此时成为leader
        leaderElectionListener.onGrantLeadership(leaderSessionID);
    }

    @Override
    public void notLeader() {
        LOG.debug("{} lost the leadership.", this);
        // 此时不再是leader
        leaderElectionListener.onRevokeLeadership();
    }

    private void handleChangedLeaderInformation(ChildData childData) {
        // 当leader写入的信息发生变化
        if (shouldHandleLeaderInformationEvent(childData.getPath())) {
            // 获取此时leader对应的组件id
            final String componentId = extractComponentId(childData.getPath());

            final LeaderInformation leaderInformation =
                    tryReadingLeaderInformation(childData, componentId);

            leaderElectionListener.onLeaderInformationChange(componentId, leaderInformation);
        }
    }

    private String extractComponentId(String path) {
        final String[] splits = ZooKeeperUtils.splitZooKeeperPath(path);

        Preconditions.checkState(
                splits.length >= 2,
                String.format(
                        "Expecting path consisting of /<component-id>/connection_info. Got path '%s'",
                        path));

        return splits[splits.length - 2];
    }

    private void handleRemovedLeaderInformation(String removedNodePath) {
        if (shouldHandleLeaderInformationEvent(removedNodePath)) {
            final String leaderName = extractComponentId(removedNodePath);

            // 表示leader上的信息变化
            leaderElectionListener.onLeaderInformationChange(leaderName, LeaderInformation.empty());
        }
    }

    private boolean shouldHandleLeaderInformationEvent(String path) {
        return running.get()
                && leaderLatch.hasLeadership()
                && ZooKeeperUtils.isConnectionInfoPath(path);
    }

    private LeaderInformation tryReadingLeaderInformation(ChildData childData, String id) {
        LeaderInformation leaderInformation;
        try {
            leaderInformation = ZooKeeperUtils.readLeaderInformation(childData.getData());

            LOG.debug("Leader information for {} has changed to {}.", id, leaderInformation);
        } catch (IOException | ClassNotFoundException e) {
            LOG.debug(
                    "Could not read leader information for {}. Rewriting the information.", id, e);
            leaderInformation = LeaderInformation.empty();
        }

        return leaderInformation;
    }

    /**
     * This selector finds all connection info nodes. See {@link ZooKeeperLeaderElectionHaServices}
     * for more details on the Znode layout.
     */
    private static class ConnectionInfoNodeSelector implements TreeCacheSelector {
        @Override
        public boolean traverseChildren(String fullPath) {
            return true;
        }

        @Override
        public boolean acceptChild(String fullPath) {
            return !fullPath.endsWith(ZooKeeperUtils.getLeaderLatchPath());
        }
    }

    @Override
    public String toString() {
        return String.format(
                "%s{leaderLatchPath='%s'}", getClass().getSimpleName(), leaderLatchPath);
    }
}
