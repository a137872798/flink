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

package org.apache.flink.runtime.blocklist;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link BlocklistTracker}.
 * 默认的 阻塞节点列表追踪对象
 * */
public class DefaultBlocklistTracker implements BlocklistTracker {

    /**
     * 维护所有阻塞节点
     */
    private final Map<String, BlockedNode> blockedNodes = new HashMap<>();

    /**
     * Try to add a new blocked node record. If the node (identified by node id) already exists, the
     * newly added one will be merged with the existing one.
     *
     * @param newNode the new blocked node record
     * @return the add status
     * 将节点插入map  已存在则尝试合并
     */
    private AddStatus tryAddOrMerge(BlockedNode newNode) {
        checkNotNull(newNode);
        final String nodeId = newNode.getNodeId();
        final BlockedNode existingNode = blockedNodes.get(nodeId);

        if (existingNode == null) {
            blockedNodes.put(nodeId, newNode);
            return AddStatus.ADDED;
        } else {
            // 就是更新阻塞时间戳
            BlockedNode merged =
                    newNode.getEndTimestamp() >= existingNode.getEndTimestamp()
                            ? newNode
                            : existingNode;
            if (!merged.equals(existingNode)) {
                blockedNodes.put(nodeId, merged);
                return AddStatus.MERGED;
            }
            return AddStatus.NONE;
        }
    }

    /**
     * 要增加一组阻塞节点
     * @param newNodes the new blocked node records
     * @return
     */
    @Override
    public BlockedNodeAdditionResult addNewBlockedNodes(Collection<BlockedNode> newNodes) {
        checkNotNull(newNodes);

        final Map<String, BlockedNode> newlyAddedNodes = new HashMap<>();
        final Map<String, BlockedNode> mergedNodes = new HashMap<>();
        for (BlockedNode node : newNodes) {
            String nodeId = node.getNodeId();
            AddStatus status = tryAddOrMerge(node);
            switch (status) {
                case ADDED:
                    newlyAddedNodes.put(nodeId, blockedNodes.get(nodeId));
                    break;
                case MERGED:
                    mergedNodes.put(nodeId, blockedNodes.get(nodeId));
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalStateException(
                            "Add or merge status " + status + " is not supported.");
            }
        }
        return new BlockedNodeAdditionResult(newlyAddedNodes.values(), mergedNodes.values());
    }

    @Override
    public boolean isBlockedNode(String nodeId) {
        checkNotNull(nodeId);
        return blockedNodes.containsKey(nodeId);
    }

    @Override
    public Set<String> getAllBlockedNodeIds() {
        return Collections.unmodifiableSet(blockedNodes.keySet());
    }

    @Override
    public Collection<BlockedNode> getAllBlockedNodes() {
        return Collections.unmodifiableCollection(blockedNodes.values());
    }

    @Override
    public Collection<BlockedNode> removeTimeoutNodes(long currentTimestamp) {
        Collection<BlockedNode> removedNodes = new ArrayList<>();
        final Iterator<BlockedNode> blockedNodeIterator = blockedNodes.values().iterator();
        while (blockedNodeIterator.hasNext()) {
            BlockedNode blockedNode = blockedNodeIterator.next();
            if (currentTimestamp >= blockedNode.getEndTimestamp()) {
                removedNodes.add(blockedNode);
                blockedNodeIterator.remove();
            }
        }
        return removedNodes;
    }

    private enum AddStatus {
        ADDED,
        MERGED,
        NONE
    }
}
