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

/*
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 */

package org.apache.flink.core.fs;

import org.apache.flink.annotation.Public;

/**
 * Interface that represents the client side information for a file independent of the file system.
 * 提供一些文件的信息
 */
@Public
public interface FileStatus {

    /**
     * Return the length of this file.
     *
     * @return the length of this file
     * 获取文件长度
     */
    long getLen();

    /**
     * Get the block size of the file.
     *
     * @return the number of bytes
     * 数据块长度
     */
    long getBlockSize();

    /**
     * Get the replication factor of a file.
     *
     * @return the replication factor of a file.
     * 什么是复制因子?
     */
    short getReplication();

    /**
     * Get the modification time of the file.
     *
     * @return the modification time of file in milliseconds since January 1, 1970 UTC.
     * 获取文件修改时间
     */
    long getModificationTime();

    /**
     * Get the access time of the file.
     *
     * @return the access time of file in milliseconds since January 1, 1970 UTC.
     */
    long getAccessTime();

    /**
     * Checks if this object represents a directory.
     *
     * @return <code>true</code> if this is a directory, <code>false</code> otherwise
     * 是否是一个目录
     */
    boolean isDir();

    /**
     * Returns the corresponding Path to the FileStatus.
     *
     * @return the corresponding Path to the FileStatus
     * 获取对应的文件/目录 路径
     */
    Path getPath();
}
