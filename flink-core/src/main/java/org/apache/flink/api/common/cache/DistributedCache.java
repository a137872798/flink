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

package org.apache.flink.api.common.cache;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * DistributedCache provides static methods to write the registered cache files into job
 * configuration or decode them from job configuration. It also provides user access to the file
 * locally.
 *
 * 分布式缓存
 */
@Public
public class DistributedCache {

    /**
     * Meta info about an entry in {@link DistributedCache}.
     *
     * <p>Entries have different semantics for local directories depending on where we are in the
     * job-submission process. After registration through the API {@code filePath} denotes the
     * original directory. After the upload to the cluster (which includes zipping the directory),
     * {@code filePath} denotes the (server-side) copy of the zip.
     * 缓存数据作为一个文件存储在某个目录下
     */
    public static class DistributedCacheEntry implements Serializable {

        /**
         * 文件目录
         */
        public String filePath;
        /**
         * 是否可执行
         */
        public Boolean isExecutable;
        /**
         * 是否已压缩
         */
        public boolean isZipped;

        public byte[] blobKey;

        /** Client-side constructor used by the API for initial registration. */
        public DistributedCacheEntry(String filePath, Boolean isExecutable) {
            this(filePath, isExecutable, null);
        }

        /** Client-side constructor used during job-submission for zipped directory. */
        public DistributedCacheEntry(String filePath, boolean isExecutable, boolean isZipped) {
            this(filePath, isExecutable, null, isZipped);
        }

        /** Server-side constructor used during job-submission for zipped directories. */
        public DistributedCacheEntry(
                String filePath, Boolean isExecutable, byte[] blobKey, boolean isZipped) {
            this.filePath = filePath;
            this.isExecutable = isExecutable;
            this.blobKey = blobKey;
            this.isZipped = isZipped;
        }

        /** Server-side constructor used during job-submission for files. */
        public DistributedCacheEntry(String filePath, Boolean isExecutable, byte[] blobKey) {
            this(filePath, isExecutable, blobKey, false);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DistributedCacheEntry that = (DistributedCacheEntry) o;
            return isZipped == that.isZipped
                    && Objects.equals(filePath, that.filePath)
                    && Objects.equals(isExecutable, that.isExecutable)
                    && Arrays.equals(blobKey, that.blobKey);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(filePath, isExecutable, isZipped);
            result = 31 * result + Arrays.hashCode(blobKey);
            return result;
        }

        @Override
        public String toString() {
            return "DistributedCacheEntry{"
                    + "filePath='"
                    + filePath
                    + '\''
                    + ", isExecutable="
                    + isExecutable
                    + ", isZipped="
                    + isZipped
                    + ", blobKey="
                    + Arrays.toString(blobKey)
                    + '}';
        }
    }

    // ------------------------------------------------------------------------

    // 描述一组拷贝缓存数据的任务
    private final Map<String, Future<Path>> cacheCopyTasks;

    public DistributedCache(Map<String, Future<Path>> cacheCopyTasks) {
        this.cacheCopyTasks = cacheCopyTasks;
    }

    // ------------------------------------------------------------------------

    // 获取某个任务结果
    public File getFile(String name) {
        if (name == null) {
            throw new NullPointerException("name must not be null");
        }

        Future<Path> future = cacheCopyTasks.get(name);
        if (future == null) {
            throw new IllegalArgumentException(
                    "File with name '"
                            + name
                            + "' is not available."
                            + " Did you forget to register the file?");
        }

        try {
            // 阻塞得到结果
            final Path path = future.get();
            URI tmp = path.makeQualified(path.getFileSystem()).toUri();
            // 将路径包装成file对象
            return new File(tmp);
        } catch (ExecutionException e) {
            throw new RuntimeException("An error occurred while copying the file.", e.getCause());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error while getting the file registered under '"
                            + name
                            + "' from the distributed cache",
                    e);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities to read/write cache files from/to the configuration
    // ------------------------------------------------------------------------

    // 将缓存文件信息 写入config
    public static void writeFileInfoToConfig(
            String name, DistributedCacheEntry e, Configuration conf) {
        int num = conf.getInteger(CACHE_FILE_NUM, 0) + 1;
        // 更新配置   这个应该是类似文件编号
        conf.setInteger(CACHE_FILE_NUM, num);
        conf.setString(CACHE_FILE_NAME + num, name);
        conf.setString(CACHE_FILE_PATH + num, e.filePath);
        conf.setBoolean(CACHE_FILE_EXE + num, e.isExecutable || new File(e.filePath).canExecute());
        conf.setBoolean(CACHE_FILE_DIR + num, e.isZipped || new File(e.filePath).isDirectory());
        if (e.blobKey != null) {
            conf.setBytes(CACHE_FILE_BLOB_KEY + num, e.blobKey);
        }
    }

    /**
     * 从配置中
     * @param conf
     * @return
     */
    public static Set<Entry<String, DistributedCacheEntry>> readFileInfoFromConfig(
            Configuration conf) {
        int num = conf.getInteger(CACHE_FILE_NUM, 0);
        if (num == 0) {
            return Collections.emptySet();
        }

        Map<String, DistributedCacheEntry> cacheFiles =
                new HashMap<String, DistributedCacheEntry>();
        for (int i = 1; i <= num; i++) {
            // 获取各个编号对应的文件信息
            String name = conf.getString(CACHE_FILE_NAME + i, null);
            String filePath = conf.getString(CACHE_FILE_PATH + i, null);
            boolean isExecutable = conf.getBoolean(CACHE_FILE_EXE + i, false);
            boolean isDirectory = conf.getBoolean(CACHE_FILE_DIR + i, false);

            byte[] blobKey = conf.getBytes(CACHE_FILE_BLOB_KEY + i, null);
            cacheFiles.put(
                    name, new DistributedCacheEntry(filePath, isExecutable, blobKey, isDirectory));
        }
        return cacheFiles.entrySet();
    }

    /**
     * Parses a list of distributed cache entries encoded in a string. Can be used to parse a config
     * option described by {@link org.apache.flink.configuration.PipelineOptions#CACHED_FILES}.
     *
     * <p>See {@link org.apache.flink.configuration.PipelineOptions#CACHED_FILES} for the format.
     *
     * @param files List of string encoded distributed cache entries.
     */
    public static List<Tuple2<String, DistributedCacheEntry>> parseCachedFilesFromString(
            List<String> files) {
        return files.stream()
                .map(ConfigurationUtils::parseMap)
                .map(
                        m ->
                                Tuple2.of(
                                        m.get("name"),
                                        new DistributedCacheEntry(
                                                m.get("path"),
                                                Optional.ofNullable(m.get("executable"))
                                                        .map(Boolean::parseBoolean)
                                                        .orElse(false))))
                .collect(Collectors.toList());
    }

    private static final String CACHE_FILE_NUM = "DISTRIBUTED_CACHE_FILE_NUM";

    private static final String CACHE_FILE_NAME = "DISTRIBUTED_CACHE_FILE_NAME_";

    private static final String CACHE_FILE_PATH = "DISTRIBUTED_CACHE_FILE_PATH_";

    private static final String CACHE_FILE_EXE = "DISTRIBUTED_CACHE_FILE_EXE_";

    private static final String CACHE_FILE_DIR = "DISTRIBUTED_CACHE_FILE_DIR_";

    private static final String CACHE_FILE_BLOB_KEY = "DISTRIBUTED_CACHE_FILE_BLOB_KEY_";
}
