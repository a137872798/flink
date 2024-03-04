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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.guava31.com.google.common.io.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Blob store backed by {@link FileSystem}.
 *
 * <p>This is used in addition to the local blob storage for high availability.
 *
 * 目前看来BlobStoreService 只有这一个实现
 */
public class FileSystemBlobStore implements BlobStoreService {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemBlobStore.class);

    /** The file system in which blobs are stored.
     * 不过使用的文件系统可以是本地的 也可以是分布式的
     * */
    private final FileSystem fileSystem;

    /**
     * 表示基础路径是否已经创建
     */
    private volatile boolean basePathCreated;

    /** The base path of the blob store. */
    private final String basePath;

    /** The name of the blob path. */
    public static final String BLOB_PATH_NAME = "blob";

    /**
     * 指定文件系统 并且指定存储路径 就生成了store对象了
     * @param fileSystem
     * @param storagePath
     * @throws IOException
     */
    public FileSystemBlobStore(FileSystem fileSystem, String storagePath) throws IOException {
        this.fileSystem = checkNotNull(fileSystem);
        this.basePathCreated = false;
        this.basePath = checkNotNull(storagePath) + "/" + BLOB_PATH_NAME;
    }

    private void createBasePathIfNeeded() throws IOException {
        if (!basePathCreated) {
            LOG.info("Creating highly available BLOB storage directory at {}", basePath);
            fileSystem.mkdirs(new Path(basePath));
            LOG.debug("Created highly available BLOB storage directory at {}", basePath);
            basePathCreated = true;
        }
    }

    // - Put ------------------------------------------------------------------

    @Override
    public boolean put(File localFile, JobID jobId, BlobKey blobKey) throws IOException {
        // 先确保基础目录已经创建
        createBasePathIfNeeded();
        // 生成路径
        String toBlobPath = BlobUtils.getStorageLocationPath(basePath, jobId, blobKey);

        // 创建文件系统专属文件流   拷贝本地文件  并写入到文件系统
        try (FSDataOutputStream os =
                fileSystem.create(new Path(toBlobPath), FileSystem.WriteMode.OVERWRITE)) {
            LOG.debug("Copying from {} to {}.", localFile, toBlobPath);
            Files.copy(localFile, os);

            os.sync();
        }
        return true;
    }

    // - Get ------------------------------------------------------------------

    @Override
    public boolean get(JobID jobId, BlobKey blobKey, File localFile) throws IOException {
        return get(BlobUtils.getStorageLocationPath(basePath, jobId, blobKey), localFile, blobKey);
    }

    /**
     * 从仓库读取数据 并将数据拷贝到 本地文件
     * @param fromBlobPath
     * @param toFile
     * @param blobKey
     * @return
     * @throws IOException
     */
    private boolean get(String fromBlobPath, File toFile, BlobKey blobKey) throws IOException {
        checkNotNull(fromBlobPath, "Blob path");
        checkNotNull(toFile, "File");
        checkNotNull(blobKey, "Blob key");

        // 创建目标文件
        if (!toFile.exists() && !toFile.createNewFile()) {
            throw new IOException("Failed to create target file to copy to");
        }

        final Path fromPath = new Path(fromBlobPath);

        // 产生一个类似标签的东西
        MessageDigest md = BlobUtils.createMessageDigest();

        final int buffSize = 4096; // like IOUtils#BLOCKSIZE, for chunked file copying

        boolean success = false;
        // 打开文件系统的输入流
        try (InputStream is = fileSystem.open(fromPath);
                FileOutputStream fos = new FileOutputStream(toFile)) {
            LOG.debug("Copying from {} to {}.", fromBlobPath, toFile);

            // not using IOUtils.copyBytes(is, fos) here to be able to create a hash on-the-fly
            final byte[] buf = new byte[buffSize];
            int bytesRead = is.read(buf);
            // 将所有数据拷贝到本地文件中
            while (bytesRead >= 0) {
                fos.write(buf, 0, bytesRead);
                md.update(buf, 0, bytesRead);

                bytesRead = is.read(buf);
            }

            // verify that file contents are correct
            // 生成标签 校验与hash是否一致 来判断文件是否损坏
            final byte[] computedKey = md.digest();
            if (!Arrays.equals(computedKey, blobKey.getHash())) {
                throw new IOException("Detected data corruption during transfer");
            }

            success = true;
        } finally {
            // if the copy fails, we need to remove the target file because
            // outside code relies on a correct file as long as it exists
            if (!success) {
                try {
                    toFile.delete();
                } catch (Throwable ignored) {
                }
            }
        }

        return true; // success is always true here
    }

    // - Delete ---------------------------------------------------------------

    @Override
    public boolean delete(JobID jobId, BlobKey blobKey) {
        return delete(BlobUtils.getStorageLocationPath(basePath, jobId, blobKey));
    }

    @Override
    public boolean deleteAll(JobID jobId) {
        return delete(BlobUtils.getStorageLocationPath(basePath, jobId));
    }

    /**
     * 借助文件系统 删除某个文件/目录
     * @param blobPath
     * @return
     */
    private boolean delete(String blobPath) {
        try {
            LOG.debug("Deleting {}.", blobPath);

            Path path = new Path(blobPath);

            boolean result = true;
            if (fileSystem.exists(path)) {
                result = fileSystem.delete(path, true);
            } else {
                LOG.debug(
                        "The given path {} is not present anymore. No deletion is required.", path);
            }

            // send a call to delete the directory containing the file. This will
            // fail (and be ignored) when some files still exist.
            // 尝试性删除 如果还有文件 会删除失败的
            try {
                fileSystem.delete(path.getParent(), false);
                fileSystem.delete(new Path(basePath), false);
            } catch (IOException ignored) {
            }
            return result;
        } catch (Exception e) {
            LOG.warn("Failed to delete blob at " + blobPath);
            return false;
        }
    }

    @Override
    public void closeAndCleanupAllData() {
        try {
            LOG.debug("Cleaning up {}.", basePath);

            fileSystem.delete(new Path(basePath), true);
        } catch (Exception e) {
            LOG.error("Failed to clean up recovery directory.", e);
        }
    }

    @Override
    public void close() throws IOException {
        // nothing to do for the FileSystemBlobStore
    }
}
