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

package org.apache.flink.core.fs.local;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream.Committer;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link RecoverableWriter} for the {@link LocalFileSystem}.
 * 基于本地文件系统的可恢复写入对象
 * */
@Internal
public class LocalRecoverableWriter implements RecoverableWriter {

    private final LocalFileSystem fs;

    public LocalRecoverableWriter(LocalFileSystem fs) {
        this.fs = checkNotNull(fs);
    }

    /**
     * RecoverableFsDataOutputStream 相比普通的输出流 多开放了一些api
     * @param filePath
     * @return
     * @throws IOException
     */
    @Override
    public RecoverableFsDataOutputStream open(Path filePath) throws IOException {
        final File targetFile = fs.pathToFile(filePath);
        // 产生一个临时文件
        final File tempFile = generateStagingTempFilePath(targetFile);

        // try to create the parent
        final File parent = tempFile.getParentFile();
        if (parent != null && !parent.mkdirs() && !parent.exists()) {
            throw new IOException("Failed to create the parent directory: " + parent);
        }

        // 该对象有持久化和恢复  同时在提交时 会将临时文件变成目标文件
        return new LocalRecoverableFsDataOutputStream(targetFile, tempFile);
    }

    /**
     * 通过记录相关信息的恢复对象 重新生成输出流
     * @param recoverable
     * @return
     * @throws IOException
     */
    @Override
    public RecoverableFsDataOutputStream recover(ResumeRecoverable recoverable) throws IOException {
        if (recoverable instanceof LocalRecoverable) {
            return new LocalRecoverableFsDataOutputStream((LocalRecoverable) recoverable);
        } else {
            throw new IllegalArgumentException(
                    "LocalFileSystem cannot recover recoverable for other file system: "
                            + recoverable);
        }
    }

    @Override
    public boolean requiresCleanupOfRecoverableState() {
        return false;
    }

    @Override
    public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
        return false;
    }

    /**
     * 生成提交对象
     * @param recoverable
     * @return
     * @throws IOException
     */
    @Override
    public Committer recoverForCommit(CommitRecoverable recoverable) throws IOException {
        if (recoverable instanceof LocalRecoverable) {
            return new LocalRecoverableFsDataOutputStream.LocalCommitter(
                    (LocalRecoverable) recoverable);
        } else {
            throw new IllegalArgumentException(
                    "LocalFileSystem cannot recover recoverable for other file system: "
                            + recoverable);
        }
    }

    // commit对象和 resume对象 也需要支持序列化

    @Override
    public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
        @SuppressWarnings("unchecked")
        SimpleVersionedSerializer<CommitRecoverable> typedSerializer =
                (SimpleVersionedSerializer<CommitRecoverable>)
                        (SimpleVersionedSerializer<?>) LocalRecoverableSerializer.INSTANCE;

        return typedSerializer;
    }

    @Override
    public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
        @SuppressWarnings("unchecked")
        SimpleVersionedSerializer<ResumeRecoverable> typedSerializer =
                (SimpleVersionedSerializer<ResumeRecoverable>)
                        (SimpleVersionedSerializer<?>) LocalRecoverableSerializer.INSTANCE;

        return typedSerializer;
    }

    @Override
    public boolean supportsResume() {
        return true;
    }

    /**
     * 生成临时文件
     * @param targetFile
     * @return
     */
    @VisibleForTesting
    public static File generateStagingTempFilePath(File targetFile) {
        checkArgument(!targetFile.isDirectory(), "targetFile must not be a directory");

        final File parent = targetFile.getParentFile();
        final String name = targetFile.getName();

        checkArgument(parent != null, "targetFile must not be the root directory");

        while (true) {
            File candidate =
                    new File(parent, "." + name + ".inprogress." + UUID.randomUUID().toString());
            if (!candidate.exists()) {
                return candidate;
            }
        }
    }
}
