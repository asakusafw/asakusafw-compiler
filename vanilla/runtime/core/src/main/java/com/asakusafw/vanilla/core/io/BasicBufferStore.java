/**
 * Copyright 2011-2017 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.vanilla.core.io;

import static java.nio.file.StandardOpenOption.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.EnumSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.vanilla.core.util.SystemProperty;

/**
 * A basic implementation of {@link BufferStore}.
 * @since 0.4.0
 * @version 0.4.1
 */
public class BasicBufferStore implements BufferStore, InterruptibleIo {

    static final Logger LOG = LoggerFactory.getLogger(BasicBufferStore.class);

    static final int DEFAULT_PARTITION = 0;

    private final AtomicInteger counter = new AtomicInteger();

    private final File directory;

    private final int division;

    /**
     * Creates a new instance.
     */
    public BasicBufferStore() {
        this(null, DEFAULT_PARTITION);
    }

    /**
     * Creates a new instance.
     * @param base the base directory
     */
    public BasicBufferStore(File base) {
        this(base, DEFAULT_PARTITION);
    }

    /**
     * Creates a new instance.
     * @param base the base directory
     * @param division the maximum number of files in each sub-directory, or {@code 0} to disabled
     * @since 0.4.1
     */
    public BasicBufferStore(File base, int division) {
        this.directory = new File(
                base != null ? base : SystemProperty.getTemporaryDirectory(),
                String.format("asakusa-%s.tmp", UUID.randomUUID()));
        this.division = division;
    }

    /**
     * Creates a new builder for {@link BasicBufferStore}.
     * @return the created builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the directory.
     * @return the directory
     */
    public File getDirectory() {
        return directory;
    }

    @Override
    public DataReader.Provider store(ByteBuffer buffer) throws IOException, InterruptedException {
        File file = prepare();
        try (WritableByteChannel channel = Files.newByteChannel(file.toPath(), EnumSet.of(WRITE, CREATE_NEW))) {
            channel.write(buffer);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("saving buffer: {}bytes -> {}", buffer.remaining(), file);
        }
        return new FileEntry(file);
    }

    private File prepare() throws IOException {
        int id = counter.getAndIncrement();
        File dir;
        if (division == 0) {
            dir = directory;
        } else {
            dir = new File(directory, String.valueOf(id / division));
        }
        if (dir.isDirectory() == false
                && dir.mkdirs() == false
                && dir.isDirectory() == false) {
            throw new IOException();
        }
        return new File(dir, String.format("%d.buf", id)); //$NON-NLS-1$
    }

    @Override
    public void close() {
        delete(directory);
    }

    private static boolean delete(File f) {
        boolean deleted = true;
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleted &= delete(child);
                }
            }
        }
        if (deleted && f.delete() == false && f.exists()) {
            LOG.warn("failed to delete a temporary file: {}", f);
            return false;
        }
        return deleted;
    }

    @Override
    public String toString() {
        return String.format("BufferStore(%s)", directory); //$NON-NLS-1$
    }

    /**
     * A builder for {@link BasicBufferStore}.
     * @since 0.4.1
     */
    public static final class Builder {

        private File directory;

        private int division = DEFAULT_PARTITION;

        /**
         * Sets the directory.
         * @param newValue the directory
         * @return this
         */
        public Builder withDirectory(File newValue) {
            this.directory = newValue;
            return this;
        }

        /**
         * Sets the partition.
         * @param newValue the partition
         * @return this
         */
        public Builder withDivision(int newValue) {
            this.division = newValue;
            return this;
        }

        /**
         * Builds a {@link BasicBufferStore}.
         * @return the created instance
         */
        public BasicBufferStore build() {
            return new BasicBufferStore(directory, division);
        }
    }

    private static final class FileEntry implements DataReader.Provider {

        private final File file;

        FileEntry(File file) {
            this.file = file;
        }

        @Override
        public DataReader open() throws IOException {
            return ByteChannelReader.open(file.toPath());
        }

        @Override
        public void close() throws IOException {
            if (file.delete() == false && file.exists()) {
                LOG.warn(MessageFormat.format(
                        "failed to delete a temporary file: {0}",
                        file));
            }
        }

        @Override
        public String toString() {
            return String.format("Entry(%s)", file); //$NON-NLS-1$
        }
    }
}
