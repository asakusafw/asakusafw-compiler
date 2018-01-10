/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.packaging;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An implementation of {@link ResourceRepository} on a local ZIP archive.
 */
public class ZipRepository implements ResourceRepository {

    static final Logger LOG = LoggerFactory.getLogger(ZipRepository.class);

    private final File archive;

    /**
     * Creates a new instance.
     * @param archive the repository archive
     * @throws IOException if failed to open the target archive
     */
    public ZipRepository(File archive) throws IOException {
        if (archive.isFile() == false) {
            throw new IOException(MessageFormat.format(
                    "{0} must be a valid ZIP archive file",
                    archive));
        }
        this.archive = archive.getAbsoluteFile().getCanonicalFile();
    }

    @Override
    public Cursor createCursor() throws IOException {
        FileInputStream input = new FileInputStream(archive);
        boolean success = false;
        try {
            Cursor cursor = new EntryCursor(archive, new ZipInputStream(input));
            success = true;
            return cursor;
        } finally {
            if (success == false) {
                input.close();
            }
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + archive.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ZipRepository other = (ZipRepository) obj;
        if (!archive.equals(other.archive)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Zip({0})", //$NON-NLS-1$
                archive);
    }

    private static class EntryCursor implements Cursor {

        private final File source;

        private final ZipInputStream stream;

        private ZipEntry current;

        private int entries;

        EntryCursor(File source, ZipInputStream stream) {
            assert source != null;
            assert stream != null;
            this.source = source;
            this.stream = stream;
            this.entries = 0;
        }

        @Override
        public boolean next() throws IOException {
            while (true) {
                current = stream.getNextEntry();
                if (current == null) {
                    if (entries == 0) {
                        throw new IOException(MessageFormat.format(
                                "invalid ZIP format: {0}",
                                source));
                    }
                    return false;
                }
                entries++;
                if (current.isDirectory() == false) {
                    return true;
                }
            }
        }

        @Override
        public Location getLocation() {
            return Location.of(current.getName().replace('\\', '/'));
        }

        @Override
        public InputStream openResource() throws IOException {
            return new ZipEntryInputStream(stream);
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }

    private static class ZipEntryInputStream extends InputStream {

        private final ZipInputStream zipped;

        private boolean closed = false;

        ZipEntryInputStream(ZipInputStream zipped) {
            this.zipped = zipped;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return zipped.read(b);
        }

        @Override
        public int read() throws IOException {
            return zipped.read();
        }

        @Override
        public int available() throws IOException {
            return zipped.available();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return zipped.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return zipped.skip(n);
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            zipped.closeEntry();
            closed = true;
        }
    }
}
