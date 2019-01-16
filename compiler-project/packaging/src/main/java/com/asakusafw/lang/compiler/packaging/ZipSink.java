/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An implementation of {@link ResourceSink} which adds resources into ZIP archive.
 */
public class ZipSink implements ResourceSink {

    private static final Charset ENCODING = StandardCharsets.UTF_8;

    private final ZipOutputStream output;

    /**
     * Creates a new instance.
     * @param output the target file
     * @throws IOException if failed to create ZIP file
     */
    public ZipSink(File output) throws IOException {
        this(new ZipOutputStream(ResourceUtil.create(output), ENCODING));
    }

    /**
     * Creates a new instance.
     * @param output the target ZIP output stream
     */
    public ZipSink(ZipOutputStream output) {
        this.output = output;
    }

    @Override
    public void add(Location location, InputStream contents) throws IOException {
        output.putNextEntry(new ZipEntry(location.toPath()));
        ResourceUtil.copy(contents, output);
    }

    @Override
    public void add(Location location, ContentProvider provider) throws IOException {
        output.putNextEntry(new ZipEntry(location.toPath()));
        try (OutputStream contents = new ZipEntryOutputStream(output)) {
            provider.writeTo(output);
        }
    }

    @Override
    public void close() throws IOException {
        output.close();
    }

    private static class ZipEntryOutputStream extends OutputStream {

        private final ZipOutputStream zipped;

        private boolean closed = false;

        ZipEntryOutputStream(ZipOutputStream zipped) {
            this.zipped = zipped;
        }

        @Override
        public void write(byte[] b) throws IOException {
            zipped.write(b);
        }

        @Override
        public void write(int b) throws IOException {
            zipped.write(b);
        }

        @Override
        public void flush() throws IOException {
            zipped.flush();
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            zipped.write(b, off, len);
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
