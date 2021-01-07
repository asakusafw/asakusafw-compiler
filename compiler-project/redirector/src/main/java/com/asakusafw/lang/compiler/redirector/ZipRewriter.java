/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.redirector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrite ZIP/JAR file to redirect API invocations.
 */
public class ZipRewriter {

    static final Logger LOG = LoggerFactory.getLogger(ZipRewriter.class);

    static final String CLASS_EXTENSION = ".class"; //$NON-NLS-1$

    final ClassRewriter classRewriter;

    /**
     * Creates a new instance.
     * @param rule the redirect rule
     */
    public ZipRewriter(RedirectRule rule) {
        this.classRewriter = new ClassRewriter(rule);
    }

    /**
     * Rewrite entries in the ZIP archive and write them into the output.
     * @param input the source ZIP archive
     * @param output the target ZIP archive
     * @throws IOException if failed to rewrite by I/O error
     */
    public void rewrite(ZipInputStream input, ZipOutputStream output) throws IOException {
        while (true) {
            ZipEntry entry = input.getNextEntry();
            if (entry == null) {
                break;
            }
            ZipEntry next = new ZipEntry(entry.getName());
            next.setTime(next.getTime());
            next.setMethod(entry.getMethod());
            if (entry.getExtra() != null) {
                next.setExtra(entry.getExtra());
            }
            next.setComment(entry.getComment());
            output.putNextEntry(next);
            if (entry.isDirectory()) {
                continue;
            }
            if (isTarget(entry)) {
                LOG.trace("rewrite class: {}", entry.getName()); //$NON-NLS-1$
                classRewriter.rewrite(input, output);
            } else {
                LOG.trace("    copy file: {}", entry.getName()); //$NON-NLS-1$
                Util.copy(input, output);
            }
        }
    }

    /**
     * Rewrite entries in the ZIP archive file.
     * @param file the target ZIP file
     * @throws IOException if failed to rewrite by I/O error
     */
    public void rewrite(File file) throws IOException {
        LOG.debug("rewrting JAR file: {}", file); //$NON-NLS-1$
        File temporary = File.createTempFile("redirect-", ".zip"); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            try (ZipInputStream input = new ZipInputStream(new FileInputStream(file));
                    ZipOutputStream output = new ZipOutputStream(new FileOutputStream(temporary))) {
                rewrite(input, output);
            }
            try (InputStream input = new FileInputStream(temporary);
                    OutputStream output = new FileOutputStream(file)) {
                Util.copy(input, output);
            }
        } catch (IOException e) {
            throw new IOException(MessageFormat.format(
                    "failed to rewrite JAR file: {0}",
                    file), e);
        } finally {
            if (temporary.isFile() && temporary.delete() == false) {
                LOG.warn(MessageFormat.format(
                        "failed to delete a temporary file: {0}",
                        temporary));
            }
        }
    }

    private boolean isTarget(ZipEntry entry) {
        return entry.getName().endsWith(CLASS_EXTENSION);
    }
}
