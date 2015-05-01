/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.tool.yaess.compress;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Installs artifact files.
 */
public class Installer implements Closeable {

    static final Logger LOG = LoggerFactory.getLogger(Installer.class);

    private final LinkedList<Entry> entries = new LinkedList<>();

    /**
     * Adds an installer action.
     * @param contents the installation contents
     * @param target the installation target path
     * @param backup the backup file path
     */
    public void register(File contents, File target, File backup) {
        entries.add(new Entry(contents, target, backup));
    }

    /**
     * Performs installation.
     * @throws IOException if failed to install by I/O error
     */
    public void install() throws IOException {
        while (entries.isEmpty() == false) {
            Entry entry = entries.getFirst();
            move(entry.target, entry.backup);
            move(entry.contents, entry.target);
            entries.removeFirst();
        }
    }

    private void move(File source, File destination) throws IOException {
        Util.delete(destination);
        Util.prepare(destination);
        if (source.renameTo(destination) == false) {
            try (InputStream in = new FileInputStream(source);
                    OutputStream out = Util.create(destination)) {
                Util.copy(in, out);
            }
            Util.deleteSoft(source);
        }
    }

    @Override
    public void close() throws IOException {
        while (entries.isEmpty() == false) {
            Entry entry = entries.getFirst();
            Util.deleteSoft(entry.contents);
            entries.removeFirst();
        }
    }

    private static class Entry {

        final File contents;

        final File target;

        final File backup;

        public Entry(File contents, File target, File backup) {
            this.contents = contents;
            this.target = target;
            this.backup = backup;
        }
    }
}
