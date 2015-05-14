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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Injects {@link Client} class and its script into the target library.
 */
public final class ClientInjector {

    static final Logger LOG = LoggerFactory.getLogger(ClientInjector.class);

    private ClientInjector() {
        return;
    }

    /**
     * Injects {@link Client} class file and its script.
     * @param source the target JAR file
     * @param destination the destination JAR file (may not exist)
     * @param entries the target classes
     * @throws IOException if failed to inject
     */
    public static void inject(File source, File destination, List<String> entries) throws IOException {
        try (ZipInputStream in = new ZipInputStream(new FileInputStream(source));
                ZipOutputStream out = new ZipOutputStream(Util.create(destination))) {
            copyEntries(in, out);
            putClient(out);
            putEntries(out, entries);
        }
    }

    private static void copyEntries(ZipInputStream input, ZipOutputStream output) throws IOException {
        while (true) {
            ZipEntry entry = input.getNextEntry();
            if (entry == null) {
                break;
            }
            String path = entry.getName();
            ZipEntry next = new ZipEntry(path);
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
            Util.copy(input, output);
        }
    }

    private static void putClient(ZipOutputStream output) throws IOException {
        String path = Util.getClientClassName().replace('.', '/') + ".class";
        try (InputStream resource = ClientInjector.class.getClassLoader().getResourceAsStream(path)) {
            if (resource == null) {
                throw new FileNotFoundException(path);
            }
            output.putNextEntry(new ZipEntry(path));
            Util.copy(resource, output);
        }
    }

    private static void putEntries(ZipOutputStream output, List<String> entries) throws IOException {
        output.putNextEntry(new ZipEntry(Client.PATH_ENTRIES));
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, Client.ENCODING));
        for (String entry : entries) {
            writer.print(entry);
            writer.print('\n');
        }
        writer.flush();
    }
}
