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
package com.asakusafw.bridge.redirector;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * ZIP archive utilities.
 */
final class ZipUtil {

    private ZipUtil() {
        return;
    }

    static byte[] consume(InputStream contents) throws IOException {
        byte[] bytes;
        try (ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
            Util.copy(contents, buf);
            bytes = buf.toByteArray();
        }
        return bytes;
    }

    static Map<String, byte[]> dump(ZipInputStream input) throws IOException {
        Map<String, byte[]> results = new LinkedHashMap<>();
        while (true) {
            ZipEntry entry = input.getNextEntry();
            if (entry == null) {
                break;
            }
            if (entry.isDirectory()) {
                continue;
            }
            results.put(entry.getName(), consume(input));
        }
        return results;
    }

    static void load(ZipOutputStream output, Map<String, byte[]> entries) throws IOException {
        for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
            ZipEntry name = new ZipEntry(entry.getKey());
            byte[] contents = entry.getValue();
            output.putNextEntry(name);
            output.write(contents);
        }
    }
}
