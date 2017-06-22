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
package com.asakusafw.lang.info.cli;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

final class ListUtil {

    static final String ENV_ASAKUSA_HOME = "ASAKUSA_HOME";

    static final File ASAKUSA_BATCHAPPS_HOME;
    static {
        ASAKUSA_BATCHAPPS_HOME = Stream.of(
            Optional.ofNullable(System.getenv("ASAKUSA_BATCHAPPS_HOME"))
                .map(it -> it.trim())
                .filter(it -> it.isEmpty() == false)
                .map(File::new),
            Optional.ofNullable(System.getenv(ENV_ASAKUSA_HOME))
                .map(it -> it.trim())
                .filter(it -> it.isEmpty() == false)
                .map(File::new)
                .map(it -> new File(it, "batchapps")))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(File::isDirectory)
            .findFirst()
            .orElse(null);
    }

    static final String PATH_BATCH_INFO = "etc/batch-info.json";

    private ListUtil() {
        return;
    }

    static Optional<File> findBatchInfo(File batchapp) {
        return Optional.ofNullable(batchapp)
                .map(it -> new File(batchapp, PATH_BATCH_INFO))
                .filter(File::isFile);
    }

    static String normalize(Object value) {
        return Optional.ofNullable(value)
                .map(String::valueOf)
                .orElse("N/A");
    }

    static String padding(int count) {
        if (count < 0) {
            return "";
        }
        StringBuilder buf = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            buf.append(' ');
        }
        return buf.toString();
    }

    static void printBlock(PrintWriter writer, int indent, Map<String, ?> members) {
        if (members.isEmpty()) {
            return;
        }
        int maxKeyLen = members.keySet().stream()
                .mapToInt(String::length)
                .max()
                .getAsInt();
        members.forEach((k, v) -> {
            writer.printf("%s%s: %s%n",
                    padding(indent + maxKeyLen - k.length()),
                    k,
                    normalize(v));
        });
    }

    static void printBlock(PrintWriter writer, int indent, String title, List<?> members) {
        if (members.isEmpty()) {
            writer.printf("%s%s: -%n", ListUtil.padding(indent), normalize(title));
        } else {
            writer.printf("%s%s:%n", ListUtil.padding(indent), normalize(title));
            members.forEach(it -> writer.printf("%s%s%n", ListUtil.padding(indent + 4), normalize(it)));
        }
    }
}
