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
package com.asakusafw.bridge.launch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides extension contents.
 * @since 0.3.0
 */
public class ExtensionLoader {

    static final Logger LOG = LoggerFactory.getLogger(ExtensionLoader.class);

    /**
     * The prefix of environment variable names for each extension BLOBs.
     */
    static final String ENV_EXTENSION_PREFIX = "ASAKUSA_EXTENSION_";

    private final Map<String, File> extensions = new LinkedHashMap<>();

    /**
     * Creates a new instance which loads extensions from the current {@link System#getenv() environment variables}.
     */
    public ExtensionLoader() {
        this(System.getenv());
    }

    /**
     * Creates a new instance.
     * @param environmentVariables the environment variables
     */
    public ExtensionLoader(Map<String, String> environmentVariables) {
        Objects.requireNonNull(environmentVariables);
        for (Map.Entry<String, String> entry : environmentVariables.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(ENV_EXTENSION_PREFIX) == false) {
                continue;
            }
            String extension = key.substring(ENV_EXTENSION_PREFIX.length());
            File blob = new File(entry.getValue());
            if (blob.isFile()) {
                extensions.put(extension, blob);
            } else {
                LOG.warn(MessageFormat.format(
                        "invalid extension BLOB: {0}={1}",
                        extension, blob.getAbsolutePath()));
            }
        }
    }

    /**
     * Returns the all available extension names.
     * @return the available extension names
     */
    public Set<String> getAvailableExtensions() {
        return Collections.unmodifiableSet(extensions.keySet());
    }

    /**
     * Returns whether the target extension is available or not.
     * @param extension the target extension name
     * @return {@code true} if the target extension is available, otherwise {@code false}
     */
    public boolean isAvailable(String extension) {
        Objects.requireNonNull(extension);
        return extensions.containsKey(extension);
    }

    /**
     * Returns the contents of the target extension BLOB.
     * @param extension the target extension name
     * @return the target extension contents
     * @throws IOException if I/O error was occurred while opening the target contents
     */
    public InputStream open(String extension) throws IOException {
        Objects.requireNonNull(extension);
        File file = extensions.get(extension);
        if (file == null) {
            throw new NoSuchElementException(MessageFormat.format(
                    "unknown extension: {0}",
                    extension));
        }
        return new FileInputStream(file);
    }
}
