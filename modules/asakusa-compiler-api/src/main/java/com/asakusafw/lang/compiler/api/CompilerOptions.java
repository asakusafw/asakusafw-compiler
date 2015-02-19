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
package com.asakusafw.lang.compiler.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a set of Asakusa DSL compiler options.
 */
public class CompilerOptions {

    private final String buildId;

    private final String runtimeWorkingDirectory;

    private final Map<String, String> properties;

    /**
     * Creates a new instance.
     * @param buildId the current build ID
     * @param runtimeWorkingDirectory the runtime working directory
     * @param properties the generic compiler properties
     */
    public CompilerOptions(
            String buildId,
            String runtimeWorkingDirectory,
            Map<String, String> properties) {
        this.buildId = buildId;
        this.runtimeWorkingDirectory = runtimeWorkingDirectory;
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    /**
     * Returns the current build ID.
     * @return the current build ID
     */
    public String getBuildId() {
        return buildId;
    }

    /**
     * Returns the framework specific runtime working directory.
     * <p>
     * This generally contains variables (<code>${...}</code>) to prevent from conflict between working files
     * of jobflow executions.
     * </p>
     * @return the runtime working directory path (trailing {@code '/'} is removed)
     * @see #getRuntimeWorkingPath(String)
     */
    public String getRuntimeWorkingDirectory() {
        return runtimeWorkingDirectory;
    }

    /**
     * Returns the framework specific runtime working file path.
     * @param relativePath relative path from {@link #getRuntimeWorkingDirectory() the runtime working directory}
     * @return the runtime working file path
     */
    public String getRuntimeWorkingPath(String relativePath) {
        return String.format("%s/%s", getRuntimeWorkingDirectory(), relativePath); //$NON-NLS-1$
    }

    /**
     * Returns the generic compiler properties.
     * @return the generic compiler properties
     */
    public Map<String, String> getProperties() {
        return properties;
    }
}
