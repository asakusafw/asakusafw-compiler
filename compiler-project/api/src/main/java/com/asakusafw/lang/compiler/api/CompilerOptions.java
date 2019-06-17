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
package com.asakusafw.lang.compiler.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import com.asakusafw.lang.compiler.common.util.StringUtil;

/**
 * Represents a set of Asakusa DSL compiler options.
 * @since 0.1.0
 * @version 0.5.4
 */
public class CompilerOptions {

    /**
     * the system property key prefix of each compiler options.
     * each property is only available if the target option was absent.
     * @since 0.5.4
     */
    public static final String PREFIX_SYSTEM_PROPERTY = "asakusafw.lang."; //$NON-NLS-1$

    /**
     * Represents a path of the default file system root.
     */
    public static final String ROOT_DIRECTORY = "/"; //$NON-NLS-1$

    /**
     * Represents a path of the file system working directory.
     */
    public static final String CURRENT_DIRECTORY = "."; //$NON-NLS-1$

    /**
     * Represents a path segment of execution ID.
     */
    public static final String NAME_EXECUTION_ID = "${execution_id}"; //$NON-NLS-1$

    private final String buildId;

    private final String runtimeWorkingDirectory;

    private final Map<String, String> properties;

    /**
     * Creates a new instance.
     * Clients may use {@link #builder()} instead of directly use this constructor.
     * @param buildId the current build ID
     * @param runtimeWorkingDirectory URI of the runtime working directory
     * @param properties the generic compiler properties
     * @see #builder()
     */
    public CompilerOptions(
            String buildId,
            String runtimeWorkingDirectory,
            Map<String, String> properties) {
        this.buildId = buildId;
        this.runtimeWorkingDirectory = normalize(runtimeWorkingDirectory);
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    /**
     * Creates a new builder instance.
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
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
     * This generally contains variables (<code>${...}</code>) to isolate working files
     * between different jobflow executions.
     * </p>
     * @return the runtime working directory path (trailing {@code '/'} is removed)
     * @see #getRuntimeWorkingPath(String)
     * @see #CURRENT_DIRECTORY
     * @see #ROOT_DIRECTORY
     * @see #NAME_EXECUTION_ID
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
        return path(getRuntimeWorkingDirectory(), relativePath);
    }

    /**
     * Returns the <em>RAW</em> compiler properties.
     * @return the <em>RAW</em> compiler properties
     */
    public Map<String, String> getRawProperties() {
        return properties;
    }

    /**
     * Returns generic compiler properties which have the provided common prefix.
     * @param propertyKeyPrefix the common prefix
     * @return the compiler properties
     */
    public Map<String, String> getProperties(String propertyKeyPrefix) {
        Map<String, String> results = new LinkedHashMap<>();
        String systemPropertyPrefix = PREFIX_SYSTEM_PROPERTY + propertyKeyPrefix;
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof String)) {
                continue;
            }
            String rawKey = (String) entry.getKey();
            if (!rawKey.startsWith(systemPropertyPrefix)) {
                continue;
            }
            String key = rawKey.substring(PREFIX_SYSTEM_PROPERTY.length());
            String value = (String) entry.getValue();
            results.put(key, value);
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(propertyKeyPrefix)) {
                results.put(key, entry.getValue());
            }
        }
        return results;
    }

    /**
     * Returns a generic compiler property.
     * @param propertyKey the property key
     * @param defaultValue the default value (nullable)
     * @return the property value, or the default value if the property value is not defined
     */
    public String get(String propertyKey, String defaultValue) {
        String value = properties.get(propertyKey);
        if (value == null) {
            return System.getProperty(PREFIX_SYSTEM_PROPERTY + propertyKey, defaultValue);
        }
        return value;
    }

    /**
     * Returns a generic compiler property.
     * @param propertyKey the property key
     * @param defaultValue the default value
     * @return the property value, or the default value if the property value is not defined
     */
    public boolean get(String propertyKey, boolean defaultValue) {
        String value = get(propertyKey, StringUtil.EMPTY).trim();
        if (value.isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    static String normalize(String path) {
        String result = path;
        while (result.endsWith("/")) { //$NON-NLS-1$
            if (result.equals(ROOT_DIRECTORY)) {
                break;
            }
            result = result.substring(0, result.length() - 1);
        }
        if (result.isEmpty()) {
            return CURRENT_DIRECTORY;
        }
        return result;
    }

    static String path(String basePath, String relativePath) {
        switch (basePath) {
        case CURRENT_DIRECTORY:
            return relativePath;
        case ROOT_DIRECTORY:
            return String.format("/%s", relativePath); //$NON-NLS-1$
        default:
            return String.format("%s/%s", basePath, relativePath); //$NON-NLS-1$
        }
    }

    /**
     * A builder for {@link CompilerOptions}.
     */
    public static class Builder {

        /**
         * The default runtime working directory.
         */
        public static final String DEFAULT_RUNTIME_WORKING_DIRECTORY =
                ".asakusafw.tmp/" + NAME_EXECUTION_ID; //$NON-NLS-1$

        private String buildId;

        private String runtimeWorkingDirectory = DEFAULT_RUNTIME_WORKING_DIRECTORY;

        private final Map<String, String> properties = new LinkedHashMap<>();

        /**
         * Sets the explicit build ID, instead of generated one.
         * @param newValue the explicit build ID
         * @return this
         */
        public Builder withBuildId(String newValue) {
            this.buildId = newValue;
            return this;
        }

        /**
         * Sets the framework specific runtime working directory.
         * @param newValue the runtime working directory path (trailing {@code '/'} is removed)
         * @param isolate isolates working directories between different jobflow executions
         *     by inserting {@link CompilerOptions#NAME_EXECUTION_ID execution ID} to end of the path
         * @return this
         */
        public Builder withRuntimeWorkingDirectory(String newValue, boolean isolate) {
            String base = newValue == null ? CURRENT_DIRECTORY : normalize(newValue);
            if (isolate) {
                this.runtimeWorkingDirectory = path(base, NAME_EXECUTION_ID);
            } else {
                this.runtimeWorkingDirectory = base;
            }
            return this;
        }

        /**
         * Sets a property.
         * @param key the property key
         * @param value the property value
         * @return this
         */
        public Builder withProperty(String key, String value) {
            return withProperties(Collections.singletonMap(key, value));
        }

        /**
         * Sets a property.
         * @param newValues new keys and values
         * @return this
         */
        public Builder withProperties(Map<String, String> newValues) {
            for (Map.Entry<String, String> entry : newValues.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (value == null) {
                    properties.remove(key);
                } else {
                    properties.put(key, value);
                }
            }
            return this;
        }

        /**
         * Builds and returns a {@link CompilerOptions} object.
         * @return the built object
         */
        public CompilerOptions build() {
            return new CompilerOptions(
                    buildId == null ? UUID.randomUUID().toString() : buildId,
                    runtimeWorkingDirectory,
                    properties);
        }
    }
}
