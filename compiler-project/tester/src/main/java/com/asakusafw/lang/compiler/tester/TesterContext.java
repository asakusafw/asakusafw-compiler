/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.tester;

import java.io.File;
import java.text.MessageFormat;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.packaging.ResourceUtil;

/**
 * A root context for testing.
 */
public class TesterContext {

    static final Logger LOG = LoggerFactory.getLogger(TesterContext.class);

    /**
     * Environmental variable of the framework home path.
     */
    public static final String ENV_FRAMEWORK_PATH = "ASAKUSA_HOME"; //$NON-NLS-1$

    /**
     * Environmental variable of the batch applications installation base path.
     */
    public static final String ENV_BATCHAPPS_PATH = "ASAKUSA_BATCHAPPS_HOME"; //$NON-NLS-1$

    /**
     * The default path of batch application installation base path (relative from the framework home path).
     */
    public static final String DEFAULT_BATCHAPPS_PATH = "batchapps"; //$NON-NLS-1$

    private final ClassLoader classLoader;

    private final Map<String, String> environmentVariables;

    private final Set<File> temporaryFiles = new LinkedHashSet<>();

    /**
     * Creates a new instance.
     * @param classLoader the class loader
     * @param environmentVariables the environment variables
     */
    public TesterContext(ClassLoader classLoader, Map<String, String> environmentVariables) {
        this.classLoader = classLoader;
        this.environmentVariables = environmentVariables;
    }

    /**
     * Returns the class loader to load testing peripherals.
     * @return the class loader
     */
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Returns the environment variables.
     * @return the environment variables
     */
    public Map<String, String> getEnvironmentVariables() {
        return environmentVariables;
    }

    /**
     * Adds a temporary file which may be deleted on the relative tester was removed.
     * @param file a temporary file
     */
    public void addTemporaryFile(File file) {
        this.temporaryFiles.add(file);
    }

    /**
     * Removes all temporary files.
     * @see #addTemporaryFile(File)
     */
    public void removeTemporaryFiles() {
        for (File file : temporaryFiles) {
            if (file.exists() && ResourceUtil.delete(file) == false) {
                LOG.warn(MessageFormat.format(
                        "failed to delete a temporary file: {0}",
                        file));
            }
        }
        temporaryFiles.clear();
    }

    /**
     * Returns the framework installation path.
     * @return the framework installation path
     */
    public File getFrameworkHome() {
        return getFrameworkHome(getEnvironmentVariables());
    }

    /**
     * Returns the batch application installation base path.
     * @return the batch application installation base path
     */
    public File getBatchApplicationHome() {
        return getBatchApplicationHome(getEnvironmentVariables());
    }

    /**
     * Returns the framework installation path.
     * @param environmentVariables the environment variables
     * @return the framework installation path
     */
    public static File getFrameworkHome(Map<String, String> environmentVariables) {
        File path = getPath(environmentVariables, ENV_FRAMEWORK_PATH);
        if (path == null) {
            throw new IllegalStateException(MessageFormat.format(
                    "environment variable must be defined: {0}",
                    ENV_FRAMEWORK_PATH));
        }
        return path;
    }

    /**
     * Returns the batch application installation base path.
     * @param environmentVariables the environment variables
     * @return the batch application installation base path
     */
    public static File getBatchApplicationHome(Map<String, String> environmentVariables) {
        File path = getPath(environmentVariables, ENV_BATCHAPPS_PATH);
        if (path == null) {
            File home = getFrameworkHome(environmentVariables);
            return new File(home, DEFAULT_BATCHAPPS_PATH);
        }
        return path;
    }

    private static File getPath(Map<String, String> environmentVariables, String key) {
        String value = environmentVariables.get(key);
        if (value == null) {
            return null;
        }
        return new File(value);
    }
}
