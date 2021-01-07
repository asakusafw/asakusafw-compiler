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
package com.asakusafw.lang.compiler.tester.executor;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.basic.JobflowPackager;
import com.asakusafw.lang.compiler.tester.executor.TaskExecutor.Context;
import com.asakusafw.runtime.stage.StageConstants;
import com.asakusafw.runtime.util.VariableTable;

/**
 * Utilities for {@link TaskExecutor}.
 */
public final class TaskExecutors {

    static final String EXTENSION_LIBRARY = ".jar"; //$NON-NLS-1$

    static final Location LOCATION_CORE_LIBRARIES = Location.of("core/lib"); //$NON-NLS-1$

    static final Location LOCATION_CORE_CONFIGURATION = Location.of("core/conf/asakusa-resources.xml"); //$NON-NLS-1$

    static final Location LOCATION_EXTENSION_LIBRARIES = Location.of("ext/lib"); //$NON-NLS-1$

    static final Location LOCATION_ATTACHED_LIBRARIES = Location.of("usr/lib"); //$NON-NLS-1$

    private TaskExecutors() {
        return;
    }

    /**
     * Returns the default task executor objects via SPI.
     * @param serviceLoader the service loader
     * @return the default task executors
     */
    public static Collection<TaskExecutor> loadDefaults(ClassLoader serviceLoader) {
        List<TaskExecutor> executors = new ArrayList<>();
        for (TaskExecutor executor : ServiceLoader.load(TaskExecutor.class, serviceLoader)) {
            executors.add(executor);
        }
        return executors;
    }

    /**
     * Resolves a path string using the current context.
     * @param context the current task execution context
     * @param path the target path
     * @return the resolved path
     */
    public static String resolvePath(Context context, String path) {
        VariableTable table = new VariableTable(VariableTable.RedefineStrategy.ERROR);
        table.defineVariable(StageConstants.VAR_USER, System.getProperty("user.name")); //$NON-NLS-1$
        table.defineVariable(StageConstants.VAR_BATCH_ID, context.getBatch().getBatchId());
        table.defineVariable(StageConstants.VAR_FLOW_ID, context.getJobflow().getFlowId());
        table.defineVariable(StageConstants.VAR_EXECUTION_ID, context.getExecutionId());
        return table.parse(path, true);
    }

    /**
     * Returns a framework file.
     * @param context the current task execution context
     * @param location the relative location from the framework installation root
     * @return the framework file path
     */
    public static File getFrameworkFile(Context context, Location location) {
        File base = context.getTesterContext().getFrameworkHome();
        File file = new File(base, location.toPath(File.separatorChar));
        return file;
    }

    /**
     * Returns a file in the current batch application package.
     * @param context the current task execution context
     * @param location the relative location from the batch application package root
     * @return the application file path
     */
    public static File getApplicationFile(Context context, Location location) {
        File base = new File(
                context.getTesterContext().getBatchApplicationHome(),
                context.getBatch().getBatchId());
        File file = new File(base, location.toPath(File.separatorChar));
        return file;
    }

    /**
     * Resolves a list of {@link CommandToken} for the current execution context.
     * @param context the current task execution context
     * @param tokens the target command tokens
     * @return the resolved tokens
     */
    public static List<String> resolveCommandTokens(Context context, List<? extends CommandToken> tokens) {
        List<String> results = new ArrayList<>();
        for (CommandToken token : tokens) {
            String resolved = resolveCommandToken(context, token);
            results.add(resolved);
        }
        return results;
    }

    /**
     * Resolves a {@link CommandToken} for the current execution context.
     * @param context the current task execution context
     * @param token the target command token
     * @return the resolved token
     */
    public static String resolveCommandToken(Context context, CommandToken token) {
        switch (token.getTokenKind()) {
        case TEXT:
            return token.getImage();
        case BATCH_ID:
            return context.getBatch().getBatchId();
        case FLOW_ID:
            return context.getJobflow().getFlowId();
        case EXECUTION_ID:
            return context.getExecutionId();
        case BATCH_ARGUMENTS:
            return encodeBatchArguments(context.getBatchArguments());
        default:
            throw new AssertionError(token);
        }
    }

    private static String encodeBatchArguments(Map<String, String> arguments) {
        VariableTable table = new VariableTable();
        table.defineVariables(arguments);
        return table.toSerialString();
    }

    /**
     * Returns the jobflow library file.
     * @param context the current task execution context
     * @return the library file
     */
    public static File getJobflowLibrary(Context context) {
        Location location = JobflowPackager.getLibraryLocation(context.getJobflow().getFlowId());
        return getApplicationFile(context, location);
    }

    /**
     * Returns the core configuration file.
     * @param context the current task execution context
     * @return the configuration file (a.k.a. {@code "asakusa-runtime.xml"})
     */
    public static File getCoreConfigurationFile(Context context) {
        return getFrameworkFile(context, LOCATION_CORE_CONFIGURATION);
    }

    /**
     * Returns the attached library files.
     * @param context the current task execution context
     * @return the library files, or an empty set if there are no such files
     */
    public static Set<File> getAttachedLibraries(Context context) {
        File directory = getApplicationFile(context, LOCATION_ATTACHED_LIBRARIES);
        return getLibraries(directory);
    }

    /**
     * Returns the library files on the framework directory.
     * @param context the current task execution context
     * @param location the directory location (relative from the framework installation path)
     * @return the library files, or an empty set if there are no such files
     */
    public static Set<File> getFrameworkLibraries(Context context, Location location) {
        File directory = getFrameworkFile(context, location);
        return getLibraries(directory);
    }

    /**
     * Returns the framework core library files.
     * @param context the current task execution context
     * @return the library files, or an empty set if there are no such files
     */
    public static Set<File> getCoreLibraries(Context context) {
        return getFrameworkLibraries(context, LOCATION_CORE_LIBRARIES);
    }

    /**
     * Returns the framework extension library files.
     * @param context the current task execution context
     * @return the library files, or an empty set if there are no such files
     */
    public static Set<File> getExtensionLibraries(Context context) {
        return getFrameworkLibraries(context, LOCATION_EXTENSION_LIBRARIES);
    }

    private static Set<File> getLibraries(File directory) {
        if (directory.isDirectory() == false) {
            return Collections.emptySet();
        }
        Set<File> results = new LinkedHashSet<>();
        for (File file : list(directory)) {
            if (file.getName().endsWith(EXTENSION_LIBRARY)) {
                results.add(file);
            }
        }
        return results;
    }

    private static List<File> list(File directory) {
        return Optional.ofNullable(directory.listFiles())
                .map(Arrays::asList)
                .orElse(Collections.emptyList());
    }
}
