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
package com.asakusafw.lang.compiler.api.basic;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.CommandToken;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.BasicExtensionContainer;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * An abstract implementation of {@link com.asakusafw.lang.compiler.api.JobflowProcessor.Context}.
 */
public abstract class AbstractJobflowProcessorContext extends BasicExtensionContainer
        implements JobflowProcessor.Context {

    private static final String EXTENSION_CLASS = ".class"; //$NON-NLS-1$

    private final TaskContainerMap tasks = new TaskContainerMap();

    private final ExternalPortContainer externals = new ExternalPortContainer();

    /**
     * Returns tasks which are executed in this context.
     * @return tasks
     */
    public TaskContainerMap getTasks() {
        return tasks;
    }

    /**
     * Returns the external ports container.
     * @return the external ports container
     */
    public ExternalPortContainer getExternalPorts() {
        return externals;
    }

    @Override
    public OutputStream addClassFile(ClassDescription aClass) throws IOException {
        String path = aClass.getInternalName() + EXTENSION_CLASS;
        return addResourceFile(Location.of(path, '/'));
    }

    @Override
    public ExternalInputReference addExternalInput(String name, ExternalInputInfo info) {
        ExternalInputReference result = createExternalInput(name, info);
        externals.addInput(result);
        return result;
    }

    @Override
    public ExternalOutputReference addExternalOutput(
            String name,
            ExternalOutputInfo info,
            Collection<String> internalOutputPaths) {
        ExternalOutputReference result = createExternalOutput(name, info, internalOutputPaths);
        externals.addOutput(result);
        return result;
    }

    /**
     * Creates a new {@link ExternalInputReference}.
     * @param name the input name
     * @param info the structural information of this external input
     * @return the created instance
     * @see #addExternalInput(String, ExternalInputInfo)
     */
    protected abstract ExternalInputReference createExternalInput(String name, ExternalInputInfo info);

    /**
     * Creates a new {@link ExternalOutputReference}.
     * @param name the input name
     * @param info the structural information of this external output
     * @param internalOutputPaths the output paths which will be internally generated in this jobflow
     * @return the created instance
     * @see #addExternalOutput(String, ExternalOutputInfo, Collection)
     */
    protected abstract ExternalOutputReference createExternalOutput(
            String name,
            ExternalOutputInfo info,
            Collection<String> internalOutputPaths);

    @Override
    public final TaskReference addTask(
            String moduleName,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            TaskReference... blockers) {
        return addTask(moduleName, profileName, command, arguments, Collections.emptySet(), blockers);
    }

    @Override
    public final TaskReference addFinalizer(
            String moduleName,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            TaskReference... blockers) {
        return addFinalizer(moduleName, profileName, command, arguments, Collections.emptySet(), blockers);
    }

    @Override
    public TaskReference addTask(
            String moduleName, String profileName,
            Location command, List<? extends CommandToken> arguments, Collection<String> extensions,
            TaskReference... blockers) {
        return addTask(tasks.getMainTaskContainer(),
                moduleName, profileName,
                command, arguments, extensions,
                blockers);
    }

    @Override
    public TaskReference addFinalizer(
            String moduleName, String profileName,
            Location command, List<? extends CommandToken> arguments, Collection<String> extensions,
            TaskReference... blockers) {
        return addTask(tasks.getFinalizeTaskContainer(),
                moduleName, profileName,
                command, arguments, extensions,
                blockers);
    }

    private TaskReference addTask(
            TaskContainer container,
            String moduleName,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            Collection<String> extensions,
            TaskReference... blockers) {
        CommandTaskReference task = new CommandTaskReference(
                moduleName, profileName,
                command, arguments, extensions,
                Arrays.asList(blockers));
        container.add(task);
        return task;
    }
}
