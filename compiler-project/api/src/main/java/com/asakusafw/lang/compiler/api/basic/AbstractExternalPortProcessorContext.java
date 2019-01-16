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
package com.asakusafw.lang.compiler.api.basic;

import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.BasicExtensionContainer;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An abstract implementation of {@link com.asakusafw.lang.compiler.api.ExternalPortProcessor.Context}.
 */
public abstract class AbstractExternalPortProcessorContext extends BasicExtensionContainer
        implements ExternalPortProcessor.Context {

    private static final String EXTENSION_CLASS = ".class"; //$NON-NLS-1$

    private final TaskContainerMap tasks = new TaskContainerMap();

    /**
     * Returns tasks which are executed in this context.
     * @return tasks
     */
    public TaskContainerMap getTasks() {
        return tasks;
    }

    @Override
    public OutputStream addClassFile(ClassDescription aClass) throws IOException {
        String path = aClass.getInternalName() + EXTENSION_CLASS;
        return addResourceFile(Location.of(path, '/'));
    }

    @Override
    public void addTask(TaskReference.Phase phase, TaskReference task) {
        tasks.getTaskContainer(phase).add(task);
    }
}
