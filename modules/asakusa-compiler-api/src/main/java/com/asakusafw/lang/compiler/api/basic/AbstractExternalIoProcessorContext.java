package com.asakusafw.lang.compiler.api.basic;

import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.ExternalIoProcessor;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An abstract implementation of {@link com.asakusafw.lang.compiler.api.ExternalIoProcessor.Context}.
 */
public abstract class AbstractExternalIoProcessorContext extends BasicExtensionContainer
        implements ExternalIoProcessor.Context {

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
        String path = aClass.getName().replace('.', '/') + EXTENSION_CLASS;
        return addResourceFile(Location.of(path, '/'));
    }

    @Override
    public void addTask(TaskReference.Phase phase, TaskReference task) {
        tasks.getTaskContainer(phase).add(task);
    }
}
