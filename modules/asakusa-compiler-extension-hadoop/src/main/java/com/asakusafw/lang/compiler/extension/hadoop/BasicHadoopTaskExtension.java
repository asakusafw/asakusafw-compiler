package com.asakusafw.lang.compiler.extension.hadoop;

import java.util.Arrays;

import com.asakusafw.lang.compiler.api.basic.TaskContainer;
import com.asakusafw.lang.compiler.api.basic.TaskContainerMap;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Basic implementation of {@link HadoopTaskExtension}.
 */
public class BasicHadoopTaskExtension implements HadoopTaskExtension {

    private final TaskContainerMap tasks;

    /**
     * Creates a new instance.
     * @param tasks the task reference sink
     */
    public BasicHadoopTaskExtension(TaskContainerMap tasks) {
        this.tasks = tasks;
    }

    @Override
    public TaskReference addTask(ClassDescription mainClass, TaskReference... blockers) {
        return addTask(tasks.getMainTaskContainer(), mainClass, blockers);
    }

    @Override
    public TaskReference addFinalizer(ClassDescription mainClass, TaskReference... blockers) {
        return addTask(tasks.getFinalizeTaskContainer(), mainClass, blockers);
    }

    private TaskReference addTask(
            TaskContainer container,
            ClassDescription mainClass,
            TaskReference... blockers) {
        HadoopTaskReference task = new HadoopTaskReference(mainClass, Arrays.asList(blockers));
        container.add(task);
        return task;
    }
}
