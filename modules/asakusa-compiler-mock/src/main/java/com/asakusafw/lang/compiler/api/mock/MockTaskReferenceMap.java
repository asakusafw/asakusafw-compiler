package com.asakusafw.lang.compiler.api.mock;

import com.asakusafw.lang.compiler.api.basic.TaskContainer;
import com.asakusafw.lang.compiler.api.basic.TaskContainerMap;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;

/**
 * Mock implementation of {@link TaskReferenceMap}.
 */
public class MockTaskReferenceMap extends TaskContainerMap {

    /**
     * Adds a {@link TaskReference}.
     * @param phase the target phase
     * @param task the target task
     * @return this
     */
    public MockTaskReferenceMap add(TaskReference.Phase phase, TaskReference task) {
        getTaskContainer(phase).add(task);
        return this;
    }

    /**
     * Adds {@link TaskReference}s.
     * @param phase the target phase
     * @param tasks target tasks
     * @return this
     */
    public MockTaskReferenceMap add(TaskReference.Phase phase, TaskReference... tasks) {
        TaskContainer container = getTaskContainer(phase);
        for (TaskReference task : tasks) {
            container.add(task);
        }
        return this;
    }
}
