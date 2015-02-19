package com.asakusafw.lang.compiler.api.basic;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.asakusafw.lang.compiler.api.reference.TaskReference;

/**
 * Holds {@link TaskReference} objects for each runtime phase.
 */
public class TaskContainer {

    private final TaskReference.Phase phase;

    private final Set<TaskReference> elements = new LinkedHashSet<>();

    /**
     * Creates a new instance.
     * @param phase the task phase
     */
    public TaskContainer(TaskReference.Phase phase) {
        this.phase = phase;
    }

    /**
     * Adds a task to this container.
     * Each {@link TaskReference#getBlockers() blocker task} must have been added.
     * If the task is already added to this, this will do nothing.
     * @param task the target task
     */
    public void add(TaskReference task) {
        if (elements.contains(task)) {
            return;
        }
        for (TaskReference blocker : task.getBlockers()) {
            if (elements.contains(blocker) == false) {
                throw new IllegalStateException(MessageFormat.format(
                        "blocker task is not found in {0} phase: {1}",
                        phase,
                        blocker));
            }
        }
        elements.add(task);
    }

    /**
     * Returns the added elements.
     * @return the added elements
     */
    public List<TaskReference> getElements() {
        return new ArrayList<>(elements);
    }
}
