package com.asakusafw.lang.compiler.api.basic;

import java.text.MessageFormat;
import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.api.reference.TaskReference;

/**
 * Holds {@link TaskReference} objects.
 */
public class TaskReferenceContainer {

    private final String phaseName;

    private int serialNumber = 1;

    private final Set<TaskReference> elements = new LinkedHashSet<>();

    /**
     * Creates a new instance.
     * @param phaseName the phase name
     */
    public TaskReferenceContainer(String phaseName) {
        this.phaseName = phaseName;
    }

    /**
     * Returns the next serial number of task.
     * @return the next serial number of task
     */
    public int getNextSerialNumber() {
        return serialNumber++;
    }

    /**
     * Adds a task to this container.
     * If the task is already added to this, this will do nothing.
     * @param task the target task
     */
    public void add(TaskReference task) {
        if (elements.contains(task)) {
            return;
        }
        for (TaskReference blocker : task.getBlockerTasks()) {
            if (elements.contains(blocker) == false) {
                throw new IllegalStateException(MessageFormat.format(
                        "blocker task is not found in {0} phase: {1}",
                        phaseName,
                        blocker));
            }
        }
        elements.add(task);
    }

    /**
     * Returns the added elements.
     * @return the added elements
     */
    public Set<TaskReference> getElements() {
        return new LinkedHashSet<>(elements);
    }
}
