package com.asakusafw.lang.compiler.api.basic;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;

/**
 * Holds {@link TaskReference} objects for whole executions.
 */
public class TaskContainerMap implements TaskReferenceMap {

    private final Map<TaskReference.Phase, TaskContainer> containers;
    {
        Map<TaskReference.Phase, TaskContainer> map = new EnumMap<>(TaskReference.Phase.class);
        for (TaskReference.Phase phase : TaskReference.Phase.values()) {
            map.put(phase, new TaskContainer(phase));
        }
        this.containers = Collections.unmodifiableMap(map);
    }

    @Override
    public Collection<? extends TaskReference> getTasks(Phase phase) {
        return getTaskContainer(phase).getElements();
    }

    /**
     * Returns a {@link TaskContainer} for the specified phase.
     * @param phase the target phase
     * @return the related task reference container
     */
    public TaskContainer getTaskContainer(TaskReference.Phase phase) {
        return containers.get(phase);
    }

    /**
     * Returns a {@link TaskContainer} for {@code initialize} phase.
     * @return task reference container
     */
    public TaskContainer getInitializeTaskContainer() {
        return getTaskContainer(TaskReference.Phase.INITIALIZE);
    }

    /**
     * Returns a {@link TaskContainer} for {@code import} phase.
     * @return task reference container
     */
    public TaskContainer getImportTaskContainer() {
        return getTaskContainer(TaskReference.Phase.IMPORT);
    }

    /**
     * Returns a {@link TaskContainer} for {@code prologue} phase.
     * @return task reference container
     */
    public TaskContainer getPrologueTaskContainer() {
        return getTaskContainer(TaskReference.Phase.PROLOGUE);
    }

    /**
     * Returns a {@link TaskContainer} for {@code main} phase.
     * @return task reference container
     */
    public TaskContainer getMainTaskContainer() {
        return getTaskContainer(TaskReference.Phase.MAIN);
    }

    /**
     * Returns a {@link TaskContainer} for {@code epilogue} phase.
     * @return task reference container
     */
    public TaskContainer getEpilogueTaskContainer() {
        return getTaskContainer(TaskReference.Phase.EPILOGUE);
    }

    /**
     * Returns a {@link TaskContainer} for {@code export} phase.
     * @return task reference container
     */
    public TaskContainer getExportTaskContainer() {
        return getTaskContainer(TaskReference.Phase.EXPORT);
    }

    /**
     * Returns a {@link TaskContainer} for {@code finalize} phase.
     * @return task reference container
     */
    public TaskContainer getFinalizeTaskContainer() {
        return getTaskContainer(TaskReference.Phase.FINALIZE);
    }
}
