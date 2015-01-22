package com.asakusafw.lang.compiler.api.reference;

import java.util.Collection;

/**
 * Represents a task in runtime.
 */
public interface TaskReference extends Reference {

    /**
     * Returns the task kind.
     * @return the task kind
     */
    TaskKind getTaskKind();

    /**
     * Returns tasks which must be executed before this task.
     * @return the blocker tasks
     */
    Collection<? extends TaskReference> getBlockerTasks();

    /**
     * Represents a kind of {@link TaskReference}.
     */
    public static enum TaskKind {

        /**
         * Run with {@code hadoop} command.
         * @see HadoopTaskReference
         */
        HADOOP,

        /**
         * Run any command.
         * @see CommandTaskReference
         */
        COMMAND,
    }
}
