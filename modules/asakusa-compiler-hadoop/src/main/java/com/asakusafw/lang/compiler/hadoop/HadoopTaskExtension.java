package com.asakusafw.lang.compiler.hadoop;

import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An extension for using Hadoop tasks.
 */
public interface HadoopTaskExtension {

    /**
     * Adds a Hadoop sub-application to execute in this application.
     * @param phase the execution phase
     * @param mainClass the main class
     * @param blockers the blocker sub-applications
     * @return a symbol that represents the added sub-application
     */
    TaskReference addTask(TaskReference.Phase phase, ClassDescription mainClass, TaskReference... blockers);
}
