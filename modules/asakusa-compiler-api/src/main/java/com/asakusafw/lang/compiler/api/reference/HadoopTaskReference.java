package com.asakusafw.lang.compiler.api.reference;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * A symbol of task using {@code hadoop} command.
 */
public class HadoopTaskReference implements TaskReference {

    private final int serialNumber;

    private final ClassDescription mainClass;

    private final List<TaskReference> blockerTasks;

    /**
     * Creates a new instance.
     * @param serialNumber the serial number of this task (unique in the same execution phase)
     * @param mainClass the main class
     * @param blockerTasks the blocker tasks
     */
    public HadoopTaskReference(
            int serialNumber,
            ClassDescription mainClass,
            List<? extends TaskReference> blockerTasks) {
        this.serialNumber = serialNumber;
        this.mainClass = mainClass;
        this.blockerTasks = Collections.unmodifiableList(new ArrayList<>(blockerTasks));
    }

    @Override
    public TaskKind getTaskKind() {
        return TaskKind.HADOOP;
    }

    /**
     * Returns the serial number of this task: this must be identical in the same execution phase.
     * @return the serial number
     */
    public int getSerialNumber() {
        return serialNumber;
    }

    @Override
    public List<TaskReference> getBlockerTasks() {
        return blockerTasks;
    }

    /**
     * Returns the main class.
     * @return the main class
     */
    public ClassDescription getMainClass() {
        return mainClass;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "HadoopTask({0})", //$NON-NLS-1$
                mainClass.getName());
    }
}
