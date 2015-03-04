package com.asakusafw.lang.compiler.extension.hadoop;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.BasicAttributeContainer;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * A symbol of task using {@code hadoop} command.
 */
public class HadoopTaskReference extends BasicAttributeContainer implements TaskReference {

    private static final String MODULE_NAME = "hadoop"; //$NON-NLS-1$

    private final String moduleName;

    private final ClassDescription mainClass;

    private final List<TaskReference> blockerTasks;

    /**
     * Creates a new instance.
     * @param mainClass the main class
     * @param blockerTasks the blocker tasks
     */
    public HadoopTaskReference(
            ClassDescription mainClass,
            List<? extends TaskReference> blockerTasks) {
        this(MODULE_NAME, mainClass, blockerTasks);
    }

    /**
     * Creates a new instance.
     * @param moduleName the module name
     * @param mainClass the main class
     * @param blockerTasks the blocker tasks
     */
    public HadoopTaskReference(
            String moduleName,
            ClassDescription mainClass,
            List<? extends TaskReference> blockerTasks) {
        this.moduleName = moduleName;
        this.mainClass = mainClass;
        this.blockerTasks = Collections.unmodifiableList(new ArrayList<>(blockerTasks));
    }

    @Override
    public String getModuleName() {
        return moduleName;
    }

    @Override
    public List<TaskReference> getBlockers() {
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
