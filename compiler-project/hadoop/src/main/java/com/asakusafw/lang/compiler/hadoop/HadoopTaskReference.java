/**
 * Copyright 2011-2017 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.lang.compiler.hadoop;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.BasicAttributeContainer;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * A symbol of task using {@code hadoop} command.
 * @since 0.1.0
 * @version 0.3.0
 */
public class HadoopTaskReference extends BasicAttributeContainer implements TaskReference {

    private static final String MODULE_NAME = "hadoop"; //$NON-NLS-1$

    private final String moduleName;

    private final ClassDescription mainClass;

    private final Set<String> extensions;

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
     * @param mainClass the main class
     * @param extensions the acceptable extension names
     * @param blockerTasks the blocker tasks
     * @since 0.3.0
     */
    public HadoopTaskReference(
            ClassDescription mainClass,
            Collection<String> extensions,
            List<? extends TaskReference> blockerTasks) {
        this(MODULE_NAME, mainClass, extensions, blockerTasks);
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
        this(moduleName, mainClass, Collections.emptySet(), blockerTasks);
    }

    /**
     * Creates a new instance.
     * @param moduleName the module name
     * @param mainClass the main class
     * @param extensions the acceptable extension names
     * @param blockerTasks the blocker tasks
     * @since 0.3.0
     */
    public HadoopTaskReference(
            String moduleName,
            ClassDescription mainClass,
            Collection<String> extensions,
            List<? extends TaskReference> blockerTasks) {
        this.moduleName = moduleName;
        this.mainClass = mainClass;
        this.extensions = Collections.unmodifiableSet(new LinkedHashSet<>(extensions));
        this.blockerTasks = Collections.unmodifiableList(new ArrayList<>(blockerTasks));
    }

    @Override
    public String getModuleName() {
        return moduleName;
    }

    @Override
    public Set<String> getExtensions() {
        return extensions;
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
                mainClass.getClassName());
    }
}
