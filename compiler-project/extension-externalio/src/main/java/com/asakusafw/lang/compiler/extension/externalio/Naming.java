/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.externalio;

import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Naming utilities about external components.
 */
public final class Naming {

    /**
     * The base package for the generated classes.
     */
    public static final Location BASE_PACKAGE = Location.of("com.asakusafw.application", '.'); //$NON-NLS-1$

    private Naming() {
        return;
    }

    /**
     * Returns a standard stage ID.
     * @param moduleId the target module ID
     * @param phase the target phase
     * @return a standard stage ID
     */
    public static String getStageId(String moduleId, TaskReference.Phase phase) {
        return Location.of(phase.getSymbol())
                .append(Location.of(moduleId, '.'))
                .toPath('.');
    }

    /**
     * Returns the package name.
     * @param moduleId the target module ID
     * @param phase the target phase
     * @return the corresponded package name
     */
    public static String getPackage(String moduleId, TaskReference.Phase phase) {
        String name = getLocation(moduleId, phase).toPath('.');
        return name;
    }

    /**
     * Returns a class name for prologue phase.
     * @param moduleId the target module ID
     * @param phase the target phase
     * @param simpleName the simple name of the class
     * @return the related class
     */
    public static ClassDescription getClass(String moduleId, TaskReference.Phase phase, String simpleName) {
        String name = getLocation(moduleId, phase).append(simpleName).toPath('.');
        return new ClassDescription(name);
    }

    /**
     * Returns a class name for prologue phase.
     * @param moduleId the target module ID
     * @param phase the target phase
     * @param simpleName the simple name of the class
     * @param index the class index
     * @return the related class
     */
    public static ClassDescription getClass(String moduleId, TaskReference.Phase phase, String simpleName, int index) {
        String indexedName = String.format("%s%d", simpleName, index); //$NON-NLS-1$
        return getClass(moduleId, phase, indexedName);
    }

    private static Location getLocation(String moduleId, TaskReference.Phase phase) {
        return BASE_PACKAGE
                .append(Location.of(moduleId, '.'))
                .append(Location.of(phase.getSymbol(), '.'));
    }

}
