/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.core.adapter;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Utilities for this package.
 *
 */
final class Util {

    private static final String EXTENSION_CLASS = ".class"; //$NON-NLS-1$

    private Util() {
        return;
    }

    /**
     * Returns the file location of the class.
     * @param description the target class description
     * @return the file location of the class (relative from the classpath root)
     */
    public static Location toClassFileLocation(ClassDescription description) {
        String path = description.getName().replace('.', '/') + EXTENSION_CLASS;
        return Location.of(path, '/');
    }
}
