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
