package com.asakusafw.lang.compiler.packaging;

import java.io.File;
import java.io.IOException;

import com.asakusafw.lang.compiler.common.Location;

/**
 * A file visitor in containers.
 */
public interface FileVisitor {

    /**
     * Processes the target file.
     * @param location the resource path (relative from the container root)
     * @param file the target file
     * @return {@code true} if also visits each element in the current directory
     * @throws IOException if failed to process the target file
     */
    boolean process(Location location, File file) throws IOException;
}