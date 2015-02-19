package com.asakusafw.lang.compiler.common;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An abstract super interface of resource containers.
 */
public interface ResourceContainer {

    /**
     * Adds a new resource and returns its output stream.
     * @param location the resource path (relative from the container root)
     * @return the output stream to set the target contents
     * @throws IOException if failed to create a new resource
     */
    OutputStream addResource(Location location) throws IOException;
}
