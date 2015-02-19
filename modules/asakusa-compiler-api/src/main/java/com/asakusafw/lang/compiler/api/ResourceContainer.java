package com.asakusafw.lang.compiler.api;

import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.common.Location;

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
