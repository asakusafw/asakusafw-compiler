package com.asakusafw.lang.compiler.packaging;

import java.io.IOException;
import java.io.InputStream;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Represents a resource.
 */
public interface ResourceItem {

    /**
     * Returns the resource path where this item exists.
     * @return the resource path
     */
    Location getLocation();

    /**
     * Returns the contents of this resource.
     * @return the contents of this resource
     * @throws IOException if failed to open the resource by I/O error
     */
    InputStream openResource() throws IOException;
}
