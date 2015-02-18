package com.asakusafw.lang.compiler.packaging;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Accepts resources.
 */
public interface ResourceSink extends Closeable {

    /**
     * Accepts an resource.
     * @param location the resource location
     * @param contents the resource contents (will be consumed immediately)
     * @throws IOException if failed to accept the resource by I/O error
     */
    void add(Location location, InputStream contents) throws IOException;
}
