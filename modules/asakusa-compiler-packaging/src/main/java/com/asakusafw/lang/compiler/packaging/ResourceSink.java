package com.asakusafw.lang.compiler.packaging;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

    /**
     * Accepts an resource.
     * @param location the resource location
     * @param callback the callback object for preparing resource contents
     * @throws IOException if failed to accept the resource by I/O error
     */
    void add(Location location, Callback callback) throws IOException;

    /**
     * A callback interface for preparing resource contents.
     * @see ResourceSink#add(Location, Callback)
     */
    public interface Callback {

        /**
         * Adds an resource.
         * The {@link OutputStream} will be closed after this method execution was finished.
         * @param location the  resource location
         * @param contents the resource contents sink
         * @throws IOException if error occurred while executing this method
         */
        void add(Location location, OutputStream contents) throws IOException;
    }
}
