package com.asakusafw.lang.compiler.packaging;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Provides resources.
 */
public interface ResourceRepository {

    /**
     * Returns a new cursor for enumerating resources in this repository.
     * @return the created cursor
     * @throws IOException if failed to create a cursor
     */
    Cursor createCursor() throws IOException;

    /**
     * Represents a cursor for enumerating resources in {@link ResourceRepository}.
     */
    public interface Cursor extends Closeable {

        /**
         * An empty cursor.
         */
        Cursor EMPTY = new Cursor() {

            @Override
            public boolean next() {
                return false;
            }

            @Override
            public Location getLocation() {
                throw new IllegalStateException();
            }

            @Override
            public InputStream openResource() {
                throw new IllegalStateException();
            }

            @Override
            public void close() {
                return;
            }
        };

        /**
         * Advances this cursor and returns whether the next resource exists or not.
         * @return {@code true} if the next resource exists, otherwise {@code false}
         * @throws IOException if failed to advance this cursor by I/O error
         */
        boolean next() throws IOException;

        /**
         * Returns the resource path on this cursor.
         * @return the resource path (relative from the root of {@link ResourceRepository})
         */
        Location getLocation();

        /**
         * Returns the contents of the resource on this cursor.
         * @return the contents of the resource
         * @throws IOException if failed to open the resource by I/O error
         */
        InputStream openResource() throws IOException;
    }
}
