/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
