/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.dag.api.processor;

import java.io.IOException;

import com.asakusafw.dag.api.common.ObjectCursor;

/**
 * An abstract super interface of reading object groups from input edges.
 * @since 0.4.0
 */
public interface GroupReader extends EdgeReader, ObjectCursor {

    /**
     * Advances the cursor in the next group, and returns whether or not the next group exists.
     * This method may change previously {@link #getGroup()} result objects.
     * @throws IOException if I/O error occurred while reading the next group
     * @throws InterruptedException if interrupted while reading the next group
     * @return {@code true} if the next group exists, otherwise {@code false}
     */
    boolean nextGroup() throws IOException, InterruptedException;

    /**
     * Returns group information which is on the current cursor position.
     * @return the current group information
     * @throws IllegalStateException if the cursor is not on the current group
     * @throws IOException if I/O error occurred while reading the current group
     * @throws InterruptedException if interrupted while reading the current group
     * @see #nextGroup()
     */
    GroupInfo getGroup() throws IOException, InterruptedException;

    /**
     * Advances the cursor in the current group, and returns whether or not the next object exists.
     * This method may change previously {@link #getObject()} result objects.
     * @throws IOException if I/O error occurred while reading the next object
     * @throws InterruptedException if interrupted while reading the next object
     * @return {@code true} if the next object exists, otherwise {@code false}
     */
    @Override
    boolean nextObject() throws IOException, InterruptedException;

    /**
     * Returns an object which is on the current cursor position.
     * @return the current object
     * @throws IllegalStateException if the cursor is not on the current object
     * @throws IOException if I/O error occurred while reading the current object
     * @throws InterruptedException if interrupted while reading the current object
     * @see #nextObject()
     */
    @Override
    Object getObject() throws IOException, InterruptedException;

    /**
     * An abstract group key information.
     * @since 0.4.0
     */
    interface GroupInfo extends Comparable<GroupInfo> {

        /**
         * Returns the group value.
         * @return the group value
         * @throws IOException if I/O error was occurred while reading the value
         * @throws InterruptedException if interrupted while reading the value
         * @throws UnsupportedOperationException if this does not provide any values
         */
        Object getValue() throws IOException, InterruptedException;
    }
}
