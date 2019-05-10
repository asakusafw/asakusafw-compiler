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
package com.asakusafw.vanilla.core.io;

import java.io.IOException;

/**
 * Provides binary large object.
 * @since 0.5.3
 */
public interface BlobStore {

    /**
     * Creates a new BLOB and returns a writer of it.
     * @return the created writer
     * @throws IOException if I/O error was occurred while preparing the BLOB
     * @throws InterruptedException if interrupted while preparing the BLOB
     * @see #commit(DataWriter)
     */
    DataWriter create() throws IOException, InterruptedException;

    /**
     * Commits a created BLOB data created by {@link #create()}.
     * This operation must be called BEFORE the given writer was closed,
     * and the writer will be disabled after this operation.
     * @param writer the BLOB writer
     * @return the BLOB data
     * @throws IOException if I/O error was occurred while saving the BLOB
     * @throws InterruptedException if interrupted while saving the BLOB
     */
    DataReader.Provider commit(DataWriter writer) throws IOException, InterruptedException;
}
