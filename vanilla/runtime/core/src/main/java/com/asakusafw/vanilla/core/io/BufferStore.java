/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
import java.nio.ByteBuffer;

/**
 * Represents a buffer store.
 * @since 0.4.0
 */
@FunctionalInterface
public interface BufferStore {

    /**
     * Stores the given buffer into this store.
     * @param buffer the target buffer
     * @return the stored entry
     * @throws IOException if I/O error was occurred while saving the buffer
     * @throws InterruptedException if interrupted while saving the buffer
     */
    DataReader.Provider store(ByteBuffer buffer) throws IOException, InterruptedException;
}
