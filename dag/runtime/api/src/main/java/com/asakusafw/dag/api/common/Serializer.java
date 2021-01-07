/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.dag.api.common;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Serializes objects.
 * @since 0.4.0
 */
@FunctionalInterface
public interface Serializer {

    /**
     * Writes an object into the {@link DataOutput}.
     * @param object the object
     * @param output the target output
     * @throws IOException if I/O error was occurred while writing the object
     * @throws InterruptedException if interrupted while writing the object
     */
    void serialize(Object object, DataOutput output) throws IOException, InterruptedException;
}
