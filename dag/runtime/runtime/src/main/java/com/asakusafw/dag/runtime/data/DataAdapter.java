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
package com.asakusafw.dag.runtime.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An abstract super interface of data adapters.
 * @param <T> the data type
 * @since 0.4.1
 */
public interface DataAdapter<T> {

    /**
     * Creates a new data object.
     * @return the created object
     */
    T create();

    /**
     * Copies the given object into the another object.
     * @param source the source object
     * @param destination the destination object
     */
    void copy(T source, T destination);

    /**
     * Writes the given object into the output.
     * @param source the source object
     * @param output the target output
     * @throws IOException if I/O error was occurred while writing the object
     */
    void write(T source, DataOutput output) throws IOException;

    /**
     * Reads an object from the given input.
     * @param input the source input
     * @param destination the destination object
     * @throws IOException if I/O error was occurred while reading the object
     */
    void read(DataInput input, T destination) throws IOException;
}
