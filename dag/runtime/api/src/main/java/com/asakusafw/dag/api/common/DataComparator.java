/**
 * Copyright 2011-2017 Asakusa Framework Team.
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

import java.io.DataInput;
import java.io.IOException;

/**
 * Compares serialized data sequences.
 * @since 0.4.0
 */
@FunctionalInterface
public interface DataComparator {

    /**
     * Compares between the two serialized data sequences in each given {@link DataInput}.
     * @param a the first {@link DataInput}
     * @param b the second {@link DataInput}
     * @return {@code 0} - the two values are both equivalent,
     *   {@code < 0} - the first value is less than the second one, or
     *   {@code > 0} - the second value is less than the second one
     * @throws IOException if I/O error was occurred while comparing the values
     */
    int compare(DataInput a, DataInput b) throws IOException;
}
