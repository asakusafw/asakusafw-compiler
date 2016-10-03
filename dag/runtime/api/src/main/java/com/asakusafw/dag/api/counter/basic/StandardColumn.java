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
package com.asakusafw.dag.api.counter.basic;

import com.asakusafw.dag.api.counter.CounterGroup;

/**
 * Represents standard columns of {@link CounterGroup}.
 * @since 0.4.0
 */
public enum StandardColumn implements CounterGroup.Column {

    /**
     * The number of input records.
     */
    INPUT_RECORD("number of input records"),

    /**
     * The number of input groups.
     */
    INPUT_GROUP("number of input groups"),

    /**
     * The number of output records.
     */
    OUTPUT_RECORD("number of output records"),

    /**
     * The input data size in bytes.
     */
    INPUT_DATA_SIZE("input data size in bytes"),

    /**
     * The output data size in bytes.
     */
    OUTPUT_DATA_SIZE("output data size in bytes"),

    /**
     * The input data size in bytes.
     */
    INPUT_FILE_SIZE("input file size in bytes"),

    /**
     * The output data size in bytes.
     */
    OUTPUT_FILE_SIZE("output file size in bytes"),
    ;

    private final String description;

    StandardColumn(String description) {
        this.description = description;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getIndexText() {
        return String.format("STANDARD.%04d", ordinal()); //$NON-NLS-1$
    }
}
