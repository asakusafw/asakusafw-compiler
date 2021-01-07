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
package com.asakusafw.lang.compiler.mapreduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Represents a copy stage structure.
 * @see CopyStageEmitter
 */
public class CopyStageInfo {

    final StageInfo meta;

    final List<Operation> operations;

    final String baseOutputPath;

    /**
     * Creates a new instance.
     * @param meta meta information for the stage
     * @param operations the copy operations
     * @param baseOutputPath the base output path
     */
    public CopyStageInfo(StageInfo meta, List<Operation> operations, String baseOutputPath) {
        this.meta = meta;
        this.operations = Collections.unmodifiableList(new ArrayList<>(operations));
        this.baseOutputPath = baseOutputPath;
    }

    /**
     * Represents a copy operation.
     */
    public static class Operation {

        final String outputName;

        final SourceInfo source;

        final ClassDescription outputFormatClass;

        final Map<String, String> outputAttributes;

        /**
         * Creates a new instance.
         * @param outputName the output name (must consist of alphabets and digits)
         * @param source the source information
         * @param outputFormatClass the output format class
         * @param outputAttributes the output attributes (for the output format class)
         * @see MapReduceUtil#quoteOutputName(String)
         */
        public Operation(
                String outputName,
                SourceInfo source,
                ClassDescription outputFormatClass,
                Map<String, String> outputAttributes) {
            this.outputName = outputName;
            this.source = source;
            this.outputFormatClass = outputFormatClass;
            this.outputAttributes = Collections.unmodifiableMap(new LinkedHashMap<>(outputAttributes));
        }
    }
}
