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
package com.asakusafw.lang.compiler.extension.directio.emitter;

import java.util.List;

import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.extension.directio.OutputPattern;
import com.asakusafw.lang.compiler.extension.externalio.ExternalPortStageInfo;
import com.asakusafw.lang.compiler.mapreduce.SourceInfo;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.runtime.directio.FilePattern;

/**
 * Represents a Direct I/O outputs.
 */
public class OutputStageInfo {

    final ExternalPortStageInfo meta;

    final List<Operation> operations;

    final String baseOutputPath;

    /**
     * Creates a new instance.
     * @param meta the meta information
     * @param operations each output operation
     * @param baseOutputPath the stage base output path
     */
    public OutputStageInfo(ExternalPortStageInfo meta, List<Operation> operations, String baseOutputPath) {
        this.meta = meta;
        this.operations = operations;
        this.baseOutputPath = baseOutputPath;
    }

    /**
     * Represents an operation for each output.
     */
    public static class Operation {

        final String name;

        final DataModelReference dataModel;

        final List<SourceInfo> sources;

        final String basePath;

        final OutputPattern resourcePattern;

        final List<FilePattern> deletePatterns;

        final ClassDescription dataFormatClass;

        /**
         * Creates a new instance.
         * @param name the name of this operation
         * @param dataModel the target data model
         * @param sources source information
         * @param basePath target base path
         * @param resourcePattern the output pattern
         * @param dataFormatClass {@link DataFormat} class name
         * @param deletePatterns delete file patterns
         */
        public Operation(
                String name,
                DataModelReference dataModel,
                List<SourceInfo> sources,
                String basePath,
                OutputPattern resourcePattern,
                List<FilePattern> deletePatterns,
                ClassDescription dataFormatClass) {
            this.name = name;
            this.dataModel = dataModel;
            this.sources = sources;
            this.basePath = basePath;
            this.resourcePattern = resourcePattern;
            this.deletePatterns = deletePatterns;
            this.dataFormatClass = dataFormatClass;
        }
    }
}
