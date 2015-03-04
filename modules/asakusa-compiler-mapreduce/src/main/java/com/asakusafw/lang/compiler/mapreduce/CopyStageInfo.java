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
     * Computes the output path for this stage.
     * @param basePath the output base path
     * @param name the output operation name
     * @return the computed output path
     */
    public static String getOutputPath(String basePath, String name) {
        return MapReduceUtil.getStageOutputPath(basePath, name);
    }

    /**
     * Represents a copy operation.
     */
    public static class Operation {

        final String name;

        final SourceInfo source;

        final ClassDescription outputFormatClass;

        final Map<String, String> outputAttributes;

        /**
         * Creates a new instance.
         * @param name the operation name
         * @param source the source information
         * @param outputFormatClass the output format class
         * @param outputAttributes the output attributes (for the output format class)
         */
        public Operation(
                String name, SourceInfo source,
                ClassDescription outputFormatClass,
                Map<String, String> outputAttributes) {
            this.name = name;
            this.source = source;
            this.outputFormatClass = outputFormatClass;
            this.outputAttributes = Collections.unmodifiableMap(new LinkedHashMap<>(outputAttributes));
        }
    }
}
