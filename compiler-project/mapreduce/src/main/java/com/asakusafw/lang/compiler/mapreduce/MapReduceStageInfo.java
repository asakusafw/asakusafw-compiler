/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
 * Represents a MapReduce stage structure.
 */
public class MapReduceStageInfo {

    final StageInfo meta;

    final List<Input> inputs;

    final List<Output> outputs;

    final List<Resource> resources;

    final Shuffle shuffle;

    final String baseOutputPath;

    /**
     * Creates a new instance without shuffle operation.
     * @param meta meta information for the stage
     * @param inputs the stage input information
     * @param outputs the stage output information
     * @param resources the stage resource information
     * @param baseOutputPath the base output path
     */
    public MapReduceStageInfo(
            StageInfo meta,
            List<Input> inputs,
            List<Output> outputs,
            List<Resource> resources,
            String baseOutputPath) {
        this(meta, inputs, outputs, resources, null, baseOutputPath);
    }

    /**
     * Creates a new instance.
     * @param meta meta information for the stage
     * @param inputs the stage input information
     * @param outputs the stage output information
     * @param resources the stage resource information
     * @param shuffle the stage shuffle information
     * @param baseOutputPath the base output path
     */
    public MapReduceStageInfo(
            StageInfo meta,
            List<Input> inputs,
            List<Output> outputs,
            List<Resource> resources,
            Shuffle shuffle,
            String baseOutputPath) {
        this.meta = meta;
        this.inputs = Collections.unmodifiableList(new ArrayList<>(inputs));
        this.outputs = Collections.unmodifiableList(new ArrayList<>(outputs));
        this.resources = Collections.unmodifiableList(new ArrayList<>(resources));
        this.shuffle = shuffle;
        this.baseOutputPath = baseOutputPath;
    }

    /**
     * Represents an input for MapReduce stage.
     */
    public static class Input {

        final String path;

        final ClassDescription dataClass;

        final ClassDescription formatClass;

        final ClassDescription mapperClass;

        final Map<String, String> attributes;

        /**
         * Creates a new instance.
         * @param path the input path expression
         * @param dataClass the input data class
         * @param formatClass the input format class
         * @param mapperClass the mapper class
         * @param attributes the input attributes
         */
        public Input(
                String path,
                ClassDescription dataClass,
                ClassDescription formatClass,
                ClassDescription mapperClass,
                Map<String, String> attributes) {
            this.path = path;
            this.dataClass = dataClass;
            this.formatClass = formatClass;
            this.mapperClass = mapperClass;
            this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
        }
    }

    /**
     * Represents an output for MapReduce stage.
     */
    public static class Output {

        final String name;

        final ClassDescription keyClass;

        final ClassDescription valueClass;

        final ClassDescription formatClass;

        final Map<String, String> attributes;

        /**
         * Creates a new instance.
         * @param name the output name
         * @param keyClass the output key class
         * @param valueClass the output value class
         * @param formatClass the output format class
         * @param attributes the output attributes
         */
        public Output(
                String name,
                ClassDescription keyClass,
                ClassDescription valueClass,
                ClassDescription formatClass,
                Map<String, String> attributes) {
            this.name = name;
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.formatClass = formatClass;
            this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
        }
    }

    /**
     * Represents a broadcast resource for MapReduce stage.
     */
    public static class Resource {

        final String path;

        final String name;

        /**
         * Creates a new instance.
         * @param path the resource path
         * @param name the resource name
         */
        public Resource(String path, String name) {
            this.path = path;
            this.name = name;
        }
    }

    /**
     * Represents a shuffle operation for MapReduce stage.
     */
    public static class Shuffle {

        final ClassDescription keyClass;

        final ClassDescription valueClass;

        final ClassDescription partitionerClass;

        final ClassDescription combinerClass;

        final ClassDescription sortComparatorClass;

        final ClassDescription groupingComparatorClass;

        final ClassDescription reducerClass;

        /**
         * Creates a new instance.
         * @param keyClass the shuffle key class
         * @param valueClass the shuffle value class
         * @param partitionerClass the partitioner class
         * @param combinerClass the combiner class (optional)
         * @param sortComparatorClass the sort comparator class
         * @param groupingComparatorClass the grouping comparator class
         * @param reducerClass the reducer class
         */
        public Shuffle(
                ClassDescription keyClass, ClassDescription valueClass,
                ClassDescription partitionerClass, ClassDescription combinerClass,
                ClassDescription sortComparatorClass, ClassDescription groupingComparatorClass,
                ClassDescription reducerClass) {
            this.keyClass = keyClass;
            this.valueClass = valueClass;
            this.partitionerClass = partitionerClass;
            this.combinerClass = combinerClass;
            this.sortComparatorClass = sortComparatorClass;
            this.groupingComparatorClass = groupingComparatorClass;
            this.reducerClass = reducerClass;
        }
    }
}
