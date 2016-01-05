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
package com.asakusafw.lang.compiler.mapreduce;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Represents a MapReduce input dataset.
 */
public class SourceInfo {

    private final Set<String> paths;

    private final ClassDescription dataClass;

    private final ClassDescription formatClass;

    private final Map<String, String> attributes;

    /**
     * Creates a new instance.
     * @param path the input file path
     * @param dataClass the input dataset type
     * @param formatClass the input format type
     * @param attributes the input attributes (for the input format class)
     */
    public SourceInfo(
            String path,
            ClassDescription dataClass,
            ClassDescription formatClass,
            Map<String, String> attributes) {
        this(Collections.singleton(path), dataClass, formatClass, attributes);
    }

    /**
     * Creates a new instance.
     * @param paths the input file paths
     * @param dataClass the input dataset type
     * @param formatClass the input format type
     * @param attributes the input attributes (for the input format class)
     */
    public SourceInfo(
            Set<String> paths,
            ClassDescription dataClass,
            ClassDescription formatClass,
            Map<String, String> attributes) {
        this.paths = Collections.unmodifiableSet(new LinkedHashSet<>(paths));
        this.dataClass = dataClass;
        this.formatClass = formatClass;
        this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
    }

    /**
     * Returns the input file paths.
     * @return the input file paths
     */
    public Set<String> getPaths() {
        return paths;
    }

    /**
     * Returns the input data class.
     * @return the input data class
     */
    public ClassDescription getDataClass() {
        return dataClass;
    }

    /**
     * Returns the input format class.
     * @return the input format class
     */
    public ClassDescription getFormatClass() {
        return formatClass;
    }

    /**
     * Returns the input attributes.
     * @return the input attributes
     */
    public Map<String, String> getAttributes() {
        return attributes;
    }
}
