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
package com.asakusafw.lang.compiler.hadoop;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Represents {@code InputFormat} information for individual inputs.
 */
public class InputFormatInfo {

    private final ClassDescription formatClass;

    private final ClassDescription keyClass;

    private final ClassDescription valueClass;

    private final Map<String, String> extraConfiguration;

    /**
     * Creates a new instance.
     * @param formatClass the target {@code InputFormat} class
     * @param keyClass the input key class
     * @param valueClass the input value class
     * @param extraConfiguration the extra entries for {@code Configuration} object
     */
    public InputFormatInfo(
            ClassDescription formatClass,
            ClassDescription keyClass,
            ClassDescription valueClass,
            Map<String, String> extraConfiguration) {
        this.formatClass = formatClass;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.extraConfiguration = Collections.unmodifiableMap(new LinkedHashMap<>(extraConfiguration));
    }

    /**
     * Returns the target {@code InputFormat} class for accessing the target input.
     * @return the target {@code InputFormat} class
     */
    public ClassDescription getFormatClass() {
        return formatClass;
    }

    /**
     * Returns the input key class.
     * @return the input key class
     */
    public ClassDescription getKeyClass() {
        return keyClass;
    }

    /**
     * Returns the input value class.
     * @return the input value class
     */
    public ClassDescription getValueClass() {
        return valueClass;
    }

    /**
     * Returns the extra entries for Hadoop {@code Configuration} object.
     * Clients should merge this to the current Hadoop {@code Configuration} for accessing the target input.
     * @return the extra configuration entries
     */
    public Map<String, String> getExtraConfiguration() {
        return extraConfiguration;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "{0}{1}", //$NON-NLS-1$
                getFormatClass().getSimpleName(),
                getExtraConfiguration().toString());
    }
}
