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
package com.asakusafw.lang.info;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a batch parameter definition.
 * @since 0.4.1
 */
public class ParameterInfo {

    private final String name;

    private final String comment;

    private final boolean mandatory;

    private final String pattern;

    /**
     * Creates a new instance.
     * @param name the parameter name
     * @param comment the comment (nullable)
     * @param mandatory {@code true} if this parameter is mandatory, otherwise {@code false}
     * @param pattern the parameter value pattern in regular expression (nullable)
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ParameterInfo(
            @JsonProperty("name") String name,
            @JsonProperty("comment") String comment,
            @JsonProperty("mandatory") boolean mandatory,
            @JsonProperty("pattern") String pattern) {
        this.name = name;
        this.comment = comment;
        this.mandatory = mandatory;
        this.pattern = pattern;
    }

    /**
     * Returns the name.
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the comment.
     * @return the comment, or {@code null} if it is not defined
     */
    public String getComment() {
        return comment;
    }

    /**
     * Returns whether or not this parameter is mandatory.
     * @return {@code true} if this parameter is mandatory, otherwise {@code false}
     */
    public boolean isMandatory() {
        return mandatory;
    }

    /**
     * Returns the parameter value pattern in regular expression.
     * @return the pattern, or {@code null} if it is not defined
     */
    public String getPattern() {
        return pattern;
    }

    @Override
    public String toString() {
        return String.format("parameter(name=%s)", name); //$NON-NLS-1$
    }
}
