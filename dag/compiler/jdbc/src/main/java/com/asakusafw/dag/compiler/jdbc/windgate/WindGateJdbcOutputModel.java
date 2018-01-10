/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.jdbc.windgate;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * Represents a WindGate JDBC input model.
 * @since 0.4.0
 */
public class WindGateJdbcOutputModel extends WindGateJdbcModel {

    private String customTruncate;

    /**
     * Creates a new instance.
     * @param dataType the data type
     * @param profileName the profile name
     * @param tableName the table name
     * @param columnMappings the column mappings
     */
    public WindGateJdbcOutputModel(
            TypeDescription dataType,
            String profileName,
            String tableName,
            List<Tuple<String, PropertyName>> columnMappings) {
        super(dataType, profileName, tableName, columnMappings);
    }

    /**
     * Returns the custom truncate statement.
     * @return the custom truncate statement, or {@code null} if it is not defined
     */
    public String getCustomTruncate() {
        return customTruncate;
    }

    /**
     * Sets a the custom truncate statement.
     * @param newValue the new value
     * @return this
     */
    public WindGateJdbcOutputModel withCustomTruncate(String newValue) {
        this.customTruncate = newValue;
        return this;
    }

    /**
     * Sets the options.
     * @param newValues the options
     * @return this
     */
    public WindGateJdbcOutputModel withOptions(String... newValues) {
        Arguments.requireNonNull(newValues);
        return withOptions(Arrays.asList(newValues));
    }

    /**
     * Sets the options.
     * @param newValues the options
     * @return this
     */
    public WindGateJdbcOutputModel withOptions(Collection<String> newValues) {
        Arguments.requireNonNull(newValues);
        setOptions(newValues);
        return this;
    }
}
