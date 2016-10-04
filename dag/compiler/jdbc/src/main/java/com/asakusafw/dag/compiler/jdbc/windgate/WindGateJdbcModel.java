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
package com.asakusafw.dag.compiler.jdbc.windgate;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * An abstract implementation of WindGate JDBC I/O models.
 * @since 0.4.0
 */
public abstract class WindGateJdbcModel {

    private final TypeDescription dataType;

    private final String profileName;

    private final String tableName;

    private final List<Tuple<String, PropertyName>> columnMappings;

    private Set<String> options = new LinkedHashSet<>();

    /**
     * Creates a new instance.
     * @param dataType the data type
     * @param profileName the profile name
     * @param tableName the table name
     * @param columnMappings the column mappings
     */
    public WindGateJdbcModel(
            TypeDescription dataType,
            String profileName,
            String tableName,
            List<Tuple<String, PropertyName>> columnMappings) {
        Arguments.requireNonNull(dataType);
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(tableName);
        Arguments.requireNonNull(columnMappings);
        this.dataType = dataType;
        this.profileName = profileName;
        this.tableName = tableName;
        this.columnMappings = Arguments.freeze(columnMappings);
    }

    /**
     * Returns the data type.
     * @return the data type
     */
    public TypeDescription getDataType() {
        return dataType;
    }

    /**
     * Returns the profile name.
     * @return the profile name
     */
    public String getProfileName() {
        return profileName;
    }

    /**
     * Returns the table name.
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the column-property mappings.
     * @return the column-property mappings
     */
    public List<Tuple<String, PropertyName>> getColumnMappings() {
        return columnMappings;
    }

    /**
     * Returns the WindGate options.
     * @return the WindGate options
     */
    public Set<String> getOptions() {
        return options;
    }

    /**
     * Sets the WindGate options.
     * @param values the values
     */
    protected void setOptions(Collection<String> values) {
        this.options = Arguments.freezeToSet(values);
    }
}
