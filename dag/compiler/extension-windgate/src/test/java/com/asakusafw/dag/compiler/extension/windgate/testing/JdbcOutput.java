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
package com.asakusafw.dag.compiler.extension.windgate.testing;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.vocabulary.windgate.JdbcExporterDescription;
import com.asakusafw.windgate.core.vocabulary.DataModelJdbcSupport;

/**
 * Mock implementation of {@link JdbcExporterDescription}.
 */
public class JdbcOutput extends JdbcExporterDescription {

    private final Class<?> modelType;

    private final String profileName;

    private final String tableName;

    private final List<String> columnNames;

    private final Class<? extends DataModelJdbcSupport<?>> jdbcSupport;

    private String customTruncate;

    private Set<Option> options = Collections.emptySet();

    /**
     * Creates a new instance.
     * @param modelType the data model type
     * @param profileName the profile name
     * @param tableName the table name
     * @param columnNames the column names
     * @param jdbcSupport the JDBC support class
     */
    public JdbcOutput(
            Class<?> modelType,
            String profileName,
            String tableName,
            List<String> columnNames,
            Class<? extends DataModelJdbcSupport<?>> jdbcSupport) {
        this.modelType = modelType;
        this.profileName = profileName;
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.jdbcSupport = jdbcSupport;
    }

    @Override
    public Class<?> getModelType() {
        return modelType;
    }

    @Override
    public String getProfileName() {
        return profileName;
    }

    @Override
    public Class<? extends DataModelJdbcSupport<?>> getJdbcSupport() {
        return jdbcSupport;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public List<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public String getCustomTruncate() {
        return customTruncate;
    }

    @Override
    public Set<Option> getOptions() {
        return options;
    }

    /**
     * Sets a new value for {@link #getCustomTruncate()}.
     * @param newValue the value to set
     * @return this
     */
    public JdbcOutput withCustomTruncate(String newValue) {
        this.customTruncate = newValue;
        return this;
    }

    /**
     * Sets a new value for {@link #getOptions()}.
     * @param newValue the value to set
     * @return this
     */
    public JdbcOutput setOptions(Collection<Option> newValue) {
        this.options = Arguments.freezeToSet(newValue);
        return this;
    }
}
