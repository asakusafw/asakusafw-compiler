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
package com.asakusafw.dag.runtime.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Drives data into {@link PreparedStatement} as its parameters.
 * @param <T> the data model type
 * @since 0.4.0
 */
@FunctionalInterface
public interface PreparedStatementAdapter<T> {

    /**
     * Drives the object into the prepared statement as its parameters.
     * @param row the target statement
     * @param object the object to drive
     * @throws SQLException if error occurred while driving the object
     */
    void drive(PreparedStatement row, T object) throws SQLException;
}
