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
package com.asakusafw.dag.runtime.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Extracts data from {@link ResultSet}.
 * @param <T> the data model type
 * @since 0.4.0
 */
@FunctionalInterface
public interface ResultSetAdapter<T> {

    /**
     * Extracts the current row of data, and maps them to an object.
     * The returned object may be changed after clients invokes this method once more.
     * @param row the source result set
     * @return the extracted object
     * @throws SQLException if error occurred while extracting object
     */
    T extract(ResultSet row) throws SQLException;
}
