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
package com.asakusafw.dag.runtime.jdbc;

import java.io.IOException;
import java.sql.Connection;

/**
 * Processes operations via JDBC.
 * @since 0.4.0
 */
@FunctionalInterface
public interface JdbcOperationDriver {

    /**
     * Performs this operation.
     * This never commits the current transaction.
     * @param connection the shared JDBC connection
     * @throws IOException if I/O error was occurred while performing the operation
     * @throws InterruptedException if interrupted while performing the operation
     */
    void perform(Connection connection) throws IOException, InterruptedException;
}
