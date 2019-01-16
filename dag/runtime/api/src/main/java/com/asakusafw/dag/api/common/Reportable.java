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
package com.asakusafw.dag.api.common;

import java.io.IOException;

/**
 * Represents a reporter.
 * @since 0.4.0
 */
@FunctionalInterface
public interface Reportable {

    /**
     * Generates a report.
     * @throws IOException if I/O error was occurred while generating the report
     * @throws InterruptedException if interrupted while generating the report
     */
    void report() throws IOException, InterruptedException;
}
