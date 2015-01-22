/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.api;

/**
 * Represents a set of Asakusa DSL compiler options.
 */
public class CompilerOptions {

    private final String runtimeWorkingDirectory;

    /**
     * Creates a new instance.
     * @param runtimeWorkingDirectory the runtime working directory
     */
    public CompilerOptions(String runtimeWorkingDirectory) {
        this.runtimeWorkingDirectory = runtimeWorkingDirectory;
    }

    /**
     * Returns the path of runtime working directory.
     * This may relative path, or may include variables (<code>${...}</code>).
     * @return the runtime working directory path (trailing {@code '/'} is removed)
     */
    public String getRuntimeWorkingDirectory() {
        return runtimeWorkingDirectory;
    }
}
