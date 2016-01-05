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
package com.asakusafw.lang.compiler.tester.executor;

import java.io.IOException;
import java.util.Map;

import com.asakusafw.lang.compiler.tester.TesterContext;

/**
 * Executes a compilation artifact for testing.
 * @param <T> the artifact type
 */
public interface ArtifactExecutor<T> {

    /**
     * Executes the artifact with empty batch arguments.
     * @param context the current tester context
     * @param artifact the target artifact
     * @throws InterruptedException if interrupted while executing the artifact
     * @throws IOException if failed to execute the target artifact
     */
    void execute(TesterContext context, T artifact) throws InterruptedException, IOException;

    /**
     * Executes the artifact with empty batch arguments.
     * @param context the current tester context
     * @param artifact the target artifact
     * @param arguments the batch arguments
     * @throws InterruptedException if interrupted while executing the artifact
     * @throws IOException if failed to execute the target artifact
     */
    void execute(
            TesterContext context, T artifact,
            Map<String, String> arguments) throws InterruptedException, IOException;
}
