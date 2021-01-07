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
package com.asakusafw.dag.api.common;

import java.io.IOException;

/**
 * An abstract super interface of receiving objects.
 * @since 0.4.0
 */
@FunctionalInterface
public interface ObjectSink {

    /**
     * Puts an object into this sink.
     * @param object the object
     * @throws IOException if I/O error occurred while processing the object
     * @throws InterruptedException if interrupted while processing the object
     */
    void putObject(Object object) throws IOException, InterruptedException;
}
