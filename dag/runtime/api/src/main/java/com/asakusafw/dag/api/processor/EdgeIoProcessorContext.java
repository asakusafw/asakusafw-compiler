/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.dag.api.processor;

import java.io.IOException;

/**
 * An abstract super interface of processor which performs edge I/Os.
 * @since 0.4.0
 */
public interface EdgeIoProcessorContext extends ProcessorContext {

    /**
     * Returns a reader for the specified input port.
     * @param name the port name
     * @return the corresponded reader
     * @throws IOException if I/O error was occurred while initializing the reader
     * @throws InterruptedException if interrupted while initializing the reader
     */
    EdgeReader getInput(String name) throws IOException, InterruptedException;

    /**
     * Returns a writer for the specified input port.
     * @param name the port name
     * @return the corresponded writer
     * @throws IOException if I/O error was occurred while initializing the writer
     * @throws InterruptedException if interrupted while initializing the writer
     */
    EdgeWriter getOutput(String name) throws IOException, InterruptedException;
}
