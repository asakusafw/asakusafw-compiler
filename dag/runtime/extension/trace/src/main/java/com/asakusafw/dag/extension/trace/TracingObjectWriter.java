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
package com.asakusafw.dag.extension.trace;

import java.io.IOException;
import java.util.function.Consumer;

import com.asakusafw.dag.api.processor.ObjectWriter;

/**
 * An implementation of {@link ObjectWriter} which can tracing all objects through {@link #putObject(Object)} method.
 * @since 0.4.0
 */
public class TracingObjectWriter extends TracingObjectSink implements ObjectWriter {

    private final ObjectWriter delegate;

    /**
     * Creates a new instance.
     * @param delegate the delegate object
     * @param sink the trace sink
     */
    public TracingObjectWriter(ObjectWriter delegate, Consumer<Object> sink) {
        super(delegate, sink);
        this.delegate = delegate;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        delegate.close();
    }
}
