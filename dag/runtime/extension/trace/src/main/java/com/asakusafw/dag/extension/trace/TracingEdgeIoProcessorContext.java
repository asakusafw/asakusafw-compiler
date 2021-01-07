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
package com.asakusafw.dag.extension.trace;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

import com.asakusafw.dag.api.processor.EdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.EdgeReader;
import com.asakusafw.dag.api.processor.EdgeWriter;
import com.asakusafw.dag.api.processor.GroupReader;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.dag.api.processor.basic.ForwardEdgeIoProcessorContext;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * An abstract implementation of {@link EdgeIoProcessorContext} which enables tracing I/O.
 * @since 0.4.0
 */
public abstract class TracingEdgeIoProcessorContext implements ForwardEdgeIoProcessorContext {

    private final Function<String, Consumer<Object>> sinks;

    /**
     * Creates a new instance.
     * @param sinks the trace sinks provider for each input/output name
     */
    public TracingEdgeIoProcessorContext(Function<String, Consumer<Object>> sinks) {
        Arguments.requireNonNull(sinks);
        this.sinks = sinks;
    }

    @Override
    public EdgeReader getInput(String name) throws IOException, InterruptedException {
        Consumer<Object> sink = sinks.apply(name);
        EdgeReader forward = ForwardEdgeIoProcessorContext.super.getInput(name);
        if (sink != null) {
            if (forward instanceof ObjectReader) {
                return new TracingObjectReader((ObjectReader) forward, sink);
            } else if (forward instanceof GroupReader) {
                return new TracingGroupReader((GroupReader) forward, sink);
            }
        }
        return forward;
    }

    @Override
    public EdgeWriter getOutput(String name) throws IOException, InterruptedException {
        Consumer<Object> sink = sinks.apply(name);
        EdgeWriter forward = ForwardEdgeIoProcessorContext.super.getOutput(name);
        if (sink != null) {
            if (forward instanceof ObjectWriter) {
                return new TracingObjectWriter((ObjectWriter) forward, sink);
            }
        }
        return forward;
    }
}
