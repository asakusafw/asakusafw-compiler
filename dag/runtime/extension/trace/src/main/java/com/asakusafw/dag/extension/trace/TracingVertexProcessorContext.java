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

import java.util.function.Consumer;
import java.util.function.Function;

import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.ForwardVertexProcessorContext;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * An implementation of {@link VertexProcessorContext} which enables tracing I/O.
 * @since 0.4.0
 */
public class TracingVertexProcessorContext extends TracingEdgeIoProcessorContext
        implements ForwardVertexProcessorContext {

    private final VertexProcessorContext delegate;

    /**
     * Creates a new instance.
     * @param delegate the delegate object
     * @param sinks the trace sinks provider for each input/output name
     */
    public TracingVertexProcessorContext(VertexProcessorContext delegate, Function<String, Consumer<Object>> sinks) {
        super(sinks);
        Arguments.requireNonNull(delegate);
        this.delegate = delegate;
    }

    @Override
    public VertexProcessorContext getForward() {
        return delegate;
    }
}
