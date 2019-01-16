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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.extension.ProcessorContextDecorator;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * An implementation of {@link ProcessorContextDecorator} for enabling tracing edge I/Os.
 * @since 0.4.0
 */
public class TracingProcessorContextDecorator implements ProcessorContextDecorator {

    static final Logger LOG = LoggerFactory.getLogger(TracingProcessorContextDecorator.class);

    private final PortTracer tracer;

    /**
     * Creates a new instance.
     * @param tracer the tracer
     */
    public TracingProcessorContextDecorator(PortTracer tracer) {
        Arguments.requireNonNull(tracer);
        this.tracer = tracer;
    }

    @Override
    public VertexProcessorContext bless(VertexProcessorContext context) {
        Function<String, Consumer<Object>> sinks = getSinks(context.getVertexId());
        if (sinks == null) {
            return ProcessorContextDecorator.super.bless(context);
        } else {
            LOG.debug("enable tracing: vertex={}", context.getVertexId());
            return new TracingVertexProcessorContext(context, sinks);
        }
    }

    @Override
    public TaskProcessorContext bless(TaskProcessorContext context) {
        Function<String, Consumer<Object>> sinks = getSinks(context.getVertexId());
        if (sinks == null) {
            return ProcessorContextDecorator.super.bless(context);
        } else {
            LOG.debug("enable tracing: vertex={}, task={}", context.getVertexId(), context.getTaskId());
            return new TracingTaskProcessorContext(context, sinks);
        }
    }

    private Function<String, Consumer<Object>> getSinks(String vertexId) {
        if (tracer.isSupported(vertexId)) {
            return portId -> tracer.getSink(vertexId, portId);
        }
        return null;
    }
}
