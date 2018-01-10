/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import com.asakusafw.dag.api.common.ObjectSink;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * An implementation of {@link ObjectSink} which can tracing all objects through {@link #putObject(Object)} method.
 * @since 0.4.0
 */
public class TracingObjectSink implements ObjectSink {

    private final ObjectSink delegate;

    private final Consumer<Object> sink;

    /**
     * Creates a new instance.
     * @param delegate the delegate object
     * @param sink the trace sink
     */
    public TracingObjectSink(ObjectSink delegate, Consumer<Object> sink) {
        Arguments.requireNonNull(delegate);
        Arguments.requireNonNull(sink);
        this.delegate = delegate;
        this.sink = sink;
    }

    @Override
    public void putObject(Object object) throws IOException, InterruptedException {
        sink.accept(object);
        delegate.putObject(object);
    }
}
