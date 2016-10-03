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
package com.asakusafw.dag.extension.trace;

import java.io.IOException;
import java.util.function.Consumer;

import com.asakusafw.dag.api.common.ObjectCursor;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * An implementation of {@link ObjectCursor} which can tracing all objects through {@link #getObject()} method.
 * @since 0.4.0
 */
public class TracingObjectCursor implements ObjectCursor {

    private final ObjectCursor delegate;

    private final Consumer<Object> sink;

    private boolean first = true;

    /**
     * Creates a new instance.
     * @param delegate the delegate object
     * @param sink the trace sink
     */
    public TracingObjectCursor(ObjectCursor delegate, Consumer<Object> sink) {
        Arguments.requireNonNull(delegate);
        Arguments.requireNonNull(sink);
        this.delegate = delegate;
        this.sink = sink;
    }

    @Override
    public boolean nextObject() throws IOException, InterruptedException {
        if (delegate.nextObject()) {
            first = true;
            return true;
        }
        return false;
    }

    @Override
    public Object getObject() throws IOException, InterruptedException {
        Object result = delegate.getObject();
        if (first) {
            sink.accept(result);
            first = false;
        }
        return result;
    }
}
