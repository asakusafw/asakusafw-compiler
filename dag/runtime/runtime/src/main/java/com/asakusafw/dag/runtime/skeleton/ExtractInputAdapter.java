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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import com.asakusafw.dag.api.processor.EdgeIoProcessorContext;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.dag.runtime.adapter.InputAdapter;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * {@link InputAdapter} for extract edge inputs.
 * @since 0.4.0
 */
public class ExtractInputAdapter implements InputAdapter<ExtractOperation.Input> {

    private final AtomicReference<String> input = new AtomicReference<>();

    /**
     * Creates a new instance.
     * @param context the context
     */
    public ExtractInputAdapter(VertexProcessorContext context) {
        Arguments.requireNonNull(context);
    }

    /**
     * Bind the input.
     * @param name the input name
     * @return this
     */
    public final ExtractInputAdapter bind(String name) {
        if (input.compareAndSet(null, name) == false) {
            throw new IllegalStateException();
        }
        return this;
    }

    @Override
    public InputHandler<ExtractOperation.Input, ? super EdgeIoProcessorContext> newHandler()
            throws IOException, InterruptedException {
        String name = input.get();
        Invariants.requireNonNull(name);
        return new ExtractInputHandler(name);
    }
}
