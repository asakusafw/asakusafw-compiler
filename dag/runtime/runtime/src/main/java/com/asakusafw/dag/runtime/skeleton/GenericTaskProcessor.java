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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;
import java.util.List;

import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.runtime.adapter.CompositeContextHandler;
import com.asakusafw.dag.runtime.adapter.ContextHandler;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.Operation;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Generic style {@link TaskProcessor}.
 * @param <T> the operation input type
 * @since 0.4.0
 */
public class GenericTaskProcessor<T> implements TaskProcessor {

    private final InputHandler<? extends T, ? super TaskProcessorContext> input;

    private final Operation<? super T> operation;

    private final ContextHandler<TaskProcessorContext> extraHandler;

    /**
     * Creates a new instance.
     * @param input the input handler
     * @param operation the main operation
     * @param handlers extra context handlers
     */
    public GenericTaskProcessor(
            InputHandler<? extends T, ? super TaskProcessorContext> input,
            Operation<? super T> operation,
            List<? extends ContextHandler<? super TaskProcessorContext>> handlers) {
        Arguments.requireNonNull(input);
        Arguments.requireNonNull(operation);
        Arguments.requireNonNull(handlers);
        this.input = input;
        this.operation = operation;
        this.extraHandler = CompositeContextHandler.of(handlers);
    }

    @Override
    public void run(TaskProcessorContext context) throws IOException, InterruptedException {
        try (InputHandler.InputSession<? extends T> in = input.start(context);
                ContextHandler.Session session = extraHandler.start(context)) {
            while (in.next()) {
                operation.process(in.get());
            }
        }
    }
}
