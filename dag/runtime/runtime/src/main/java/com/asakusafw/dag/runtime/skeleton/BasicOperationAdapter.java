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
package com.asakusafw.dag.runtime.skeleton;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.runtime.adapter.Operation;
import com.asakusafw.dag.runtime.adapter.OperationAdapter;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * Basic implementation of {@link OperationAdapter}.
 * @param <T> the input type
 * @since 0.4.0
 */
public class BasicOperationAdapter<T> implements OperationAdapter<T> {

    private final AtomicReference<Function<Context, ? extends Operation<? super T>>> entity = new AtomicReference<>();

    /**
     * Creates a new instance.
     * @param context the current context
     */
    public BasicOperationAdapter(VertexProcessorContext context) {
        Arguments.requireNonNull(context);
    }

    /**
     * Binds the operation body.
     * @param operationClass the operation class, which constructor can accepts an
     *     {@link com.asakusafw.dag.runtime.adapter.OperationAdapter.Context OperationAdapter.Context} object
     * @return this
     */
    public BasicOperationAdapter<T> bind(Class<? extends Operation<? super T>> operationClass) {
        Arguments.requireNonNull(operationClass);
        Constructor<? extends Operation<? super T>> ctor =
                Invariants.safe(() -> operationClass.getConstructor(Context.class));
        return bind(c -> Invariants.safe(() -> ctor.newInstance(c)));
    }

    /**
     * Binds the operation body.
     * @param provider the operation body provider
     * @return this
     */
    public BasicOperationAdapter<T> bind(Function<Context, ? extends Operation<? super T>> provider) {
        Arguments.requireNonNull(provider);
        if (entity.compareAndSet(null, provider) == false) {
            throw new IllegalStateException();
        }
        return this;
    }

    @Override
    public void initialize() throws IOException, InterruptedException {
        Invariants.requireNonNull(entity.get());
    }

    @Override
    public Operation<? super T> newInstance(Context context) throws IOException, InterruptedException {
        Operation<? super T> result = entity.get().apply(context);
        return result;
    }
}
