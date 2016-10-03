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
package com.asakusafw.dag.runtime.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.model.DataModel;

/**
 * Mock {@link Result}.
 * @param <T> the acceptable type
 */
public class MockSink<T extends DataModel<T>> implements Result<T> {

    private final List<T> buffer = new ArrayList<>();

    private final UnaryOperator<T> operator;

    /**
     * Creates a new instance.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public MockSink() {
        this(v -> {
            DataModel copy = Lang.safe(() -> v.getClass().newInstance());
            copy.copyFrom(v);
            return (T) copy;
        });
    }

    /**
     * Creates a new instance.
     * @param operator the mapping operator
     */
    public MockSink(UnaryOperator<T> operator) {
        this.operator = operator;
    }

    @Override
    public void add(T result) {
        buffer.add(operator.apply(result));
    }

    /**
     * Returns the added elements.
     * @return the added elements
     */
    public List<T> get() {
        return buffer;
    }

    /**
     * Returns the added elements.
     * @param mapping the element mapper
     * @param <S> the result type
     * @return the added elements
     */
    public <S> List<S> get(Function<? super T, ? extends S> mapping) {
        return Lang.project(buffer, mapping);
    }
}
