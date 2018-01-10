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
package com.asakusafw.dag.api.counter.basic;

import java.text.MessageFormat;
import java.util.List;
import java.util.function.Supplier;

import com.asakusafw.dag.api.counter.CounterGroup;
import com.asakusafw.dag.api.counter.CounterGroup.Scope;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * A basic implementation of {@link com.asakusafw.dag.api.counter.CounterGroup.Category}.
 * @param <T> the type of target {@link CounterGroup}
 * @since 0.4.0
 */
public class BasicCounterGroupCategory<T extends CounterGroup> implements CounterGroup.Category<T> {

    private final String description;

    private final Scope scope;

    private final List<CounterGroup.Column> columns;

    private final String indexText;

    private final Supplier<? extends T> supplier;

    /**
     * Creates a new instance.
     * @param description the description of the target {@link CounterGroup}
     * @param scope the scope of the target {@link CounterGroup}
     * @param columns the available columns in the {@link CounterGroup}
     * @param provider a supplier which provides the target {@link CounterGroup}
     */
    public BasicCounterGroupCategory(
            String description,
            Scope scope,
            List<? extends CounterGroup.Column> columns,
            Supplier<? extends T> provider) {
        this(description, scope, columns, String.format("basic.%s", description), provider); //$NON-NLS-1$
    }

    /**
     * Creates a new instance.
     * @param description the description of the target {@link CounterGroup}
     * @param scope the scope of the target {@link CounterGroup}
     * @param columns the available columns in the {@link CounterGroup}
     * @param indexText the index text
     * @param provider a supplier which provides the target {@link CounterGroup}
     */
    public BasicCounterGroupCategory(
            String description,
            Scope scope,
            List<? extends CounterGroup.Column> columns,
            String indexText,
            Supplier<? extends T> provider) {
        Arguments.requireNonNull(description);
        Arguments.requireNonNull(scope);
        Arguments.requireNonNull(columns);
        Arguments.requireNonNull(indexText);
        Arguments.requireNonNull(provider);
        this.description = description;
        this.scope = scope;
        this.columns = Arguments.freeze(columns);
        this.indexText = indexText;
        this.supplier = provider;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public Scope getScope() {
        return scope;
    }

    @Override
    public List<CounterGroup.Column> getColumns() {
        return columns;
    }

    @Override
    public T newInstance() {
        return supplier.get();
    }

    @Override
    public String getIndexText() {
        return indexText;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "CounterGroup.Descriptor({0}: {1})",
                description,
                columns);
    }
}
