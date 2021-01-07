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
package com.asakusafw.dag.runtime.jdbc.testing;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.value.ValueOption;

/**
 * An abstract implementation of {@link DataModel}.
 * @since 0.4.0
 * @param <TSelf> the self type
 */
public abstract class DataModelBase<TSelf extends DataModel<TSelf>> implements DataModel<TSelf> {

    /**
     * Returns the self.
     * @return the self
     */
    @SuppressWarnings("unchecked")
    protected TSelf self() {
        return (TSelf) this;
    }

    /**
     * Returns the property extractor functions.
     * @return the property extractor functions
     */
    protected abstract List<Function<TSelf, ? extends ValueOption<?>>> properties();

    @SuppressWarnings("deprecation")
    @Override
    public void reset() {
        TSelf self = self();
        properties().stream()
                .map(p -> p.apply(self))
                .forEach(ValueOption::setNull);
    }

    @SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
    @Override
    public final void copyFrom(TSelf other) {
        TSelf self = self();
        for (Function<TSelf, ? extends ValueOption<?>> p : properties()) {
            ValueOption from = p.apply(other);
            ValueOption to = p.apply(self);
            to.copyFrom(from);
        }
    }

    @Override
    public int hashCode() {
        TSelf self = self();
        return Lang.hashCode(properties().stream()
                .map(p -> p.apply(self))
                .toArray());
    }

    @Override
    public boolean equals(Object obj) {
        TSelf self = self();
        return Lang.equals(self, obj, properties());
    }

    @Override
    public String toString() {
        TSelf self = self();
        return properties().stream()
                .map(p -> p.apply(self))
                .map(String::valueOf)
                .collect(Collectors.joining(", ", "(", ")"));
    }
}
