/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.optimizer.basic;

import java.util.Map;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacteristics;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizer;

/**
 * Composition of {@link OperatorCharacterizer}.
 * @param <T> the characteristics type
 */
public final class CompositeOperatorCharacterizer<T extends OperatorCharacteristics>
        extends AbstractCompositeEngine<OperatorCharacterizer<? extends T>>
        implements OperatorCharacterizer<T> {

    CompositeOperatorCharacterizer(
            OperatorCharacterizer<? extends T> defaultElement,
            Map<OperatorKind, OperatorCharacterizer<? extends T>> kinds,
            Map<String, OperatorCharacterizer<? extends T>> inputs,
            Map<String, OperatorCharacterizer<? extends T>> outputs,
            Map<CoreOperatorKind, OperatorCharacterizer<? extends T>> cores,
            Map<ClassDescription, OperatorCharacterizer<? extends T>> users,
            Map<String, OperatorCharacterizer<? extends T>> customs) {
        super(safe(defaultElement), kinds, inputs, outputs, cores, users, customs);
    }

    private static <T extends OperatorCharacteristics> OperatorCharacterizer<? extends T> safe(
            OperatorCharacterizer<? extends T> element) {
        if (element == null) {
            return new UnsupportedOperatorCharacterizer<>();
        }
        return element;
    }

    /**
     * Creates a new builder for this class.
     * @return the created builder
     * @param <T> the characteristics type
     */
    public static <T extends OperatorCharacteristics> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public T extract(OperatorCharacterizer.Context context, Operator operator) {
        OperatorCharacterizer<? extends T> delegate = getElement(operator);
        assert delegate != null;
        return delegate.extract(context, operator);
    }

    /**
     * Builder for {@link CompositeOperatorCharacterizer}.
     * @param <T> the characteristics type
     */
    public static class Builder<T extends OperatorCharacteristics>
            extends AbstractBuilder<Builder<T>, OperatorCharacterizer<? extends T>> {

        @Override
        protected OperatorCharacterizer<? extends T> doBuild(
                OperatorCharacterizer<? extends T> defaultElement,
                Map<OperatorKind, OperatorCharacterizer<? extends T>> kindElements,
                Map<String, OperatorCharacterizer<? extends T>> inputElements,
                Map<String, OperatorCharacterizer<? extends T>> outputElements,
                Map<CoreOperatorKind, OperatorCharacterizer<? extends T>> coreElements,
                Map<ClassDescription, OperatorCharacterizer<? extends T>> userElements,
                Map<String, OperatorCharacterizer<? extends T>> customElements) {
            return new CompositeOperatorCharacterizer<>(
                    defaultElement, kindElements,
                    inputElements, outputElements,
                    coreElements, userElements, customElements);
        }
    }
}
