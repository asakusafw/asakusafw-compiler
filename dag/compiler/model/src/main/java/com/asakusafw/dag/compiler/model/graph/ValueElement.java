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
package com.asakusafw.dag.compiler.model.graph;

import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Represents a value element.
 * @since 0.4.0
 */
public class ValueElement implements VertexElement {

    private final ValueDescription value;

    private final TypeDescription runtimeType;

    /**
     * Creates a new instance.
     * @param value the target value
     */
    public ValueElement(ValueDescription value) {
        Arguments.requireNonNull(value);
        this.value = value;
        this.runtimeType = value.getValueType();
    }

    /**
     * Creates a new instance.
     * @param value the target value
     * @param type the runtime type
     */
    public ValueElement(ValueDescription value, TypeDescription type) {
        Arguments.requireNonNull(value);
        Arguments.requireNonNull(type);
        this.value = value;
        this.runtimeType = type;
    }

    @Override
    public ElementKind getElementKind() {
        return ElementKind.VALUE;
    }

    /**
     * Returns the value.
     * @return the value
     */
    public ValueDescription getValue() {
        return value;
    }

    @Override
    public TypeDescription getRuntimeType() {
        return runtimeType;
    }

    @Override
    public List<? extends VertexElement> getDependencies() {
        return Collections.emptyList();
    }
    @Override
    public String toString() {
        return String.format("Value:%s", //$NON-NLS-1$
                getValue());
    }
}
