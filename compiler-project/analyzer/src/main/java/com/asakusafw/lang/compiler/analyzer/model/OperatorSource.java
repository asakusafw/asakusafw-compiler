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
package com.asakusafw.lang.compiler.analyzer.model;

import java.lang.reflect.AnnotatedElement;
import java.util.Objects;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;

/**
 * Represents origin of operators.
 * @since 0.3.0
 */
public class OperatorSource {

    private final Operator.OperatorKind operatorKind;

    private final AnnotatedElement origin;

    /**
     * Creates a new instance.
     * @param operatorKind the analyzing operator kind
     * @param origin the original element
     */
    public OperatorSource(OperatorKind operatorKind, AnnotatedElement origin) {
        Objects.requireNonNull(operatorKind);
        Objects.requireNonNull(origin);
        this.operatorKind = operatorKind;
        this.origin = origin;
    }

    /**
     * Returns the operator kind.
     * @return the operator kind
     */
    public Operator.OperatorKind getOperatorKind() {
        return operatorKind;
    }

    /**
     * Returns the original element.
     * @return the original element
     */
    public AnnotatedElement getOrigin() {
        return origin;
    }
}
