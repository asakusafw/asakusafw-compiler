/**
 * Copyright 2011-2019 Asakusa Framework Team.
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

import java.util.Arrays;
import java.util.List;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Represents an operator node.
 * @since 0.4.0
 */
public class OperatorNode implements ClassNode, DataNode {

    private final ClassDescription implementationType;

    private final TypeDescription runtimeType;

    private final TypeDescription dataType;

    private final List<VertexElement> dependencies;

    /**
     * Creates a new instance.
     * @param implementationType the implementation type
     * @param runtimeType the runtime type
     * @param dataType the input data type
     * @param dependencies the required dependencies
     */
    public OperatorNode(
            ClassDescription implementationType,
            TypeDescription runtimeType,
            TypeDescription dataType,
            VertexElement... dependencies) {
        this(implementationType, runtimeType, dataType, Arrays.asList(Arguments.requireNonNull(dependencies)));
    }

    /**
     * Creates a new instance.
     * @param implementationType the implementation type
     * @param runtimeType the runtime type
     * @param dataType the input data type
     * @param dependencies the required dependencies
     */
    public OperatorNode(
            ClassDescription implementationType,
            TypeDescription runtimeType,
            TypeDescription dataType,
            List<? extends VertexElement> dependencies) {
        Arguments.requireNonNull(implementationType);
        Arguments.requireNonNull(runtimeType);
        Arguments.requireNonNull(dataType);
        Arguments.requireNonNull(dependencies);
        this.implementationType = implementationType;
        this.runtimeType = runtimeType;
        this.dataType = dataType;
        this.dependencies = Arguments.freeze(dependencies);
    }

    @Override
    public final ElementKind getElementKind() {
        return ElementKind.OPERATOR;
    }

    @Override
    public ClassDescription getImplementationType() {
        return implementationType;
    }

    @Override
    public TypeDescription getDataType() {
        return dataType;
    }

    @Override
    public TypeDescription getRuntimeType() {
        return runtimeType;
    }

    @Override
    public List<? extends VertexElement> getDependencies() {
        return dependencies;
    }

    @Override
    public String toString() {
        return String.format("Operator:%s(%s)", //$NON-NLS-1$
                getImplementationType().getClassName(),
                getDataType());
    }
}
