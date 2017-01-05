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

import java.util.List;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Represents an aggregate node.
 * @since 0.4.0
 */
public class AggregateNode implements ClassNode, DataNode {

    private final ClassDescription implementationType;

    private final TypeDescription runtimeType;

    private final ClassDescription mapperType;

    private final ClassDescription copierType;

    private final ClassDescription combinerType;

    private final TypeDescription inputType;

    private final TypeDescription outputType;

    private final List<VertexElement> dependencies;

    /**
     * Creates a new instance.
     * @param implementationType the implementation type
     * @param runtimeType the runtime type
     * @param mapperType the mapper type (nullable)
     * @param copierType the copier type (nullable)
     * @param combinerType the combiner type (nullable)
     * @param inputType the input type
     * @param outputType the output type
     * @param dependencies the required dependencies
     */
    public AggregateNode(
            ClassDescription implementationType,
            TypeDescription runtimeType,
            ClassDescription mapperType,
            ClassDescription copierType,
            ClassDescription combinerType,
            TypeDescription inputType,
            TypeDescription outputType,
            List<? extends VertexElement> dependencies) {
        Arguments.requireNonNull(implementationType);
        Arguments.requireNonNull(runtimeType);
        Arguments.requireNonNull(inputType);
        Arguments.requireNonNull(outputType);
        Arguments.requireNonNull(dependencies);
        this.implementationType = implementationType;
        this.runtimeType = runtimeType;
        this.mapperType = mapperType;
        this.copierType = copierType;
        this.combinerType = combinerType;
        this.inputType = inputType;
        this.outputType = outputType;
        this.dependencies = Arguments.freeze(dependencies);
    }

    @Override
    public ElementKind getElementKind() {
        return ElementKind.AGGREGATE;
    }

    @Override
    public ClassDescription getImplementationType() {
        return implementationType;
    }

    @Override
    public TypeDescription getRuntimeType() {
        return runtimeType;
    }

    @Override
    public TypeDescription getDataType() {
        return outputType;
    }

    /**
     * Returns the mapper type.
     * @return the mapper type
     */
    public ClassDescription getMapperType() {
        return mapperType;
    }

    /**
     * Returns the copier type.
     * @return the copier type
     */
    public ClassDescription getCopierType() {
        return copierType;
    }

    /**
     * Returns the combiner type.
     * @return the combiner type
     */
    public ClassDescription getCombinerType() {
        return combinerType;
    }

    /**
     * Returns the input type.
     * @return the input type
     */
    public TypeDescription getInputType() {
        return inputType;
    }

    /**
     * Returns the output type.
     * @return the output type
     */
    public TypeDescription getOutputType() {
        return outputType;
    }

    @Override
    public List<? extends VertexElement> getDependencies() {
        return dependencies;
    }

    @Override
    public String toString() {
        return String.format("Aggregate:%s(%s)", //$NON-NLS-1$
                getImplementationType().getClassName(),
                getDataType());
    }
}
