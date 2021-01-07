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
package com.asakusafw.dag.compiler.model.graph;

import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Represents an output node.
 * @since 0.4.0
 */
public class OutputNode implements DataNode {

    private final String id;

    private final TypeDescription runtimeType;

    private final TypeDescription dataType;

    /**
     * Creates a new instance.
     * @param id the output ID
     * @param runtimeType the runtime type
     * @param dataType the data type
     */
    public OutputNode(String id, TypeDescription runtimeType, TypeDescription dataType) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(runtimeType);
        Arguments.requireNonNull(dataType);
        this.id = id;
        this.runtimeType = runtimeType;
        this.dataType = dataType;
    }

    @Override
    public ElementKind getElementKind() {
        return ElementKind.OUTPUT;
    }

    /**
     * Returns the output ID.
     * @return the output ID
     */
    public String getId() {
        return id;
    }

    @Override
    public TypeDescription getRuntimeType() {
        return runtimeType;
    }

    @Override
    public TypeDescription getDataType() {
        return dataType;
    }

    @Override
    public List<? extends VertexElement> getDependencies() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return String.format("Output:(%s)->@%s", //$NON-NLS-1$
                getDataType(),
                getId());
    }
}
