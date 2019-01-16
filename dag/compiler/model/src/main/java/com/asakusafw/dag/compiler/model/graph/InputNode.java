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

import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.model.description.BasicTypeDescription;
import com.asakusafw.lang.compiler.model.description.BasicTypeDescription.BasicTypeKind;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Represents an input node.
 * @since 0.4.0
 */
public class InputNode implements VertexElement {

    private final VertexElement consumer;

    /**
     * Creates a new instance.
     * @param consumer the input consumer
     */
    public InputNode(VertexElement consumer) {
        Arguments.requireNonNull(consumer);
        this.consumer = consumer;
    }

    @Override
    public ElementKind getElementKind() {
        return ElementKind.INPUT;
    }

    /**
     * Returns the input consumer.
     * @return the consumer
     */
    public VertexElement getConsumer() {
        return consumer;
    }

    @Override
    public TypeDescription getRuntimeType() {
        return new BasicTypeDescription(BasicTypeKind.VOID);
    }

    @Override
    public List<? extends VertexElement> getDependencies() {
        return Collections.singletonList(consumer);
    }

    @Override
    public String toString() {
        return "Input"; //$NON-NLS-1$
    }
}
