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
package com.asakusafw.dag.compiler.model.graph;

import java.util.Collections;
import java.util.List;

import com.asakusafw.dag.api.processor.ProcessorContext;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Represents a special elements.
 * @since 0.4.0
 */
public enum SpecialElement implements VertexElement {

    /**
     * The runtime context.
     */
    CONTEXT(ElementKind.CONTEXT, Descriptions.typeOf(ProcessorContext.class)),
    ;

    private final ElementKind kind;

    private final TypeDescription type;

    SpecialElement(ElementKind kind, TypeDescription type) {
        this.kind = kind;
        this.type = type;
    }

    @Override
    public ElementKind getElementKind() {
        return kind;
    }

    @Override
    public TypeDescription getRuntimeType() {
        return type;
    }

    @Override
    public List<? extends VertexElement> getDependencies() {
        return Collections.emptyList();
    }
}
