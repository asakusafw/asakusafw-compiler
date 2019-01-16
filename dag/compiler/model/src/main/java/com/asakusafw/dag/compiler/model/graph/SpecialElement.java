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
import java.util.Objects;

import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Represents a special element.
 * @since 0.4.1
 */
public class SpecialElement implements VertexElement {

    private final ElementKind kind;

    private final TypeDescription type;

    /**
     * Creates a new instance.
     * @param kind the element kind
     * @param type the element type
     */
    public SpecialElement(ElementKind kind, TypeDescription type) {
        Arguments.requireNonNull(kind);
        Arguments.requireNonNull(type);
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Objects.hashCode(kind);
        result = prime * result + Objects.hashCode(type);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SpecialElement other = (SpecialElement) obj;
        if (!Objects.equals(kind, other.kind)) {
            return false;
        }
        if (!Objects.equals(type, other.type)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return kind.toString();
    }
}
