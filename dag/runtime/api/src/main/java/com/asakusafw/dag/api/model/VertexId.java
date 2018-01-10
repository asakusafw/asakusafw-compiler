/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.dag.api.model;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Objects;

import com.asakusafw.lang.utils.common.Arguments;

/**
 * Represents ID of vertices.
 * @since 0.4.0
 */
public class VertexId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String vertexName;

    /**
     * Creates a new instance.
     * @param vertexName the target vertex name
     */
    public VertexId(String vertexName) {
        Arguments.requireNonNull(vertexName);
        this.vertexName = vertexName;
    }

    /**
     * Returns the vertex name.
     * @return the vertex name
     */
    public String getName() {
        return vertexName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Objects.hashCode(vertexName);
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
        VertexId other = (VertexId) obj;
        if (!vertexName.equals(other.vertexName)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Vertex({0})", //$NON-NLS-1$
                vertexName);
    }
}
