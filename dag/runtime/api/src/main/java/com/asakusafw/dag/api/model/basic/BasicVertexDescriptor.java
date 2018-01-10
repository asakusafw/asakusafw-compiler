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
package com.asakusafw.dag.api.model.basic;

import java.text.MessageFormat;
import java.util.Objects;

import com.asakusafw.dag.api.common.SupplierInfo;
import com.asakusafw.dag.api.model.VertexDescriptor;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * A basic implementation of {@link VertexDescriptor}.
 * @since 0.4.2
 */
public class BasicVertexDescriptor implements VertexDescriptor {

    private static final long serialVersionUID = 1L;

    private final SupplierInfo processor;

    /**
     * Creates a new instance.
     * @param processor information of supplier which provides {@link VertexProcessor}
     */
    public BasicVertexDescriptor(SupplierInfo processor) {
        Arguments.requireNonNull(processor);
        this.processor = processor;
    }

    /**
     * Returns information of supplier which provides {@link VertexProcessor}.
     * @return the processor supplier information
     */
    public SupplierInfo getProcessor() {
        return processor;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Objects.hashCode(processor);
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
        BasicVertexDescriptor other = (BasicVertexDescriptor) obj;
        if (!Objects.equals(processor, other.processor)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Vertex({0})", //$NON-NLS-1$
                processor);
    }
}
