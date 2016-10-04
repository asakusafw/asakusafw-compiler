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
package com.asakusafw.dag.compiler.flow;

import com.asakusafw.dag.api.model.EdgeDescriptor;
import com.asakusafw.dag.api.model.VertexDescriptor;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;

/**
 * Provides descriptors of DAG API.
 * @since 0.4.0
 */
public interface DagDescriptorFactory {

    /**
     * Creates a new {@link VertexDescriptor}.
     * @param processor the {@link VertexProcessor} class
     * @return the created descriptor
     */
    VertexDescriptor newVertex(ClassDescription processor);

    /**
     * Creates a new void {@link EdgeDescriptor}.
     * @return the created descriptor
     */
    EdgeDescriptor newVoidEdge();

    /**
     * Creates a new one-to-one {@link EdgeDescriptor}.
     * @param dataType the target data type
     * @param serde the ser/de class
     * @return the created descriptor
     */
    EdgeDescriptor newOneToOneEdge(TypeDescription dataType, ClassDescription serde);

    /**
     * Creates a new broadcast {@link EdgeDescriptor}.
     * @param dataType the target data type
     * @param serde the ser/de class
     * @return the created descriptor
     */
    EdgeDescriptor newBroadcastEdge(TypeDescription dataType, ClassDescription serde);

    /**
     * Creates a new scatter-gather {@link EdgeDescriptor}.
     * @param dataType the data type
     * @param serde the custom ser/de supplier
     * @param group the grouping information
     * @return the created descriptor
     */
    EdgeDescriptor newScatterGatherEdge(TypeDescription dataType, ClassDescription serde, Group group);

    /**
     * Creates a new one-to-one {@link EdgeDescriptor}.
     * @param dataType the target data type
     * @return the created descriptor
     */
    EdgeDescriptor newOneToOneEdge(TypeDescription dataType);

    /**
     * Creates a new broadcast {@link EdgeDescriptor}.
     * @param dataType the data type
     * @return the created descriptor
     */
    EdgeDescriptor newBroadcastEdge(TypeDescription dataType);

    /**
     * Creates a new scatter-gather {@link EdgeDescriptor}.
     * @param dataType the data type
     * @param group the grouping information
     * @return the created descriptor
     */
    EdgeDescriptor newScatterGatherEdge(TypeDescription dataType, Group group);

}