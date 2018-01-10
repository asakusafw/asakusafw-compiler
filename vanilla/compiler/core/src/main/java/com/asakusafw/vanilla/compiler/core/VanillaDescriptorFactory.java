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
package com.asakusafw.vanilla.compiler.core;

import com.asakusafw.dag.api.common.SupplierInfo;
import com.asakusafw.dag.api.model.EdgeDescriptor;
import com.asakusafw.dag.api.model.VertexDescriptor;
import com.asakusafw.dag.api.model.basic.BasicEdgeDescriptor;
import com.asakusafw.dag.api.model.basic.BasicEdgeDescriptor.Movement;
import com.asakusafw.dag.api.model.basic.BasicVertexDescriptor;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.DataComparatorGenerator;
import com.asakusafw.dag.compiler.codegen.KeyValueSerDeGenerator;
import com.asakusafw.dag.compiler.codegen.ValueSerDeGenerator;
import com.asakusafw.dag.compiler.flow.DagDescriptorFactory;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * Provides descriptors of DAG API.
 * @since 0.4.0
 */
public class VanillaDescriptorFactory implements DagDescriptorFactory {

    private final ClassGeneratorContext context;

    /**
     * Creates a new instance.
     * @param context the current context
     */
    public VanillaDescriptorFactory(ClassGeneratorContext context) {
        Arguments.requireNonNull(context);
        this.context = context;
    }

    @Override
    public VertexDescriptor newVertex(ClassDescription processor) {
        return new BasicVertexDescriptor(toSupplier(processor));
    }

    @Override
    public EdgeDescriptor newVoidEdge() {
        return new BasicEdgeDescriptor(Movement.NOTHING, null, null);
    }

    @Override
    public EdgeDescriptor newOneToOneEdge(TypeDescription dataType, ClassDescription serde) {
        Arguments.requireNonNull(dataType);
        Arguments.requireNonNull(serde);
        return new BasicEdgeDescriptor(Movement.ONE_TO_ONE, toSupplier(serde), null);
    }

    @Override
    public EdgeDescriptor newBroadcastEdge(TypeDescription dataType, ClassDescription serde) {
        Arguments.requireNonNull(dataType);
        Arguments.requireNonNull(serde);
        return new BasicEdgeDescriptor(Movement.BROADCAST, toSupplier(serde), null);
    }

    @Override
    public EdgeDescriptor newScatterGatherEdge(TypeDescription dataType, ClassDescription serde, Group group) {
        Arguments.requireNonNull(dataType);
        Arguments.requireNonNull(group);
        SupplierInfo comparatorInfo = Optionals.of(group.getOrdering())
                .filter(o -> o.isEmpty() == false)
                .map(o -> DataComparatorGenerator.get(context, dataType, o))
                .map(VanillaDescriptorFactory::toSupplier)
                .orElse(null);
        return new BasicEdgeDescriptor(Movement.SCATTER_GATHER, toSupplier(serde), comparatorInfo);
    }

    @Override
    public EdgeDescriptor newOneToOneEdge(TypeDescription dataType) {
        Arguments.requireNonNull(dataType);
        ClassDescription serde = ValueSerDeGenerator.get(context, dataType);
        return newOneToOneEdge(dataType, serde);
    }

    @Override
    public EdgeDescriptor newBroadcastEdge(TypeDescription dataType) {
        Arguments.requireNonNull(dataType);
        ClassDescription serde = ValueSerDeGenerator.get(context, dataType);
        return newBroadcastEdge(dataType, serde);
    }

    @Override
    public EdgeDescriptor newScatterGatherEdge(TypeDescription dataType, Group group) {
        Arguments.requireNonNull(dataType);
        Arguments.requireNonNull(group);
        ClassDescription serde = KeyValueSerDeGenerator.get(context, dataType, group);
        return newScatterGatherEdge(dataType, serde, group);
    }

    private static SupplierInfo toSupplier(ClassDescription aClass) {
        return SupplierInfo.of(aClass.getBinaryName());
    }
}
