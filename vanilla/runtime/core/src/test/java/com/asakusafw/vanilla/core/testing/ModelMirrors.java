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
package com.asakusafw.vanilla.core.testing;

import java.util.function.Supplier;

import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.dag.api.common.SupplierInfo;
import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.api.model.basic.BasicEdgeDescriptor;
import com.asakusafw.dag.api.model.basic.BasicEdgeDescriptor.Movement;
import com.asakusafw.dag.api.model.basic.BasicVertexDescriptor;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * A common utilities for Vanilla mirror models.
 */
public final class ModelMirrors {

    private ModelMirrors() {
        return;
    }

    /**
     * Creates a new {@link SupplierInfo} from the given class.
     * @param element the target class
     * @return the created {@link SupplierInfo} which provides instances of the given class
     */
    public static SupplierInfo supplier(Class<?> element) {
        return SupplierInfo.of(element.getName());
    }

    /**
     * Creates a new {@link SupplierInfo} from the given supplier.
     * @param element the target supplier
     * @return the created {@link SupplierInfo} which provides the supplier
     */
    public static SupplierInfo supplier(Supplier<?> element) {
        return cl -> element;
    }

    /**
     * Returns a {@link BasicVertexDescriptor} of the given {@link VertexProcessor}.
     * @param processor the processor class
     * @return the created descriptor
     */
    public static BasicVertexDescriptor vertex(Class<?> processor) {
        return new BasicVertexDescriptor(supplier(processor));
    }

    /**
     * Returns a {@link BasicVertexDescriptor} of the given {@link VertexProcessor}.
     * @param processor the processor supplier
     * @return the created descriptor
     */
    public static BasicVertexDescriptor vertex(Supplier<VertexProcessor> processor) {
        return new BasicVertexDescriptor(supplier(processor));
    }

    /**
     * Creates a new nothing edge descriptor.
     * @return the created instance.
     */
    public static BasicEdgeDescriptor nothing() {
        return new BasicEdgeDescriptor(Movement.NOTHING, null, null);
    }

    /**
     * Creates a new one-to-one edge descriptor.
     * @param serde information of supplier which provides {@link ValueSerDe}
     * @return the created instance
     */
    public static BasicEdgeDescriptor oneToOne(Class<?> serde) {
        return new BasicEdgeDescriptor(Movement.ONE_TO_ONE, supplier(serde), null);
    }

    /**
     * Creates a new broadcast edge descriptor.
     * @param serde information of supplier which provides {@link ValueSerDe}
     * @return the created instance
     */
    public static BasicEdgeDescriptor broadcast(Class<?> serde) {
        return new BasicEdgeDescriptor(Movement.BROADCAST, supplier(serde), null);
    }

    /**
     * Creates a new scatter-gather edge descriptor.
     * @param serde information of supplier which provides {@link KeyValueSerDe}
     * @param comparator the value comparator (nullable)
     * @return the created instance
     */
    public static BasicEdgeDescriptor scatterGather(Class<?> serde, Class<?> comparator) {
        return new BasicEdgeDescriptor(Movement.SCATTER_GATHER, supplier(serde), Optionals.of(comparator)
                .map(ModelMirrors::supplier)
                .orElse(null));
    }
}
