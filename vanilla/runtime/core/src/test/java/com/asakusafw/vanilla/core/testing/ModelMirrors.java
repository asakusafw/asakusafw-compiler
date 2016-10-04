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
package com.asakusafw.vanilla.core.testing;

import java.util.function.Supplier;

import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.dag.api.common.SupplierInfo;
import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.vanilla.api.VanillaEdgeDescriptor;
import com.asakusafw.vanilla.api.VanillaVertexDescriptor;

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
     * Returns a {@link VanillaVertexDescriptor} of the given {@link VertexProcessor}.
     * @param processor the processor class
     * @return the created descriptor
     */
    public static VanillaVertexDescriptor vertex(Class<?> processor) {
        return new VanillaVertexDescriptor(supplier(processor));
    }

    /**
     * Returns a {@link VanillaVertexDescriptor} of the given {@link VertexProcessor}.
     * @param processor the processor supplier
     * @return the created descriptor
     */
    public static VanillaVertexDescriptor vertex(Supplier<VertexProcessor> processor) {
        return new VanillaVertexDescriptor(supplier(processor));
    }

    /**
     * Creates a new nothing edge descriptor.
     * @return the created instance.
     */
    public static VanillaEdgeDescriptor nothing() {
        return VanillaEdgeDescriptor.newNothing();
    }

    /**
     * Creates a new one-to-one edge descriptor.
     * @param serde information of supplier which provides {@link ValueSerDe}
     * @return the created instance
     */
    public static VanillaEdgeDescriptor oneToOne(Class<?> serde) {
        return VanillaEdgeDescriptor.newOneToOne(supplier(serde));
    }

    /**
     * Creates a new broadcast edge descriptor.
     * @param serde information of supplier which provides {@link ValueSerDe}
     * @return the created instance
     */
    public static VanillaEdgeDescriptor broadcast(Class<?> serde) {
        return VanillaEdgeDescriptor.newBroadcast(supplier(serde));
    }

    /**
     * Creates a new scatter-gather edge descriptor.
     * @param serde information of supplier which provides {@link KeyValueSerDe}
     * @param comparator the value comparator (nullable)
     * @return the created instance
     */
    public static VanillaEdgeDescriptor scatterGather(Class<?> serde, Class<?> comparator) {
        return VanillaEdgeDescriptor.newScatterGather(
                supplier(serde),
                comparator == null ? null : supplier(comparator));
    }
}
