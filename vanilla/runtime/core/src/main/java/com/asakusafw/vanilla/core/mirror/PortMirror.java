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
package com.asakusafw.vanilla.core.mirror;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import com.asakusafw.dag.api.common.DataComparator;
import com.asakusafw.dag.api.common.KeyValueSerDe;
import com.asakusafw.dag.api.common.SupplierInfo;
import com.asakusafw.dag.api.common.TaggedSupplier;
import com.asakusafw.dag.api.common.ValueSerDe;
import com.asakusafw.dag.api.model.PortId;
import com.asakusafw.dag.api.model.PortInfo;
import com.asakusafw.dag.api.model.basic.BasicEdgeDescriptor;
import com.asakusafw.dag.api.model.basic.BasicEdgeDescriptor.PortType;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * An abstract implementation of I/O port mirror of vertices.
 * @since 0.4.0
 */
public abstract class PortMirror {

    private final VertexMirror owner;

    private final PortInfo info;

    private final BasicEdgeDescriptor descriptor;

    /**
     * Creates a new instance.
     * @param owner the owner vertex
     * @param info the original information
     * @param descriptor the edge descriptor
     */
    public PortMirror(VertexMirror owner, PortInfo info, BasicEdgeDescriptor descriptor) {
        this.owner = owner;
        this.info = info;
        this.descriptor = descriptor;
    }

    /**
     * Returns the owner.
     * @return the owner
     */
    public VertexMirror getOwner() {
        return owner;
    }

    /**
     * Returns the ID.
     * @return the port ID
     */
    public PortId getId() {
        return info.getId();
    }

    /**
     * Returns the movement type.
     * @return the movement type
     */
    public BasicEdgeDescriptor.Movement getMovement() {
        return descriptor.getMovement();
    }

    /**
     * Returns the opposites of this port.
     * @return the opposite ports
     */
    public abstract List<? extends PortMirror> getOpposites();

    /**
     * Creates a new value ser/de.
     * @param loader the class loader
     * @return the created ser/de
     */
    public ValueSerDe newValueSerDe(ClassLoader loader) {
        Arguments.requireNonNull(loader);
        Invariants.require(descriptor.getMovement().getPortType() == PortType.VALUE);
        return (ValueSerDe) resolveSupplierInfo(loader, descriptor.getSerDe());
    }

    /**
     * Creates a new key-value ser/de.
     * @param loader the class loader
     * @return the created ser/de
     */
    public KeyValueSerDe newKeyValueSerDe(ClassLoader loader) {
        Arguments.requireNonNull(loader);
        Invariants.require(descriptor.getMovement().getPortType() == PortType.KEY_VALUE);
        return (KeyValueSerDe) resolveSupplierInfo(loader, descriptor.getSerDe());
    }

    /**
     * Creates a new comparator.
     * @param loader the class loader
     * @return the created comparator, or {@code null} if it is not defined
     */
    public DataComparator newComparator(ClassLoader loader) {
        Arguments.requireNonNull(loader);
        Invariants.require(descriptor.getMovement().getPortType() == PortType.KEY_VALUE);
        SupplierInfo supplier = descriptor.getComparator();
        if (supplier == null) {
            return null;
        } else {
            return (DataComparator) resolveSupplierInfo(loader, supplier);
        }
    }

    private Object resolveSupplierInfo(ClassLoader loader, SupplierInfo supplierInfo) {
        Supplier<?> supplier = supplierInfo.newInstance(loader);
        if (supplier instanceof TaggedSupplier<?>) {
            return ((TaggedSupplier<?>) supplier).get(info.getTag());
        } else {
            return supplier.get();
        }
    }

    @Override
    public String toString() {
        return getId().toString();
    }

    /**
     * An abstract implementation of {@link PortMirror}.
     * @param <TSelf> the self type
     * @param <TOpposite> the opposite type
     * @since 0.4.0
     */
    public abstract static class Abstract<
            TSelf extends Abstract<TSelf, TOpposite>,
            TOpposite extends Abstract<TOpposite, TSelf>> extends PortMirror {

        private final List<TOpposite> opposites = new ArrayList<>();

        /**
         * Creates a new instance.
         * @param owner the owner vertex
         * @param info the original information
         * @param descriptor the edge descriptor
         */
        public Abstract(VertexMirror owner, PortInfo info, BasicEdgeDescriptor descriptor) {
            super(owner, info, descriptor);
        }

        /**
         * Connects to the given port.
         * @param opposite the target port
         * @return this
         */
        public final TSelf connect(TOpposite opposite) {
            Arguments.requireNonNull(opposite);
            @SuppressWarnings("unchecked")
            TSelf self = (TSelf) this;
            self.connect0(opposite);
            opposite.connect0(self);
            return self;
        }

        final void connect0(TOpposite opposite) {
            Arguments.requireNonNull(opposite);
            opposites.add(opposite);
        }

        @Override
        public List<TOpposite> getOpposites() {
            return Collections.unmodifiableList(opposites);
        }
    }
}
