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

import com.asakusafw.lang.utils.common.Arguments;

/**
 * Describes edges between vertices.
 * @since 0.4.0
 */
public class EdgeInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PortId upstream;

    private final PortId downstream;

    private final EdgeDescriptor descriptor;

    /**
     * Creates a new instance.
     * @param upstream the upstream port ID
     * @param downstream the downstream port ID
     * @param descriptor the processor descriptor
     */
    public EdgeInfo(PortId upstream, PortId downstream, EdgeDescriptor descriptor) {
        Arguments.requireNonNull(upstream);
        Arguments.requireNonNull(downstream);
        Arguments.requireNonNull(descriptor);
        this.upstream = upstream;
        this.downstream = downstream;
        this.descriptor = descriptor;
    }

    /**
     * Returns the upstream port ID.
     * @return the upstream port ID
     */
    public PortId getUpstreamId() {
        return upstream;
    }

    /**
     * Returns the downstream port ID.
     * @return the downstream port ID
     */
    public PortId getDownstreamId() {
        return downstream;
    }

    /**
     * Returns the descriptor which processes data exchanging on this edge.
     * @return the descriptor
     */
    public EdgeDescriptor getDescriptor() {
        return descriptor;
    }
}
