/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import com.asakusafw.dag.api.model.PortInfo;
import com.asakusafw.dag.api.model.PortInfo.Direction;
import com.asakusafw.dag.api.model.basic.BasicEdgeDescriptor;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Represents an output port of vertices.
 * @since 0.4.0
 */
public class OutputPortMirror extends PortMirror.Abstract<OutputPortMirror, InputPortMirror> {

    /**
     * Creates a new instance.
     * @param owner the owner vertex
     * @param info the original information
     * @param descriptor the edge descriptor
     */
    public OutputPortMirror(VertexMirror owner, PortInfo info, BasicEdgeDescriptor descriptor) {
        super(owner, info, descriptor);
        Arguments.require(info.getDirection() == Direction.OUTPUT);
    }
}
