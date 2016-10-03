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
package com.asakusafw.dag.compiler.model.graph;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An abstract super interface of {@link VertexElement} that has a custom implementation.
 * @since 0.4.0
 */
public interface ClassNode extends VertexElement {

    /**
     * Returns the implementation type.
     * @return the implementation type
     */
    ClassDescription getImplementationType();
}
