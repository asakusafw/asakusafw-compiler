/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.model.plan;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Holds implementation class name.
 * @since 0.4.0
 */
public class Implementation {

    private final ClassDescription type;

    /**
     * Creates a new instance.
     * @param type the implementation type
     */
    public Implementation(ClassDescription type) {
        Arguments.requireNonNull(type);
        this.type = type;
    }

    /**
     * Returns the type.
     * @return the type
     */
    public ClassDescription getType() {
        return type;
    }

    @Override
    public String toString() {
        return type.getClassName();
    }
}
