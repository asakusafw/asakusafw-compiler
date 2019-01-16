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
package com.asakusafw.dag.compiler.codegen;

import java.util.Optional;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * Utilities about name.
 * @since 0.4.0
 */
public final class NameUtil {

    private NameUtil() {
        return;
    }

    /**
     * Returns a simple name of the target type.
     * @param type the target type
     * @return the simple name, or {@code null} if the type does not have a valid simple name
     */
    public static String getSimpleName(TypeDescription type) {
        Arguments.requireNonNull(type);
        return Optional.of(type)
                .filter(ClassDescription.class::isInstance)
                .map(ClassDescription.class::cast)
                .map(ClassDescription::getSimpleName)
                .orElse(null);
    }

    /**
     * Returns a simple name hint for the target type.
     * @param type the target type
     * @param suffix the name suffix
     * @return the simple name hint, or {@code null} if the type does not have a valid simple name
     */
    public static String getSimpleNameHint(TypeDescription type, String suffix) {
        Arguments.requireNonNull(type);
        Arguments.requireNonNull(suffix);
        return Optionals.of(getSimpleName(type)).map(s -> s + suffix).orElse(null);
    }
}
