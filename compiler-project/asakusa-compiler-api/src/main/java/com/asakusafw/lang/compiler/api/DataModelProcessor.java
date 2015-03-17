/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.api;

import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Process data model types.
 */
public interface DataModelProcessor {

    /**
     * Returns whether this processor can process the target data model type.
     * @param context the current context
     * @param type the target type
     * @return {@code true} if this processor can process the target data model type type, otherwise {@code false}
     * @throws DiagnosticException if failed to extract information from the target description
     */
    boolean isSupported(Context context, TypeDescription type);

    /**
     * Processes the target type.
     * @param context the current context
     * @param type the target type
     * @return the symbol for the target data model type
     * @throws DiagnosticException if exception occurred while processing the target type
     */
    DataModelReference process(Context context, TypeDescription type);

    /**
     * Represents a context object for {@link DataModelProcessor}.
     */
    public static interface Context extends ExtensionContainer {

        /**
         * Returns the class loader to obtain the target application classes.
         * @return the class loader
         */
        ClassLoader getClassLoader();
    }
}
