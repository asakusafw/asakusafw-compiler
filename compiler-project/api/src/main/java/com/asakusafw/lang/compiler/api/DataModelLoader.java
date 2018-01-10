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
package com.asakusafw.lang.compiler.api;

import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Provides {@link DataModelReference}.
 */
@FunctionalInterface
public interface DataModelLoader {

    /**
     * Returns a {@link DataModelReference} corresponded to the target type.
     * @param type the data model type
     * @return the data model
     * @throws DiagnosticException if failed to load the target data model
     */
    DataModelReference load(TypeDescription type);
}
