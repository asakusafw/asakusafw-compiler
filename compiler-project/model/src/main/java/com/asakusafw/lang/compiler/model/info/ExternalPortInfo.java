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
package com.asakusafw.lang.compiler.model.info;

import java.util.Set;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;

/**
 * Structural information of external inputs/outputs.
 * @since 0.1.0
 * @version 0.3.0
 */
public interface ExternalPortInfo extends DescriptionInfo {

    /**
     * Returns the original importer/exporter description class.
     * @return the importer description class
     */
    @Override
    ClassDescription getDescriptionClass();

    /**
     * Returns the target data model class.
     * @return the target data model class
     */
    ClassDescription getDataModelClass();

    /**
     * Returns the importer module name.
     * @return the importer module name
     */
    String getModuleName();

    /**
     * Returns the set of parameter names which are required in runtime.
     * @return the required parameter names, or an empty set if they are not sure
     * @since 0.3.0
     */
    Set<String> getParameterNames();

    /**
     * Returns the processor specific contents.
     * @return the processor specific contents, or {@code null} if there are no special contents
     */
    ValueDescription getContents();
}
