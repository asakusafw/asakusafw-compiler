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
package com.asakusafw.lang.compiler.api.reference;

import java.util.Set;

import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;

/**
 * An abstract interface for a symbol of external input or output.
 */
public interface ExternalPortReference extends ExternalPortInfo, Reference {

    /**
     * Returns the original port name.
     * @return the original port name
     */
    String getName();

    /**
     * The internal paths for the target port.
     * The paths may include wildcard characters.
     * @return the paths
     */
    Set<String> getPaths();
}