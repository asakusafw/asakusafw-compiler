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
package com.asakusafw.vanilla.testkit.common;

import java.util.Collection;
import java.util.Set;

import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.workflow.model.Element;

/**
 * Extra information of Asakusa Vanilla tasks.
 * @since 0.10.0
 */
public class VanillaTaskInfo implements Element.Attribute {

    private final Set<Requiremnt> requirements;

    /**
     * Creates a new instance.
     * @param requirements the task requirements
     */
    public VanillaTaskInfo(Collection<Requiremnt> requirements) {
        this.requirements = EnumUtil.freeze(requirements);
    }

    /**
     * Returns the task required features.
     * @return the requirements
     */
    public Set<Requiremnt> getRequirements() {
        return requirements;
    }

    /**
     * Represents Asakusa Vanilla task requirements.
     * @since 0.10.0
     */
    public enum Requiremnt {

        /**
         * Requires the framework core configuration file.
         */
        CORE_CONFIGURATION_FILE,

        /**
         * Requires the engine configuration file.
         */
        ENGINE_CONFIGURATION_FILE,
    }
}
