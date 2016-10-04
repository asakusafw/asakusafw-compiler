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
package com.asakusafw.vanilla.compiler.common;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Constants about Asakusa Vanilla tasks.
 * @since 0.4.0
 */
public final class VanillaTask {

    /**
     * The Vanilla components base path (relative from the Asakusa framework installation).
     */
    public static final Location PATH_ASSEMBLY = Location.of("vanilla"); //$NON-NLS-1$

    /**
     * The Vanilla bootstrap command path (relative from the Asakusa framework installation).
     */
    public static final Location PATH_COMMAND = PATH_ASSEMBLY.append(Location.of("bin/execute.sh")); //$NON-NLS-1$

    /**
     * The Vanilla configuration directory path (relative from the Asakusa framework installation).
     */
    public static final Location PATH_CONFIG_DIR = PATH_ASSEMBLY.append(Location.of("conf")); //$NON-NLS-1$

    /**
     * The Vanilla engine configuration file path (relative from the Asakusa framework installation).
     */
    public static final Location PATH_ENGINE_CONFIG = PATH_CONFIG_DIR.append(Location.of("vanilla.properties")); //$NON-NLS-1$

    /**
     * The module name.
     */
    public static final String MODULE_NAME = "vanilla"; //$NON-NLS-1$

    /**
     * The target profile name.
     */
    public static final String PROFILE_NAME = "vanilla"; //$NON-NLS-1$

    private VanillaTask() {
        return;
    }
}
