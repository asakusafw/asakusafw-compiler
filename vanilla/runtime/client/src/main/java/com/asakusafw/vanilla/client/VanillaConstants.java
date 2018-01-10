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
package com.asakusafw.vanilla.client;

/**
 * Constants of Asakusa Vanilla.
 * @since 0.4.0
 * @version 0.5.0
 */
public final class VanillaConstants {

    /**
     * The configuration key prefix ({@value}).
     */
    public static final String KEY_ENGINE_PREFIX = "com.asakusafw.vanilla."; //$NON-NLS-1$

    /**
     * The configuration key prefix of Hadoop settings ({@value}).
     */
    public static final String KEY_HADOOP_PREFIX = "hadoop.";

    /**
     * The environment variable name of Java launch options for Vanilla.
     * @since 0.5.0
     */
    public static final String ENV_VANILLA_OPTIONS = "ASAKUSA_VANILLA_OPTS";

    /**
     * The environment variable name of engine launch options for Vanilla.
     * @since 0.5.0
     */
    public static final String ENV_VANILLA_ARGUMENTS = "ASAKUSA_VANILLA_ARGS";

    /**
     * The environment variable name of Vanilla command launcher.
     * @since 0.5.0
     */
    public static final String ENV_VANILLA_LAUNCHER = "ASAKUSA_VANILLA_LAUNCHER";

    private VanillaConstants() {
        return;
    }
}
