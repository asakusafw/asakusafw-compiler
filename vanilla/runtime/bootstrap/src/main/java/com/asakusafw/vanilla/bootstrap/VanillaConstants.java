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
package com.asakusafw.vanilla.bootstrap;

/**
 * Constants of Asakusa Vanilla.
 * @since 0.5.0
 */
public final class VanillaConstants {

    /**
     * The common launcher option name of Hadoop configuration entry.
     */
    public static final String OPT_HADOOP_CONF = "--hadoop-conf";

    /**
     * The common launcher option name of execution engine configuration entry.
     */
    public static final String OPT_ENGINE_CONF = "--engine-conf";

    /**
     * The common launcher option name of application class.
     */
    public static final String OPT_APPLICATION = "--client";

    /**
     * The common launcher option name of batch ID.
     */
    public static final String OPT_BATCH_ID = "--batch-id";

    /**
     * The common launcher option name of flow ID.
     */
    public static final String OPT_FLOW_ID = "--flow-id";

    /**
     * The common launcher option name of execution ID.
     */
    public static final String OPT_EXECUTION_ID = "--execution-id";

    /**
     * The common launcher option name of serialized batch arguments.
     */
    public static final String OPT_BATCH_ARGUMENTS = "--batch-arguments";

    /**
     * The option value prefix of that represents a file location.
     */
    public static final String OPT_LOCATION_PREFIX = "@";


    static final String PATH_VANILLA_BASE = "vanilla";

    /**
     * The path of the Vanilla configuration directory.
     */
    public static final String PATH_VANILLA_CONF_DIR = PATH_VANILLA_BASE + "/conf";

    /**
     * The path of the Vanilla engine configuration file.
     */
    public static final String PATH_VANILLA_CONF_FILE = PATH_VANILLA_CONF_DIR + "/vanilla.properties";

    /**
     * The path of the Vanilla engine libraries directory.
     */
    public static final String PATH_VANILLA_LIB_DIR = PATH_VANILLA_BASE + "/lib";

    /**
     * The class name of Vanilla launcher class.
     */
    public static final String CLASS_VANILLA_LAUNCHER = "com.asakusafw.vanilla.client.VanillaLauncher";

    /**
     * The environment variable name of extra Vanilla launcher arguments.
     */
    public static final String ENV_VANILLA_LAUNCHER_ARGUMENTS = "ASAKUSA_VANILLA_ARGS";

    private VanillaConstants() {
        return;
    }
}
