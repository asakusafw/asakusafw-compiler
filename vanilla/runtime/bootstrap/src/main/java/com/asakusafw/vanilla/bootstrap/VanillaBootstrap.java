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
package com.asakusafw.vanilla.bootstrap;

import static com.asakusafw.vanilla.bootstrap.CoreConstants.*;
import static com.asakusafw.vanilla.bootstrap.VanillaConstants.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A bootstrap entry of Asakusa Vanilla.
 * @since 0.5.0
 */
public class VanillaBootstrap {

    private final Environment environment;

    private final ClassLoader classLoader;

    /**
     * Creates a new instance.
     * @param environment the environment variables
     * @param classLoader the class loader
     */
    public VanillaBootstrap(Environment environment, ClassLoader classLoader) {
        this.environment = environment;
        this.classLoader = classLoader;
    }

    /**
     * Program entry.
     * @param args the program arguments
     */
    public static void main(String... args) {
        VanillaBootstrap bootstrap = new VanillaBootstrap(Environment.system(), ClassLoader.getSystemClassLoader());
        bootstrap.exec(Context.parse(args));
    }

    /**
     * Executes Vanilla application.
     * @param context the application context
     */
    public void exec(Context context) {
        Classpath classpath = buildClasspath(context);
        Arguments arguments = buildArguments(context);
        classpath.exec(classLoader, CLASS_VANILLA_LAUNCHER, arguments.toArray());
    }

    private Classpath buildClasspath(Context context) {
        Classpath cp = new Classpath();

        Path application = getApplication(environment, context.getBatchId());
        cp.add(getAppJobflowLibFile(application, context.getFlowId()), true);
        cp.add(application.resolve(PATH_APP_USER_LIB_DIR), false);

        Path home = getHome(environment);
        cp.add(home.resolve(PATH_VANILLA_CONF_DIR), false);
        cp.addEntries(home.resolve(PATH_VANILLA_LIB_DIR), true);
        cp.addEntries(home.resolve(PATH_EXTENSION_LIB_DIR), false);
        cp.addEntries(home.resolve(PATH_CORE_LIB_DIR), true);

        cp.addEntries(home.resolve(PATH_HADOOP_EMBEDDED_LIB_DIR), true);

        return cp;
    }

    private Arguments buildArguments(Context context) {
        Arguments args = new Arguments();

        args.add(OPT_APPLICATION, context.getApplicationClassName());
        args.add(OPT_BATCH_ID, context.getBatchId());
        args.add(OPT_FLOW_ID, context.getFlowId());
        args.add(OPT_EXECUTION_ID, context.getExecutionId());
        args.add(OPT_BATCH_ARGUMENTS, context.getBatchArguments());

        Path home = getHome(environment);
        Optional.of(home.resolve(PATH_CORE_CONF_FILE))
                .filter(Files::isRegularFile)
                .ifPresent(it -> args.add(OPT_HADOOP_CONF, OPT_LOCATION_PREFIX + it));
        Optional.of(home.resolve(PATH_VANILLA_CONF_FILE))
                .filter(Files::isRegularFile)
                .ifPresent(it -> args.add(OPT_ENGINE_CONF, OPT_LOCATION_PREFIX + it));

        environment.find(ENV_VANILLA_LAUNCHER_ARGUMENTS)
                .map(it -> Arrays.stream(it.split("\\s+"))
                        .map(String::trim)
                        .filter(s -> s.isEmpty() == false)
                        .collect(Collectors.toList()))
                .ifPresent(args::add);
        args.add(context.getExtraArguments());

        return args;
    }
}
