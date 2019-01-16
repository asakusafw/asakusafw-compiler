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
package com.asakusafw.lang.compiler.mapreduce.testing;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.MessageFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.stage.StageConstants;
import com.asakusafw.runtime.stage.inprocess.InProcessStageConfigurator;
import com.asakusafw.runtime.util.VariableTable;
import com.asakusafw.runtime.windows.WindowsConfigurator;

/**
 * Tester for MapReduce stages.
 */
public final class MapReduceRunner {

    static {
        WindowsConfigurator.install();
    }

    private static final String[] EMPTY_ARGUMENTS = new String[0];

    private MapReduceRunner() {
        return;
    }

    /**
     * Executes the client class.
     * @param conf the current configuration
     * @param clientClass the target client class
     * @param executionId the execution ID
     * @param batchArguments the batch arguments
     * @param libraries additional libraries
     * @return the exit status code
     * @throws Exception if exception occurred while executing the stage
     */
    public static int execute(
            Configuration conf,
            ClassDescription clientClass,
            String executionId,
            Map<String, String> batchArguments,
            File... libraries) throws Exception {
        if (libraries.length == 0) {
            return execute0(conf, conf.getClassLoader(), clientClass, executionId, batchArguments);
        } else {
            URL[] urls = new URL[libraries.length];
            for (int i = 0; i < libraries.length; i++) {
                urls[i] = libraries[i].toURI().toURL();
            }
            try (URLClassLoader classLoader = URLClassLoader.newInstance(urls, conf.getClassLoader())) {
                return execute0(conf, classLoader, clientClass, executionId, batchArguments);
            }
        }
    }

    /**
     * Executes the client class.
     * @param conf the current configuration
     * @param clientClass the target client class
     * @param executionId the execution ID
     * @param batchArguments the batch arguments
     * @return the exit status code
     * @throws Exception if exception occurred while executing the stage
     */
    public static int execute(
            Configuration conf,
            ClassDescription clientClass,
            String executionId,
            Map<String, String> batchArguments) throws Exception {
        return execute0(conf, conf.getClassLoader(), clientClass, executionId, batchArguments);
    }

    private static int execute0(
            Configuration conf,
            ClassLoader classLoader,
            ClassDescription clientClass,
            String executionId,
            Map<String, String> batchArguments) throws Exception {
        try (ClassLoaderContext context = new ClassLoaderContext(classLoader)) {
            Configuration copy = new Configuration(conf);
            copy.setClassLoader(classLoader);
            Tool tool = resolveClient(copy, clientClass);
            configure(copy, executionId, batchArguments);
            return tool.run(EMPTY_ARGUMENTS);
        }
    }

    private static Tool resolveClient(Configuration conf, ClassDescription client) {
        try {
            Class<?> aClass = client.resolve(conf.getClassLoader());
            if (Tool.class.isAssignableFrom(aClass) == false) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "MapReduce client class must implement Tool interface: {0}",
                        client.getClassName()));
            }
            Tool tool = ReflectionUtils.newInstance(aClass.asSubclass(Tool.class), conf);
            return tool;
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "failed to resolve MapReduce client class: {0}",
                    client.getClassName()));
        }
    }

    private static void configure(Configuration conf, String executionId, Map<String, String> arguments) {
        conf.set(StageConstants.PROP_EXECUTION_ID, executionId);
        conf.set(StageConstants.PROP_USER, System.getProperty("user.name")); //$NON-NLS-1$
        conf.set(StageConstants.PROP_ASAKUSA_BATCH_ARGS, serialize(arguments));
        conf.setBoolean(InProcessStageConfigurator.KEY_FORCE, true);
    }

    private static String serialize(Map<String, String> arguments) {
        VariableTable table = new VariableTable();
        table.defineVariables(arguments);
        return table.toSerialString();
    }
}
