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
package com.asakusafw.lang.compiler.testdriver.adapter;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.asakusafw.testdriver.compiler.CompilerConfiguration;
import com.asakusafw.testdriver.compiler.CompilerSession;
import com.asakusafw.testdriver.compiler.CompilerToolkit;
import com.asakusafw.testdriver.compiler.FlowPortMap;

/**
 * An implementation of {@link CompilerToolkit}.
 * @since 0.3.0
 * @see CompilerProfileInitializer
 */
public class CompilerToolkitAdapter implements CompilerToolkit {

    @Override
    public String getName() {
        return "DAG";
    }

    @Override
    public FlowPortMap newFlowPortMap() {
        return new FlowPortMapAdapter();
    }

    @Override
    public CompilerConfiguration newConfiguration() {
        CompilerConfigurationAdapter configuration = new CompilerConfigurationAdapter();
        configuration.withDefaults();
        return configuration;
    }

    @Override
    public CompilerSession newSession(CompilerConfiguration configuration) throws IOException {
        if ((configuration instanceof CompilerConfigurationAdapter) == false) {
            throw new IllegalStateException();
        }
        CompilerConfigurationAdapter conf = (CompilerConfigurationAdapter) configuration;
        initializeWorkingDirectory(conf);
        return new CompilerSessionAdapter(conf);
    }

    private static void initializeWorkingDirectory(CompilerConfiguration conf) throws IOException {
        File workingDirectory = conf.getWorkingDirectory();
        if (workingDirectory == null) {
            throw new IllegalStateException();
        }
        if (workingDirectory.exists()) {
            FileUtils.forceDelete(workingDirectory);
        }
    }
}
