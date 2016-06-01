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
package com.asakusafw.lang.compiler.optimizer.testing;

import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.testing.MockDataModelLoader;
import com.asakusafw.lang.compiler.common.BasicExtensionContainer;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.ResourceContainer;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.optimizer.OptimizerContext;
import com.asakusafw.lang.compiler.optimizer.OptimizerToolkit;
import com.asakusafw.lang.compiler.optimizer.basic.BasicOptimizerToolkit;

/**
 * Mock implementation of {@link com.asakusafw.lang.compiler.api.JobflowProcessor.Context}.
 */
public class MockOptimizerContext extends BasicExtensionContainer implements OptimizerContext {

    private static final String EXTENSION_CLASS = ".class"; //$NON-NLS-1$

    private static final String DEFAULT_BATCH_ID = "mockbatch"; //$NON-NLS-1$

    private static final String DEFAULT_FLOW_ID = "mockflow"; //$NON-NLS-1$

    private final CompilerOptions options;

    private final ClassLoader classLoader;

    private final DataModelLoader dataModels;

    private final OptimizerToolkit toolkit = new BasicOptimizerToolkit();

    private final ResourceContainer resources;

    private String batchId = DEFAULT_BATCH_ID;

    private String flowId = DEFAULT_FLOW_ID;

    /**
     * Creates a new instance w/ using {@link MockDataModelLoader}.
     * @param options the compiler options
     * @param classLoader the target application loader
     * @param dataModels the data model loader
     * @param resources the resource container
     * @see #withBatchId(String)
     * @see #registerExtension(Class, Object)
     */
    public MockOptimizerContext(
            CompilerOptions options, ClassLoader classLoader,
            DataModelLoader dataModels, ResourceContainer resources) {
        this.options = options;
        this.classLoader = classLoader;
        this.dataModels = dataModels;
        this.resources = resources;
    }

    @Override
    public CompilerOptions getOptions() {
        return options;
    }

    @Override
    public String getBatchId() {
        return batchId;
    }

    /**
     * Sets the current batch ID.
     * @param newValue the new value
     * @return this
     */
    public MockOptimizerContext withBatchId(String newValue) {
        this.batchId = newValue;
        return this;
    }

    @Override
    public String getFlowId() {
        return flowId;
    }

    /**
     * Sets the current flow ID.
     * @param newValue the new value
     * @return this
     */
    public MockOptimizerContext withFlowId(String newValue) {
        this.flowId = newValue;
        return this;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public DataModelLoader getDataModelLoader() {
        return dataModels;
    }

    @Override
    public OptimizerToolkit getToolkit() {
        return toolkit;
    }

    @Override
    public OutputStream addResourceFile(Location location) throws IOException {
        return resources.addResource(location);
    }

    @Override
    public OutputStream addClassFile(ClassDescription aClass) throws IOException {
        String path = aClass.getInternalName() + EXTENSION_CLASS;
        return addResourceFile(Location.of(path, '/'));
    }
}
