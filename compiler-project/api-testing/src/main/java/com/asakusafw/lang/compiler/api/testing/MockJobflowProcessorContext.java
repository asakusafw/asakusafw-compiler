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
package com.asakusafw.lang.compiler.api.testing;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.basic.AbstractJobflowProcessorContext;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.common.BasicResourceContainer;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Mock implementation of {@link com.asakusafw.lang.compiler.api.JobflowProcessor.Context}.
 */
public class MockJobflowProcessorContext extends AbstractJobflowProcessorContext
        implements MockProcessorContext {

    /**
     * Returns the base path of {@link #addExternalInput(String, ExternalInputInfo) external inputs}.
     * The actual path will be follow its {@link ExternalInput#getName() name} after this prefix,
     * and it is relative from {@link CompilerOptions#getRuntimeWorkingDirectory()}.
     */
    public static final String EXTERNAL_INPUT_BASE = "external/input/"; //$NON-NLS-1$

    private static final String DEFAULT_BATCH_ID = "mockbatch"; //$NON-NLS-1$

    private final CompilerOptions options;

    private final ClassLoader classLoader;

    private final DataModelLoader dataModelLoader;

    private final BasicResourceContainer resources;

    private String batchId = DEFAULT_BATCH_ID;

    /**
     * Creates a new instance w/ using {@link MockDataModelLoader}.
     * @param options the compiler options
     * @param classLoader the target application loader
     * @param outputDirectory the build output directory
     * @see #withBatchId(String)
     * @see #registerExtension(Class, Object)
     */
    public MockJobflowProcessorContext(
            CompilerOptions options,
            ClassLoader classLoader,
            File outputDirectory) {
        this(options, classLoader, new MockDataModelLoader(classLoader), outputDirectory);
    }

    /**
     * Creates a new instance.
     * @param options the compiler options
     * @param classLoader the target application loader
     * @param dataModelLoader the data model loader
     * @param outputDirectory the build output directory
     * @see #withBatchId(String)
     * @see #registerExtension(Class, Object)
     */
    public MockJobflowProcessorContext(
            CompilerOptions options,
            ClassLoader classLoader,
            DataModelLoader dataModelLoader,
            File outputDirectory) {
        this.options = options;
        this.classLoader = classLoader;
        this.dataModelLoader = dataModelLoader;
        this.resources = new BasicResourceContainer(outputDirectory);
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
    public MockJobflowProcessorContext withBatchId(String newValue) {
        this.batchId = newValue;
        return this;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public DataModelLoader getDataModelLoader() {
        return dataModelLoader;
    }

    @Override
    public File getBaseDirectory() {
        return resources.getBasePath();
    }

    @Override
    public File getOutputFile(Location location) {
        return new File(getBaseDirectory(), location.toPath(File.separatorChar));
    }

    @Override
    public OutputStream addResourceFile(Location location) throws IOException {
        return resources.addResource(location);
    }

    @Override
    protected ExternalInputReference createExternalInput(String name, ExternalInputInfo info) {
        Set<String> paths = Collections.singleton(path(EXTERNAL_INPUT_BASE + name));
        return new ExternalInputReference(name, info, paths);
    }

    @Override
    protected ExternalOutputReference createExternalOutput(
            String name,
            ExternalOutputInfo info,
            Collection<String> internalOutputPaths) {
        return new ExternalOutputReference(name, info, internalOutputPaths);
    }

    private String path(String relative) {
        return String.format("%s/%s", getOptions().getRuntimeWorkingDirectory(), relative); //$NON-NLS-1$
    }
}
