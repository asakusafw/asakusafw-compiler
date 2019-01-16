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
package com.asakusafw.lang.compiler.api.testing;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.basic.AbstractBatchProcessorContext;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.common.BasicResourceContainer;
import com.asakusafw.lang.compiler.common.Location;

/**
 * Mock implementation of {@link com.asakusafw.lang.compiler.api.BatchProcessor.Context}.
 */
public class MockBatchProcessorContext extends AbstractBatchProcessorContext implements MockProcessorContext {

    private final CompilerOptions options;

    private final ClassLoader classLoader;

    private final BasicResourceContainer resources;

    /**
     * Creates a new instance.
     * @param options the compiler options
     * @param classLoader the target application loader
     * @param outputDirectory the build output directory
     * @see #registerExtension(Class, Object)
     */
    public MockBatchProcessorContext(
            CompilerOptions options,
            ClassLoader classLoader,
            File outputDirectory) {
        this.options = options;
        this.classLoader = classLoader;
        this.resources = new BasicResourceContainer(outputDirectory);
    }

    @Override
    public CompilerOptions getOptions() {
        return options;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
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
    public InputStream findResourceFile(JobflowReference jobflow, Location location) throws IOException {
        return null;
    }
}
