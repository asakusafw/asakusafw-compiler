package com.asakusafw.lang.compiler.api.mock;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.basic.AbstractBatchProcessorContext;
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
}
