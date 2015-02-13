package com.asakusafw.lang.compiler.api.mock;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.basic.AbstractExternalIoProcessorContext;
import com.asakusafw.lang.compiler.api.basic.BasicResourceContainer;
import com.asakusafw.lang.compiler.model.Location;

/**
 * Mock implementation of {@link com.asakusafw.lang.compiler.api.ExternalIoProcessor.Context}.
 */
public class MockExternalIoProcessorContext extends AbstractExternalIoProcessorContext
        implements MockProcessorContext {

    private final CompilerOptions options;

    private final ClassLoader classLoader;

    private final DataModelLoader dataModelLoader;

    private final BasicResourceContainer resources;

    /**
     * Creates a new instance w/ using {@link MockDataModelLoader}.
     * @param options the compiler options
     * @param classLoader the target application loader
     * @param outputDirectory the build output directory
     * @see #registerExtension(Class, Object)
     */
    public MockExternalIoProcessorContext(
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
     * @see #registerExtension(Class, Object)
     */
    public MockExternalIoProcessorContext(
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
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public DataModelLoader getDataModelLoader() {
        return dataModelLoader;
    }

    @Override
    public File getOutputDirectory() {
        return resources.getBasePath();
    }

    @Override
    public OutputStream addResourceFile(Location location) throws IOException {
        return resources.addResourceFile(location);
    }
}
