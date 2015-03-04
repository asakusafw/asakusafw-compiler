package com.asakusafw.lang.compiler.api.testing;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.basic.AbstractExternalPortProcessorContext;
import com.asakusafw.lang.compiler.common.BasicResourceContainer;
import com.asakusafw.lang.compiler.common.Location;

/**
 * Mock implementation of {@link com.asakusafw.lang.compiler.api.ExternalPortProcessor.Context}.
 */
public class MockExternalPortProcessorContext extends AbstractExternalPortProcessorContext
        implements MockProcessorContext {

    private static final String DEFAULT_BATCH_ID = "mockbatch"; //$NON-NLS-1$

    private static final String DEFAULT_FLOW_ID = "mockflow"; //$NON-NLS-1$

    private final CompilerOptions options;

    private final ClassLoader classLoader;

    private final DataModelLoader dataModelLoader;

    private final BasicResourceContainer resources;

    private String batchId = DEFAULT_BATCH_ID;

    private String flowId = DEFAULT_FLOW_ID;

    /**
     * Creates a new instance w/ using {@link MockDataModelLoader}.
     * @param options the compiler options
     * @param classLoader the target application loader
     * @param outputDirectory the build output directory
     * @see #withBatchId(String)
     * @see #withFlowId(String)
     * @see #registerExtension(Class, Object)
     */
    public MockExternalPortProcessorContext(
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
     * @see #withFlowId(String)
     * @see #registerExtension(Class, Object)
     */
    public MockExternalPortProcessorContext(
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
    public MockExternalPortProcessorContext withBatchId(String newValue) {
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
    public MockExternalPortProcessorContext withFlowId(String newValue) {
        this.flowId = newValue;
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
}
