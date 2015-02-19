package com.asakusafw.lang.compiler.core.adapter;

import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.BatchCompiler;

/**
 * An adapter for {@link BatchProcessor}.
 */
public class BatchProcessorAdapter implements BatchProcessor.Context {

    private final BatchCompiler.Context delegate;

    /**
     * Creates a new instance.
     * @param delegate the delegate target context
     */
    public BatchProcessorAdapter(BatchCompiler.Context delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompilerOptions getOptions() {
        return delegate.getOptions();
    }

    @Override
    public ClassLoader getClassLoader() {
        return delegate.getProject().getClassLoader();
    }

    @Override
    public OutputStream addResourceFile(Location location) throws IOException {
        return delegate.getOutput().addResource(location);
    }

    @Override
    public <T> T getExtension(Class<T> extension) {
        return delegate.getExtension(extension);
    }
}
