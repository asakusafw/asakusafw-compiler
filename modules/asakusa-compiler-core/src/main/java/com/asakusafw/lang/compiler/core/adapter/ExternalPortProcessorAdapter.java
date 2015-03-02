package com.asakusafw.lang.compiler.core.adapter;

import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An adapter for {@link ExternalPortProcessor}.
 */
public class ExternalPortProcessorAdapter implements ExternalPortProcessor.Context {

    private final JobflowCompiler.Context delegate;

    private final DataModelLoaderAdapter dataModelLoader;

    /**
     * Creates a new instance.
     * @param delegate the delegate target context
     */
    public ExternalPortProcessorAdapter(JobflowCompiler.Context delegate) {
        this.delegate = delegate;
        this.dataModelLoader = new DataModelLoaderAdapter(delegate);
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
    public DataModelLoader getDataModelLoader() {
        return dataModelLoader;
    }

    @Override
    public OutputStream addClassFile(ClassDescription aClass) throws IOException {
        return addResourceFile(Util.toClassFileLocation(aClass));
    }

    @Override
    public OutputStream addResourceFile(Location location) throws IOException {
        return delegate.getOutput().addResource(location);
    }

    @Override
    public void addTask(Phase phase, TaskReference task) {
        delegate.getTaskContainerMap().getTaskContainer(phase).add(task);
    }

    @Override
    public <T> T getExtension(Class<T> extension) {
        return delegate.getExtension(extension);
    }
}
