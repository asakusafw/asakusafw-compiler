package com.asakusafw.lang.compiler.core.adapter;

import com.asakusafw.lang.compiler.api.DataModelProcessor;
import com.asakusafw.lang.compiler.common.BasicExtensionContainer;
import com.asakusafw.lang.compiler.common.ExtensionContainer;
import com.asakusafw.lang.compiler.core.AnalyzerContext;

/**
 * An adapter for {@link DataModelProcessor}.
 */
public class DataModelProcessorAdapter implements DataModelProcessor.Context, ExtensionContainer.Editable {

    private final ClassLoader classLoader;

    private final ExtensionContainer.Editable extensions;

    /**
     * Creates a new instance.
     * @param delegate the delegate target context
     */
    public DataModelProcessorAdapter(AnalyzerContext delegate) {
        this.classLoader = delegate.getProject().getClassLoader();
        this.extensions = extractExtensionContainer(delegate);
    }

    private static ExtensionContainer.Editable extractExtensionContainer(Object object) {
        if (object instanceof ExtensionContainer.Editable) {
            return (ExtensionContainer.Editable) object;
        } else {
            return new BasicExtensionContainer();
        }
    }

    /**
     * Creates a new instance.
     * @param classLoader the class loader
     * @param extensions the extension container
     */
    public DataModelProcessorAdapter(ClassLoader classLoader, ExtensionContainer.Editable extensions) {
        this.classLoader = classLoader;
        this.extensions = extensions;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public <T> T getExtension(Class<T> extension) {
        return extensions.getExtension(extension);
    }

    @Override
    public <T> void registerExtension(Class<T> extension, T service) {
        extensions.registerExtension(extension, service);
    }
}
