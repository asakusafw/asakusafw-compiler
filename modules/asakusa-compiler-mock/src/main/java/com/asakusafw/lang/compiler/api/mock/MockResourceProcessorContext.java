package com.asakusafw.lang.compiler.api.mock;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.basic.AbstractResourceProcessorContext;

/**
 * Mock implementation of {@link com.asakusafw.lang.compiler.api.ResourceProcessor.Context}.
 */
public class MockResourceProcessorContext extends AbstractResourceProcessorContext {

    private final CompilerOptions options;

    private final ClassLoader classLoader;

    /**
     * Creates a new instance.
     * @param options the compiler options
     * @param classLoader the target application loader
     * @see #registerExtension(Class, Object)
     */
    public MockResourceProcessorContext(
            CompilerOptions options,
            ClassLoader classLoader) {
        this.options = options;
        this.classLoader = classLoader;
    }

    @Override
    public CompilerOptions getOptions() {
        return options;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
