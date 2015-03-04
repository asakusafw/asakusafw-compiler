package com.asakusafw.lang.compiler.api.testing;

import com.asakusafw.lang.compiler.api.basic.AbstractDataModelProcessorContext;

/**
 * Mock implementation of {@link com.asakusafw.lang.compiler.api.DataModelProcessor.Context}.
 */
public class MockDataModelProcessorContext extends AbstractDataModelProcessorContext {

    private final ClassLoader classLoader;

    /**
     * Creates a new instance.
     * @param classLoader the target application loader
     * @see #registerExtension(Class, Object)
     */
    public MockDataModelProcessorContext(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
