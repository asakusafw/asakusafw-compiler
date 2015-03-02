package com.asakusafw.lang.compiler.api.mock;

import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.runtime.model.DataModel;

/**
 * Mock implementation of {@link DataModelLoader}.
 * <p>
 * This loads each {@link DataModel} implementation, which provides {@code get<property-name>Option()} methods.
 * </p>
 */
public class MockDataModelLoader implements DataModelLoader {

    private final ClassLoader classLoader;

    /**
     * Creates a new instance.
     * @param classLoader the original class loader
     */
    public MockDataModelLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public DataModelReference load(TypeDescription type) {
        return MockDataModelProcessor.process(classLoader, type);
    }
}
