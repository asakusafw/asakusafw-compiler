package com.asakusafw.lang.compiler.api.testing;

import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.model.description.Descriptions;
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

    /**
     * Loads the data model type using {@link MockDataModelLoader}.
     * @param type the target type
     * @return the resolved reference
     */
    public static DataModelReference load(Class<?> type) {
        return load(type.getClassLoader(), Descriptions.typeOf(type));
    }

    /**
     * Loads the data model type using {@link MockDataModelLoader}.
     * @param classLoader the class loader for loading the data model type
     * @param type the target type
     * @return the resolved reference
     */
    public static DataModelReference load(ClassLoader classLoader, TypeDescription type) {
        return new MockDataModelLoader(classLoader).load(type);
    }
}
