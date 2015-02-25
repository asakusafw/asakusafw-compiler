package com.asakusafw.lang.compiler.cli.mock;

import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.DataModelLoaderFactory;

/**
 * Mock {@link DataModelLoaderFactory}.
 */
@SuppressWarnings("javadoc")
public class DummyDataModelLoaderFactory implements DataModelLoaderFactory, DummyElement {

    final String id;

    public DummyDataModelLoaderFactory() {
        this("default");
    }

    public DummyDataModelLoaderFactory(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public DataModelLoader get(ClassLoader classLoader) {
        return new DummyDataModelLoader(id);
    }
}
