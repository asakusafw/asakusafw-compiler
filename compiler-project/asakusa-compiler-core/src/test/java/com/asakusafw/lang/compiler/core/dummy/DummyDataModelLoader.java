package com.asakusafw.lang.compiler.core.dummy;

import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Mock {@link DataModelLoader}.
 */
@SuppressWarnings("javadoc")
public class DummyDataModelLoader implements DataModelLoader, DummyElement {

    final String id;

    public DummyDataModelLoader() {
        this("default");
    }

    public DummyDataModelLoader(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public DataModelReference load(TypeDescription type) {
        throw new UnsupportedOperationException();
    }
}
