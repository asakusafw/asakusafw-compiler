package com.asakusafw.lang.compiler.cli.mock;

import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.DataModelProcessor;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.model.description.TypeDescription;

/**
 * Mock {@link DataModelLoader}.
 */
@SuppressWarnings("javadoc")
public class DummyDataModelProcessor implements DataModelProcessor, DummyElement {

    final String id;

    public DummyDataModelProcessor() {
        this("default");
    }

    public DummyDataModelProcessor(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isSupported(Context context, TypeDescription type) {
        return false;
    }

    @Override
    public DataModelReference process(Context context, TypeDescription type) {
        throw new UnsupportedOperationException();
    }
}
