package com.asakusafw.lang.compiler.api.mock;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.ExternalIoProcessor;
import com.asakusafw.lang.compiler.api.basic.AbstractExternalIoProcessorContext;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.model.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * Mock implementation of {@link ExternalIoProcessor}.
 */
public class MockExternalIoProcessor implements ExternalIoProcessor {

    private static final String MODULE_NAME = "mock";

    /**
     * A dummy context for this processor.
     */
    public static final Context CONTEXT = new AbstractExternalIoProcessorContext() {
        @Override
        public CompilerOptions getOptions() {
            throw new UnsupportedOperationException();
        }
        @Override
        public DataModelLoader getDataModelLoader() {
            throw new UnsupportedOperationException();
        }
        @Override
        public ClassLoader getClassLoader() {
            return MockExternalIoProcessor.class.getClassLoader();
        }
        @Override
        public OutputStream addResourceFile(Location location) throws IOException {
            throw new UnsupportedOperationException();
        }
    };

    @Override
    public boolean isSupported(Context context, ClassDescription descriptionClass) {
        try {
            Class<?> aClass = descriptionClass.resolve(context.getClassLoader());
            return ImporterDescription.class.isAssignableFrom(aClass)
                    || ExporterDescription.class.isAssignableFrom(aClass);
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    public ExternalInputInfo resolveInput(Context context, String name, Object description) {
        if ((description instanceof ImporterDescription) == false) {
            throw new IllegalArgumentException();
        }
        ImporterDescription desc = (ImporterDescription) description;
        return new ExternalInputInfo.Basic(
                Descriptions.classOf(desc.getClass()),
                MODULE_NAME,
                Descriptions.classOf(desc.getModelType()),
                ExternalInputInfo.DataSize.valueOf(desc.getDataSize().name()),
                Collections.<String, ValueDescription>emptyMap());
    }

    @Override
    public ExternalOutputInfo resolveOutput(Context context, String name, Object description) {
        if ((description instanceof ExporterDescription) == false) {
            throw new IllegalArgumentException();
        }
        ExporterDescription desc = (ExporterDescription) description;
        return new ExternalOutputInfo.Basic(
                Descriptions.classOf(desc.getClass()),
                MODULE_NAME,
                Descriptions.classOf(desc.getModelType()),
                Collections.<String, ValueDescription>emptyMap());
    }

    @Override
    public void process(Context context,
            List<ExternalInputReference> inputs,
            List<ExternalOutputReference> outputs) throws IOException {
        return;
    }
}
