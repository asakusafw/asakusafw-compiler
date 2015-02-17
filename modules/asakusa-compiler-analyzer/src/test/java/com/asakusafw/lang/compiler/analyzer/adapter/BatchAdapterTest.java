package com.asakusafw.lang.compiler.analyzer.adapter;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.analyzer.mock.AbstractBatch;
import com.asakusafw.lang.compiler.analyzer.mock.BatchWithAttributes;
import com.asakusafw.lang.compiler.analyzer.mock.BatchWithWrongName;
import com.asakusafw.lang.compiler.analyzer.mock.BatchWithoutAnnotation;
import com.asakusafw.lang.compiler.analyzer.mock.BatchWithoutDescription;
import com.asakusafw.lang.compiler.analyzer.mock.BatchWithoutValidConstructor;
import com.asakusafw.lang.compiler.analyzer.mock.MockBatch;
import com.asakusafw.lang.compiler.analyzer.mock.NotPublicAccessor;
import com.asakusafw.lang.compiler.api.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.vocabulary.batch.Batch;
import com.asakusafw.vocabulary.batch.BatchDescription;

/**
 * Test for {@link BatchAdapter}.
 */
public class BatchAdapterTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        assertThat(BatchAdapter.isBatch(MockBatch.class), is(true));
        BatchAdapter adapter = BatchAdapter.analyze(MockBatch.class);

        BatchInfo info = adapter.getInfo();
        assertThat(info.getBatchId(), is("mock"));
        assertThat(info.getDescriptionClass(), is(Descriptions.classOf(MockBatch.class)));
        assertThat(info.getComment(), is(nullValue()));
        assertThat(info.getParameters(), is(empty()));
        assertThat(info.getAttributes(), not(hasItem(BatchInfo.Attribute.STRICT_PARAMETERS)));
        assertThat(adapter.getDescription(), is((Object) MockBatch.class));
        assertThat(adapter.getConstructor().getDeclaringClass(), is((Object) MockBatch.class));
        assertThat(adapter.getConstructor().getParameterTypes().length, is(0));
        assertThat(adapter.newInstance(), is(instanceOf(MockBatch.class)));
    }

    /**
     * batch with parameters.
     */
    @Test
    public void parameters() {
        BatchAdapter adapter = BatchAdapter.analyze(BatchWithAttributes.class);

        BatchInfo info = adapter.getInfo();
        assertThat(info.getBatchId(), is("BatchWithAttributes"));
        assertThat(info.getComment(), is("testing"));
        assertThat(info.getParameters(), hasSize(2));
        assertThat(info.getAttributes(), hasItem(BatchInfo.Attribute.STRICT_PARAMETERS));

        BatchInfo.Parameter p0 = findParameter(info, "a");
        assertThat(p0.getKey(), is("a"));
        assertThat(p0.getComment(), is("A"));
        assertThat(p0.isMandatory(), is(false));
        assertThat(p0.getPattern(), is(notNullValue()));
        assertThat(p0.getPattern().pattern(), is("a+"));

        BatchInfo.Parameter p1 = findParameter(info, "b");
        assertThat(p1.getKey(), is("b"));
        assertThat(p1.getComment(), is(nullValue()));
        assertThat(p1.isMandatory(), is(true));
        assertThat(p1.getPattern(), is(nullValue()));

    }

    private BatchInfo.Parameter findParameter(BatchInfo info, String key) {
        for (BatchInfo.Parameter parameter : info.getParameters()) {
            if (parameter.getKey().equals(key)) {
                return parameter;
            }
        }
        throw new AssertionError(key);
    }

    /**
     * batch class must be annotated.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_wo_annotation() {
        assertThat(BatchAdapter.isBatch(BatchWithoutAnnotation.class), is(false));
        BatchAdapter.analyze(BatchWithoutAnnotation.class);
    }

    /**
     * batch class must inherit {@link BatchDescription}.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_wo_description() {
        assertThat(BatchAdapter.isBatch(BatchWithoutDescription.class), is(false));
        BatchAdapter.analyze(BatchWithoutDescription.class);
    }

    /**
     * batch class must have valid ID.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_w_wrong_name() {
        BatchAdapter.analyze(BatchWithWrongName.class);
    }

    /**
     * batch class must be top-level.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_not_top_level() {
        BatchAdapter.analyze(NotTopLevelBatch.class);
    }

    /**
     * batch class must be public.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_not_public() {
        BatchAdapter.analyze(NotPublicAccessor.getBatch());
    }

    /**
     * batch class must not be abstract.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_abstract() {
        BatchAdapter.analyze(AbstractBatch.class);
    }

    /**
     * batch class must have public zero-arguments constructor.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_no_default_ctor() {
        BatchAdapter.analyze(BatchWithoutValidConstructor.class);
    }

    @SuppressWarnings("javadoc")
    @Batch(name = "NotTopLevelBatch")
    public static final class NotTopLevelBatch extends BatchDescription {
        @Override
        protected void describe() {
            return;
        }
    }
}
