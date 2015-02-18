package com.asakusafw.lang.compiler.analyzer;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.analyzer.mock.BatchWithDuplicateJobflow;
import com.asakusafw.lang.compiler.analyzer.mock.BatchWithWrongConstructor;
import com.asakusafw.lang.compiler.analyzer.mock.BatchWithWrongDescription;
import com.asakusafw.lang.compiler.analyzer.mock.BatchWithWrongJobflow;
import com.asakusafw.lang.compiler.analyzer.mock.MockBatch;
import com.asakusafw.lang.compiler.analyzer.mock.MockDiamondBatch;
import com.asakusafw.lang.compiler.analyzer.mock.MockJobflow;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.BatchElement;

/**
 * Test for {@link BatchAnalyzer}.
 */
public class BatchAnalyzerTest {

    private final BatchAnalyzer analyzer =
            new BatchAnalyzer(new JobflowAnalyzer(new FlowGraphAnalyzer(new MockExternalIoAnalyzer())));

    /**
     * simple case.
     */
    @Test
    public void simple() {
        Batch result = analyzer.analyze(MockBatch.class);
        assertThat(result.getBatchId(), is("mock"));
        assertThat(result.getDescriptionClass(), is(Descriptions.classOf(MockBatch.class)));
        assertThat(result.getElements(), hasSize(1));

        BatchElement element = result.findElement("mock");
        assertThat(element, is(notNullValue()));
        assertThat(element.getBlockerElements(), is(empty()));
        assertThat(element.getBlockingElements(), is(empty()));
        assertThat(element.getJobflow().getFlowId(), is("mock"));
        assertThat(element.getJobflow().getDescriptionClass(), is(Descriptions.classOf(MockJobflow.class)));
    }

    /**
     * w/ dependencies.
     */
    @Test
    public void dependencies() {
        Batch result = analyzer.analyze(MockDiamondBatch.class);
        assertThat(result.getElements(), hasSize(4));

        BatchElement a = result.findElement("a");
        BatchElement b = result.findElement("b");
        BatchElement c = result.findElement("c");
        BatchElement d = result.findElement("d");
        assertThat(a, is(notNullValue()));
        assertThat(b, is(notNullValue()));
        assertThat(c, is(notNullValue()));
        assertThat(d, is(notNullValue()));

        assertThat(a.getBlockerElements(), is(empty()));
        assertThat(b.getBlockerElements(), containsInAnyOrder(a));
        assertThat(c.getBlockerElements(), containsInAnyOrder(a));
        assertThat(d.getBlockerElements(), containsInAnyOrder(b, c));

        assertThat(a.getBlockingElements(), containsInAnyOrder(b, c));
        assertThat(b.getBlockingElements(), containsInAnyOrder(d));
        assertThat(c.getBlockingElements(), containsInAnyOrder(d));
        assertThat(d.getBlockingElements(), is(empty()));
    }

    /**
     * batch with wrong constructor.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_wrong_constructor() {
        analyzer.analyze(BatchWithWrongConstructor.class);
    }

    /**
     * batch with wrong description.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_wrong_description() {
        analyzer.analyze(BatchWithWrongDescription.class);
    }

    /**
     * batch with duplicated element.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_duplicate_element() {
        analyzer.analyze(BatchWithDuplicateJobflow.class);
    }

    /**
     * batch with wrong element.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_wrong_element() {
        analyzer.analyze(BatchWithWrongJobflow.class);
    }
}
