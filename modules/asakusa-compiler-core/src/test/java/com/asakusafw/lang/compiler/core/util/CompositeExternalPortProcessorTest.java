package com.asakusafw.lang.compiler.core.util;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.api.reference.ExternalOutputReference;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.adapter.ExternalPortProcessorAdapter;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * Test for {@link CompositeExternalPortProcessor}.
 */
public class CompositeExternalPortProcessorTest extends CompilerTestRoot {

    private final DummyProcessor a = new DummyProcessor("a", DummyInA.class, DummyOutA.class);

    private final DummyProcessor b = new DummyProcessor("b", DummyInB.class, DummyOutB.class);

    /**
     * is supported.
     */
    @Test
    public void isSupported() {
        ExternalPortProcessor composite = CompositeExternalPortProcessor.composite(Arrays.asList(a, b));
        ExternalPortProcessor.Context context = context();
        assertThat(composite.toString(), composite.isSupported(context, DummyInA.class), is(true));
        assertThat(composite.toString(), composite.isSupported(context, DummyOutB.class), is(true));
        assertThat(composite.toString(), composite.isSupported(context, DummyInC.class), is(false));
    }

    /**
     * analyze input.
     */
    @Test
    public void analyzeInput() {
        ExternalPortProcessor composite = CompositeExternalPortProcessor.composite(Arrays.asList(a, b));
        ExternalPortProcessor.Context context = context();
        ExternalInputInfo analyzed = composite.analyzeInput(context, "p", new DummyInA());
        assertThat(analyzed.getModuleName(), is("a"));
    }

    /**
     * analyze input.
     */
    @Test(expected = DiagnosticException.class)
    public void analyzeInput_unsupported() {
        ExternalPortProcessor composite = CompositeExternalPortProcessor.composite(Arrays.asList(a, b));
        ExternalPortProcessor.Context context = context();
        composite.analyzeInput(context, "p", new DummyInC());
    }

    /**
     * analyze output.
     */
    @Test
    public void analyzeOutput() {
        ExternalPortProcessor composite = CompositeExternalPortProcessor.composite(Arrays.asList(a, b));
        ExternalPortProcessor.Context context = context();
        ExternalOutputInfo analyzed = composite.analyzeOutput(context, "p", new DummyOutA());
        assertThat(analyzed.getModuleName(), is("a"));
    }

    /**
     * analyze output.
     */
    @Test(expected = DiagnosticException.class)
    public void analyzeOutput_unsupported() {
        ExternalPortProcessor composite = CompositeExternalPortProcessor.composite(Arrays.asList(a, b));
        ExternalPortProcessor.Context context = context();
        composite.analyzeOutput(context, "p", new DummyOutC());
    }

    /**
     * resolve input.
     */
    @Test
    public void resolveInput() {
        ExternalPortProcessor composite = CompositeExternalPortProcessor.composite(Arrays.asList(a, b));
        ExternalPortProcessor.Context context = context();
        ExternalInputInfo analyzed = composite.analyzeInput(context, "p", new DummyInA());
        ExternalInputReference resolved = composite.resolveInput(context, "p", analyzed);
        assertThat(resolved.getModuleName(), is("a"));
    }

    /**
     * resolve output.
     */
    @Test
    public void resolveOutput() {
        ExternalPortProcessor composite = CompositeExternalPortProcessor.composite(Arrays.asList(a, b));
        ExternalPortProcessor.Context context = context();
        ExternalOutputInfo analyzed = composite.analyzeOutput(context, "p", new DummyOutA());
        ExternalOutputReference resolved = composite.resolveOutput(context, "p", analyzed, Collections.singleton("o"));
        assertThat(resolved.getModuleName(), is("a"));
    }

    /**
     * process.
     * @throws Exception if failed
     */
    @Test
    public void process() throws Exception {
        ExternalPortProcessor composite = CompositeExternalPortProcessor.composite(Arrays.asList(a, b));
        ExternalPortProcessor.Context context = context();
        List<ExternalInputReference> inputs = prepInputs(context, composite, new DummyInA(), new DummyInB());
        List<ExternalOutputReference> outputs = prepOutputs(context, composite, new DummyOutA(), new DummyOutB());
        composite.process(context, inputs, outputs);
        assertThat(a.worked, is(true));
        assertThat(b.worked, is(true));
    }

    /**
     * process.
     * @throws Exception if failed
     */
    @Test
    public void process_absent() throws Exception {
        ExternalPortProcessor composite = CompositeExternalPortProcessor.composite(Arrays.asList(a, b));
        ExternalPortProcessor.Context context = context();
        List<ExternalInputReference> inputs = prepInputs(context, composite, new DummyInA());
        List<ExternalOutputReference> outputs = prepOutputs(context, composite, new DummyOutA());
        composite.process(context, inputs, outputs);
        assertThat(a.worked, is(true));
        assertThat(b.worked, is(false));
    }

    /**
     * process.
     * @throws Exception if failed
     */
    @Test
    public void process_sparse() throws Exception {
        ExternalPortProcessor composite = CompositeExternalPortProcessor.composite(Arrays.asList(a, b));
        ExternalPortProcessor.Context context = context();
        List<ExternalInputReference> inputs = prepInputs(context, composite, new DummyInA());
        List<ExternalOutputReference> outputs = prepOutputs(context, composite, new DummyOutB());
        composite.process(context, inputs, outputs);
        assertThat(a.worked, is(true));
        assertThat(b.worked, is(true));
    }

    private List<ExternalInputReference> prepInputs(
            ExternalPortProcessor.Context context,
            ExternalPortProcessor processor,
            Object... objects) {
        List<ExternalInputReference> results = new ArrayList<>();
        for (int i = 0; i < objects.length; i++) {
            String name = "p" + i;
            ExternalInputInfo analyzed = processor.analyzeInput(context, name, objects[i]);
            ExternalInputReference resolved = processor.resolveInput(context, name, analyzed);
            results.add(resolved);
        }
        return results;
    }

    private List<ExternalOutputReference> prepOutputs(
            ExternalPortProcessor.Context context,
            ExternalPortProcessor processor,
            Object... objects) {
        List<ExternalOutputReference> results = new ArrayList<>();
        for (int i = 0; i < objects.length; i++) {
            String name = "p" + i;
            ExternalOutputInfo analyzed = processor.analyzeOutput(context, name, objects[i]);
            ExternalOutputReference resolved = processor.resolveOutput(context, name, analyzed, Collections.singleton(name));
            results.add(resolved);
        }
        return results;
    }

    private ExternalPortProcessor.Context context() {
        return new ExternalPortProcessorAdapter(
                new JobflowCompiler.Context(context(true), container()),
                "dummybatch", "dummyflow");
    }

    private static class DummyInA {
        public DummyInA() {
            return;
        }
    }
    private static class DummyInB {
        public DummyInB() {
            return;
        }
    }
    private static class DummyInC {
        public DummyInC() {
            return;
        }
    }

    private static class DummyOutA {
        public DummyOutA() {
            return;
        }
    }
    private static class DummyOutB {
        public DummyOutB() {
            return;
        }
    }
    private static class DummyOutC {
        public DummyOutC() {
            return;
        }
    }

    private static class DummyProcessor implements ExternalPortProcessor {

        private final String moduleName;

        private final Class<?> supportedInputClass;

        private final Class<?> supportedOutputClass;

        boolean worked;

        public DummyProcessor(String moduleName, Class<?> input, Class<?> output) {
            this.moduleName = moduleName;
            this.supportedInputClass = input;
            this.supportedOutputClass = output;
        }

        @Override
        public boolean isSupported(AnalyzeContext context, Class<?> descriptionClass) {
            return descriptionClass == supportedInputClass || descriptionClass == supportedOutputClass;
        }

        @Override
        public ExternalInputInfo analyzeInput(AnalyzeContext context, String name, Object description) {
            assertThat(description, is(instanceOf(supportedInputClass)));
            return new ExternalInputInfo.Basic(
                    Descriptions.classOf(supportedInputClass),
                    moduleName,
                    Descriptions.classOf(String.class),
                    ExternalInputInfo.DataSize.UNKNOWN);
        }

        @Override
        public ExternalOutputInfo analyzeOutput(AnalyzeContext context, String name, Object description) {
            assertThat(description, is(instanceOf(supportedOutputClass)));
            return new ExternalOutputInfo.Basic(
                    Descriptions.classOf(supportedOutputClass),
                    moduleName,
                    Descriptions.classOf(String.class));
        }

        @Override
        public void validate(
                AnalyzeContext context,
                Map<String, ExternalInputInfo> inputs, Map<String, ExternalOutputInfo> outputs) {
            return;
        }

        @Override
        public ExternalInputReference resolveInput(Context context, String name, ExternalInputInfo info) {
            assertThat(info.getDescriptionClass(), is(Descriptions.classOf(supportedInputClass)));
            return new ExternalInputReference(name, info, Collections.singleton(name));
        }

        @Override
        public ExternalOutputReference resolveOutput(Context context, String name, ExternalOutputInfo info,
                Collection<String> internalOutputPaths) {
            assertThat(info.getDescriptionClass(), is(Descriptions.classOf(supportedOutputClass)));
            return new ExternalOutputReference(name, info, Collections.singleton(name));
        }

        @Override
        public void process(Context context, List<ExternalInputReference> inputs, List<ExternalOutputReference> outputs) {
            assertThat(worked, is(false));
            for (ExternalInputReference port : inputs) {
                assertThat(port.getDescriptionClass(), is(Descriptions.classOf(supportedInputClass)));
            }
            for (ExternalOutputReference port : outputs) {
                assertThat(port.getDescriptionClass(), is(Descriptions.classOf(supportedOutputClass)));
            }
            worked = true;
        }
    }
}
