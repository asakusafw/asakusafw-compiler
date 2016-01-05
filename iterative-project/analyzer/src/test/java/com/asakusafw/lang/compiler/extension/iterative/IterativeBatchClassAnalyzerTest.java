/**
 * Copyright 2011-2016 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.lang.compiler.extension.iterative;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.testing.MockExternalPortProcessor;
import com.asakusafw.lang.compiler.core.ClassAnalyzer;
import com.asakusafw.lang.compiler.core.ProjectRepository;
import com.asakusafw.lang.compiler.core.ToolRepository;
import com.asakusafw.lang.compiler.core.util.CompositeClassAnalyzer;
import com.asakusafw.lang.compiler.extension.iterative.mock.MockBatch;
import com.asakusafw.lang.compiler.extension.iterative.mock.MockJobflow;
import com.asakusafw.lang.compiler.extension.iterative.mock.PlainBatch;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.BatchElement;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.iterative.IterativeExtension;

/**
 * Test for {@link IterativeBatchClassAnalyzer}.
 */
public class IterativeBatchClassAnalyzerTest {

    /**
     * is batch.
     */
    @Test
    public void is_batch() {
        CompositeClassAnalyzer analyzer = new CompositeClassAnalyzer();
        ClassAnalyzer.Context context = context();
        assertThat(analyzer.isBatchClass(context, PlainBatch.class), is(true));
        assertThat(analyzer.isBatchClass(context, MockBatch.class), is(true));
        assertThat(analyzer.isBatchClass(context, MockJobflow.class), is(false));
        assertThat(analyzer.isBatchClass(context, String.class), is(false));
    }

    /**
     * is jobflow.
     */
    @Test
    public void is_jobflow() {
        CompositeClassAnalyzer analyzer = new CompositeClassAnalyzer();
        ClassAnalyzer.Context context = context();
        assertThat(analyzer.isJobflowClass(context, PlainBatch.class), is(false));
        assertThat(analyzer.isJobflowClass(context, MockBatch.class), is(true));
        assertThat(analyzer.isJobflowClass(context, MockJobflow.class), is(true));
        assertThat(analyzer.isJobflowClass(context, String.class), is(false));
    }

    /**
     * analyze batch.
     */
    @Test
    public void analyze_batch() {
        CompositeClassAnalyzer analyzer = new CompositeClassAnalyzer();
        ClassAnalyzer.Context context = context();

        Batch batch = analyzer.analyzeBatch(context, MockBatch.class);
        assertThat(batch.getElements(), hasSize(1));
        BatchElement element = batch.findElement(IterativeBatchAnalyzer.FLOW_ID);
        assertThat(element, is(notNullValue()));
        Jobflow flow = element.getJobflow();

        ExternalInput input = flow.getOperatorGraph().getInputs().get("in");
        assertThat(input, is(notNullValue()));

        IterativeExtension extension = input.getAttribute(IterativeExtension.class);
        assertThat(extension, is(notNullValue()));
        assertThat(extension.isScoped(), is(true));
        assertThat(extension.getParameters(), containsInAnyOrder("testing"));
    }

    /**
     * analyze jobflow.
     */
    @Test
    public void analyze_jobflow() {
        CompositeClassAnalyzer analyzer = new CompositeClassAnalyzer();
        ClassAnalyzer.Context context = context();

        Jobflow flow = analyzer.analyzeJobflow(context, MockBatch.class);
        assertThat(flow.getFlowId(), is(IterativeBatchAnalyzer.FLOW_ID));

        ExternalInput input = flow.getOperatorGraph().getInputs().get("in");
        assertThat(input, is(notNullValue()));

        IterativeExtension extension = input.getAttribute(IterativeExtension.class);
        assertThat(extension, is(notNullValue()));
        assertThat(extension.isScoped(), is(true));
        assertThat(extension.getParameters(), containsInAnyOrder("testing"));
    }

    private ClassAnalyzer.Context context() {
        try {
            return new ClassAnalyzer.Context(
                ProjectRepository.builder(IterativeBatchClassAnalyzerTest.class.getClassLoader())
                    .build(),
                ToolRepository.builder(IterativeBatchClassAnalyzerTest.class.getClassLoader())
                    .use(new MockExternalPortProcessor())
                    .useDefaults()
                    .build());
        } catch (IOException e) {
            throw new AssertionError();
        }
    }
}
