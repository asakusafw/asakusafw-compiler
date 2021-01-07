/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import org.junit.Test;

import com.asakusafw.lang.compiler.analyzer.FlowGraphAnalyzer;
import com.asakusafw.lang.compiler.analyzer.JobflowAnalyzer;
import com.asakusafw.lang.compiler.extension.iterative.mock.BatchWithAttributes;
import com.asakusafw.lang.compiler.extension.iterative.mock.MockBatch;
import com.asakusafw.lang.compiler.extension.iterative.mock.MockExternalPortAnalyzer;
import com.asakusafw.lang.compiler.extension.iterative.mock.MockJobflow;
import com.asakusafw.lang.compiler.extension.iterative.mock.PlainBatch;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.BatchElement;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.iterative.IterativeExtension;

/**
 * Test for {@link IterativeBatchAnalyzer}.
 */
public class IterativeBatchAnalyzerTest {

    /**
     * is batch.
     */
    @Test
    public void is_batch() {
        assertThat(IterativeBatchAnalyzer.isBatch(PlainBatch.class), is(false));
        assertThat(IterativeBatchAnalyzer.isBatch(MockBatch.class), is(true));
        assertThat(IterativeBatchAnalyzer.isBatch(MockJobflow.class), is(false));
    }

    /**
     * analyze info.
     */
    @Test
    public void info_simple() {
        BatchInfo info = IterativeBatchAnalyzer.analyzeInfo(MockBatch.class);
        assertThat(info.getBatchId(), is("MockBatch"));
        assertThat(info.getDescriptionClass(), is(Descriptions.classOf(MockBatch.class)));
        assertThat(info.getComment(), is(nullValue()));
        assertThat(info.getParameters(), is(empty()));
        assertThat(info.getAttributes(), not(hasItem(BatchInfo.Attribute.STRICT_PARAMETERS)));
    }

    /**
     * batch with parameters.
     */
    @Test
    public void info_parameters() {
        BatchInfo info = IterativeBatchAnalyzer.analyzeInfo(BatchWithAttributes.class);
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

    /**
     * analyze - simple.
     */
    @Test
    public void analyze_simple() {
        IterativeBatchAnalyzer analyzer = new IterativeBatchAnalyzer(
                new JobflowAnalyzer(
                        new FlowGraphAnalyzer(
                                new MockExternalPortAnalyzer(),
                                new IterativeOperatorAnalyzer())));
        Batch batch = analyzer.analyze(MockBatch.class);
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

    private BatchInfo.Parameter findParameter(BatchInfo info, String key) {
        for (BatchInfo.Parameter parameter : info.getParameters()) {
            if (parameter.getKey().equals(key)) {
                return parameter;
            }
        }
        throw new AssertionError(key);
    }
}
