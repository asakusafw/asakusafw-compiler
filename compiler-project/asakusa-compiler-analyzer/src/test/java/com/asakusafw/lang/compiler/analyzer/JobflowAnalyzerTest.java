/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.analyzer;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

import com.asakusafw.lang.compiler.analyzer.mock.JobflowWithInconsistentInput;
import com.asakusafw.lang.compiler.analyzer.mock.JobflowWithInconsistentOutput;
import com.asakusafw.lang.compiler.analyzer.mock.JobflowWithInvalidInput;
import com.asakusafw.lang.compiler.analyzer.mock.JobflowWithInvalidOutput;
import com.asakusafw.lang.compiler.analyzer.mock.JobflowWithWrongConstructor;
import com.asakusafw.lang.compiler.analyzer.mock.JobflowWithWrongDescription;
import com.asakusafw.lang.compiler.analyzer.mock.MockExporterDescription;
import com.asakusafw.lang.compiler.analyzer.mock.MockImporterDescription;
import com.asakusafw.lang.compiler.analyzer.mock.MockJobflow;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.Operators;

/**
 * Test for {@link JobflowAnalyzer}.
 */
public class JobflowAnalyzerTest {

    private final JobflowAnalyzer analyzer = new JobflowAnalyzer(new FlowGraphAnalyzer(new MockExternalPortAnalyzer()));

    /**
     * simple case.
     */
    @Test
    public void simple() {
        Jobflow result = analyzer.analyze(MockJobflow.class);
        assertThat(result.getFlowId(), is("mock"));
        assertThat(result.getDescriptionClass(), is(Descriptions.classOf(MockJobflow.class)));
        OperatorGraph graph = result.getOperatorGraph();

        assertThat(graph.getInputs().keySet(), containsInAnyOrder("in"));
        assertThat(graph.getOutputs().keySet(), containsInAnyOrder("out"));

        ExternalInput in = graph.getInputs().get("in");
        assertThat(in.getName(), is("in"));
        assertThat(in.isExternal(), is(true));
        assertThat(in.getInfo().getDescriptionClass(), is(Descriptions.classOf(MockImporterDescription.class)));

        Operator op = succ(in);
        assertThat(op.getOperatorKind(), is(OperatorKind.CORE));
        assertThat(((CoreOperator) op).getCoreOperatorKind(), is(CoreOperatorKind.CHECKPOINT));

        ExternalOutput out = graph.getOutputs().get("out");
        assertThat(succ(op), is(sameInstance((Object) out)));
        assertThat(out.getName(), is("out"));
        assertThat(out.isExternal(), is(true));
        assertThat(out.getInfo().getDescriptionClass(), is(Descriptions.classOf(MockExporterDescription.class)));
    }

    /**
     * jobflow with inconsistent input.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_inconsistent_input() {
        analyzer.analyze(JobflowWithInconsistentInput.class);
    }

    /**
     * jobflow with invalid importer.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_importer() {
        analyzer.analyze(JobflowWithInvalidInput.class);
    }

    /**
     * jobflow with inconsistent output.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_inconsistent_output() {
        analyzer.analyze(JobflowWithInconsistentOutput.class);
    }

    /**
     * jobflow with invalid exporter.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_exporter() {
        analyzer.analyze(JobflowWithInvalidOutput.class);
    }

    /**
     * jobflow with wrong constructor.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_wrong_constructor() {
        analyzer.analyze(JobflowWithWrongConstructor.class);
    }

    /**
     * jobflow with wrong description.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_wrong_description() {
        analyzer.analyze(JobflowWithWrongDescription.class);
    }

    private Operator succ(Operator operator) {
        Set<Operator> succ = Operators.getSuccessors(operator);
        assertThat(succ, hasSize(1));
        return succ.iterator().next();
    }
}
