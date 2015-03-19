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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Groups;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.graph.FlowBoundary;
import com.asakusafw.vocabulary.flow.graph.FlowElementDescription;
import com.asakusafw.vocabulary.flow.graph.FlowGraph;
import com.asakusafw.vocabulary.flow.graph.FlowPartDescription;
import com.asakusafw.vocabulary.flow.graph.InputDescription;
import com.asakusafw.vocabulary.flow.graph.ObservationCount;
import com.asakusafw.vocabulary.flow.graph.OutputDescription;
import com.asakusafw.vocabulary.flow.util.CoreOperators;
import com.asakusafw.vocabulary.model.Key;

/**
 * Test for {@link FlowGraphAnalyzer}.
 */
public class FlowGraphAnalyzerTest {

    private final FlowGraphAnalyzer converter = new FlowGraphAnalyzer(new MockExternalPortAnalyzer());

    /**
     * simple case.
     */
    @Test
    public void simple() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .output("d0", "p");

        inspector
            .operators(2)
            .connections(1)
            .connected("s0", "d0");
    }

    /**
     * ignore pseudo elements.
     */
    @Test
    public void pseudo() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .pseudo("o0", String.class)
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .output("d0", "p");

        inspector
            .operators(2)
            .connections(1)
            .connected("s0", "d0");
    }

    /**
     * w/ user operator.
     */
    @Test
    public void user() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .operator("o0", Mock.class, "simple")
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .operator("o0", Mock.class, "simple")
            .output("d0", "p");

        inspector
            .operators(3)
            .connections(2)
            .connected("s0", "o0")
            .connected("o0", "d0");
    }

    /**
     * w/ core operator.
     */
    @Test
    public void core() {
        FlowElementDescription extend = CoreOperators.extend(CoreOperators.empty(String.class), StringBuilder.class)
                .out
                .toOutputPort()
                .getOwner()
                .getDescription();
        FlowElementDescription project = CoreOperators.project(CoreOperators.empty(StringBuilder.class), String.class)
                .out
                .toOutputPort()
                .getOwner()
                .getDescription();

        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .add("o0", extend)
            .add("o1", project)
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "o1")
            .connect("o1", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .operator("o0", CoreOperatorKind.EXTEND)
            .operator("o1", CoreOperatorKind.PROJECT)
            .output("d0", "p");

        inspector
            .operators(4)
            .connections(3)
            .connected("s0", "o0")
            .connected("o0", "o1")
            .connected("o1", "d0");
    }

    /**
     * w/ checkpoint.
     */
    @Test
    public void checkpoint() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .pseudo("o0", String.class, FlowBoundary.STAGE)
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .operator("o0", CoreOperatorKind.CHECKPOINT)
            .output("d0", "p");

        inspector
            .operators(3)
            .connections(2)
            .connected("s0", "o0")
            .connected("o0", "d0");
    }

    /**
     * w/ flow-part.
     */
    @Test
    public void flowpart() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .add("o0", new FlowPartDescription(new MockFlowGraph()
                    .add("s1", new InputDescription("p", String.class))
                    .add("d1", new OutputDescription("p", String.class))
                    .connect("s1", "d1")
                    .toGraph()))
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .flowpart("o0", FlowDescription.class)
            .enter("o0")
                .input("s1", "p")
                .output("d1", "p")
                .exit()
            .output("d0", "p");

        inspector
            .operators(3)
            .connections(2)
            .connected("s0", "o0")
            .connected("o0", "d0")
            .enter("o0")
                .operators(2)
                .connections(1)
                .connected("s1", "d1")
                .exit();
    }

    /**
     * w/ input.
     */
    @Test
    public void input() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", new InputDesc(String.class)))
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .output("d0", "p");

        inspector
            .operators(2)
            .connections(1)
            .connected("s0", "d0");

        ExternalInput s0 = (ExternalInput) inspector.get("s0");
        assertThat(s0.getInfo(), is(notNullValue()));
        assertThat(s0.getInfo().getDescriptionClass(), is(Descriptions.classOf(InputDesc.class)));
        assertThat(s0.getInfo().getDataModelClass(), is(Descriptions.classOf(String.class)));
    }

    /**
     * w/ output.
     */
    @Test
    public void output() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .add("d0", new OutputDescription("p", new OutputDesc(String.class)))
            .connect("s0", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .output("d0", "p");

        inspector
            .operators(2)
            .connections(1)
            .connected("s0", "d0");

        ExternalOutput d0 = (ExternalOutput) inspector.get("d0");
        assertThat(d0.getInfo(), is(notNullValue()));
        assertThat(d0.getInfo().getDescriptionClass(), is(Descriptions.classOf(OutputDesc.class)));
        assertThat(d0.getInfo().getDataModelClass(), is(Descriptions.classOf(String.class)));
    }

    /**
     * w/ user operator and its arguments.
     */
    @Test
    public void user_w_arguments() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .operator("o0", Mock.class, "parameterized", Collections.singletonMap("argument", 100))
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .operator("o0", Mock.class, "parameterized")
            .output("d0", "p");

        inspector
            .operators(3)
            .connections(2)
            .connected("s0", "o0")
            .connected("o0", "d0");

        assertThat(inspector.getArgument("o0.argument").getValue(), is(Descriptions.valueOf(100)));
    }

    /**
     * w/ flow-part and its arguments.
     */
    @Test
    public void flowpart_w_arguments() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .add("o0", new FlowPartDescription(new MockFlowGraph()
                    .add("s1", new InputDescription("p", String.class))
                    .add("d1", new OutputDescription("p", String.class))
                    .connect("s1", "d1")
                    .toGraph(),
                    Arrays.asList(new FlowPartDescription.Parameter("argument", int.class, 100))))
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .flowpart("o0", FlowDescription.class)
            .enter("o0")
                .input("s1", "p")
                .output("d1", "p")
                .exit()
            .output("d0", "p");

        inspector
            .operators(3)
            .connections(2)
            .connected("s0", "o0")
            .connected("o0", "d0")
            .enter("o0")
                .operators(2)
                .connections(1)
                .connected("s1", "d1")
                .exit();

        assertThat(inspector.getArgument("o0.argument").getValue(), is(Descriptions.valueOf(100)));
    }

    /**
     * w/ shuffle key.
     */
    @Test
    public void shuffle() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .operator("o0", Mock.class, "shuffle")
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .operator("o0", Mock.class, "shuffle")
            .output("d0", "p");

        inspector
            .operators(3)
            .connections(2)
            .connected("s0", "o0")
            .connected("o0", "d0");

        assertThat(
                inspector.getInput("o0").getGroup(),
                is(Groups.parse(Arrays.asList("key"), Arrays.asList("+sort_a", "-sort_b"))));
    }

    /**
     * w/ operator constraints.
     */
    @Test
    public void constraints() {
        Map<String, Object> args = Collections.<String, Object>emptyMap();
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .operator("o0", Mock.class, "o0", args, ObservationCount.DONT_CARE)
            .operator("o1", Mock.class, "o1", args, ObservationCount.AT_LEAST_ONCE)
            .operator("o2", Mock.class, "o2", args, ObservationCount.AT_MOST_ONCE)
            .operator("o3", Mock.class, "o3", args, ObservationCount.EXACTLY_ONCE)
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "o1")
            .connect("o1", "o2")
            .connect("o2", "o3")
            .connect("o3", "d0")
            .toGraph();

        OperatorGraphInspector inspector = new OperatorGraphInspector(converter.analyze(g))
            .input("s0", "p")
            .operator("o0", Mock.class, "o0")
            .operator("o1", Mock.class, "o1")
            .operator("o2", Mock.class, "o2")
            .operator("o3", Mock.class, "o3")
            .output("d0", "p");

        assertThat(inspector.get("o0").getConstraints(), is(empty()));
        assertThat(inspector.get("o1").getConstraints(), containsInAnyOrder(OperatorConstraint.AT_LEAST_ONCE));
        assertThat(inspector.get("o2").getConstraints(), containsInAnyOrder(OperatorConstraint.AT_MOST_ONCE));
        assertThat(inspector.get("o3").getConstraints(), containsInAnyOrder(
                OperatorConstraint.AT_LEAST_ONCE, OperatorConstraint.AT_MOST_ONCE));
    }

    /**
     * w/ cyclic.
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_cyclic() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", String.class))
            .operator("o0", Mock.class, "simple")
            .operator("o1", Mock.class, "simple")
            .operator("o2", Mock.class, "simple")
            .add("d0", new OutputDescription("p", String.class))
            .connect("s0", "o0")
            .connect("o0", "o1")
            .connect("o1", "o2")
            .connect("o2", "o0")
            .connect("o0", "d0")
            .toGraph();

        converter.analyze(g);
    }

    /**
     * external inputs w/ conflict.
     */
    @Test(expected = DiagnosticException.class)
    public void input_conflict() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("conflict", new InputDesc(String.class)))
            .add("s1", new InputDescription("conflict", new InputDesc(String.class)))
            .add("d0", new OutputDescription("p", new OutputDesc(String.class)))
            .connect("s0", "d0")
            .connect("s1", "d0")
            .toGraph();

        converter.analyze(g);
    }

    /**
     * external outputs w/ conflict.
     */
    @Test(expected = DiagnosticException.class)
    public void output_conflict() {
        FlowGraph g = new MockFlowGraph()
            .add("s0", new InputDescription("p", new InputDesc(String.class)))
            .add("d0", new OutputDescription("conflict", new OutputDesc(String.class)))
            .add("d1", new OutputDescription("conflict", new OutputDesc(String.class)))
            .connect("s0", "d0")
            .connect("s0", "d1")
            .toGraph();

        converter.analyze(g);
    }

    private static abstract class Mock {

        @MockOperator
        public abstract void simple(String in, Result<String> out);

        @MockOperator(parameters = { "in", "out", "argument" })
        public abstract void parameterized(String in, Result<String> out, int argument);

        @MockOperator
        public abstract void shuffle(
                @Key(group = "key", order = { "+sortA", "-sortB" }) List<String> in,
                Result<String> out);

        @MockOperator
        public abstract void o0(String in, Result<String> out);

        @MockOperator
        public abstract void o1(String in, Result<String> out);

        @MockOperator
        public abstract void o2(String in, Result<String> out);

        @MockOperator
        public abstract void o3(String in, Result<String> out);
    }

    private static class InputDesc implements ImporterDescription {

        private final Class<?> type;

        public InputDesc(Class<?> type) {
            this.type = type;
        }

        @Override
        public Class<?> getModelType() {
            return type;
        }

        @Override
        public DataSize getDataSize() {
            return DataSize.UNKNOWN;
        }
    }

    private static class OutputDesc implements ExporterDescription {

        private final Class<?> type;

        public OutputDesc(Class<?> type) {
            this.type = type;
        }

        @Override
        public Class<?> getModelType() {
            return type;
        }
    }
}
