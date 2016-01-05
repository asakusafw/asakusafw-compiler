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
package com.asakusafw.lang.compiler.inspection;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.asakusafw.lang.compiler.inspection.BasicObjectInspector;
import com.asakusafw.lang.compiler.inspection.DslDriver;
import com.asakusafw.lang.compiler.inspection.ObjectInspector;
import com.asakusafw.lang.compiler.inspection.Util;
import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.BatchElement;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.FlowOperator;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.PlanMarkers;
import com.asakusafw.lang.inspection.InspectionNode;
import com.asakusafw.lang.inspection.InspectionNode.Port;
import com.asakusafw.lang.inspection.InspectionNode.PortReference;

/**
 * Test for {@link DslDriver}.
 */
public class DslDriverTest {

    private final DslDriver driver = new DslDriver();

    /**
     * inspects core operator.
     */
    @Test
    public void inspect_core() {
        InspectionNode node = driver.inspect("testing", CoreOperator.builder(CoreOperatorKind.CHECKPOINT)
                .input("in", classOf(String.class))
                .output("out", classOf(String.class))
                .build());
        assertThat(node.getId(), is("testing"));
        assertThat(node.getInputs().keySet(), contains("in"));
        assertThat(node.getOutputs().keySet(), contains("out"));
        assertThat(node.getElements().entrySet(), hasSize(0));
    }

    /**
     * inspects user operator.
     */
    @Test
    public void inspect_user() {
        InspectionNode node = driver.inspect("testing", UserOperator.builder(
                    new AnnotationDescription(classOf(Deprecated.class)),
                    new MethodDescription(classOf(String.class), "valueOf", typeOf(Object.class)),
                    classOf(String.class))
                .input("in", classOf(String.class))
                .output("out", classOf(String.class))
                .build());
        assertThat(node.getId(), is("testing"));
        assertThat(node.getInputs().keySet(), contains("in"));
        assertThat(node.getOutputs().keySet(), contains("out"));
        assertThat(node.getElements().entrySet(), hasSize(0));
    }

    /**
     * inspects input operator.
     */
    @Test
    public void inspect_input() {
        InspectionNode node = driver.inspect("testing", ExternalInput.newInstance("in", typeOf(String.class)));
        assertThat(node.getId(), is("testing"));
        assertThat(node.getInputs().keySet(), hasSize(0));
        assertThat(node.getOutputs().keySet(), contains(ExternalInput.PORT_NAME));
        assertThat(node.getElements().entrySet(), hasSize(0));
    }

    /**
     * inspects output operator.
     */
    @Test
    public void inspect_output() {
        InspectionNode node = driver.inspect("testing", ExternalOutput.newInstance("out", typeOf(String.class)));
        assertThat(node.getId(), is("testing"));
        assertThat(node.getInputs().keySet(), contains(ExternalOutput.PORT_NAME));
        assertThat(node.getOutputs().keySet(), hasSize(0));
        assertThat(node.getElements().entrySet(), hasSize(0));
    }

    /**
     * inspects marker operator.
     */
    @Test
    public void inspect_marker() {
        InspectionNode node = driver.inspect("testing",
                PlanMarkers.newInstance(classOf(String.class), PlanMarker.CHECKPOINT));
        assertThat(node.getId(), is("testing"));
        assertThat(node.getInputs().keySet(), contains(MarkerOperator.PORT_NAME));
        assertThat(node.getOutputs().keySet(), contains(MarkerOperator.PORT_NAME));
        assertThat(node.getElements().entrySet(), hasSize(0));
    }

    /**
     * inspects flow operator.
     */
    @Test
    public void inspect_flow() {
        OperatorGraph graph = new MockOperators()
            .input("input")
            .operator("operator")
            .output("output")
            .connect("input", "operator")
            .connect("operator", "output")
            .toGraph();

        InspectionNode node = driver.inspect("testing", FlowOperator.builder(classOf(String.class), graph)
                .input("input", typeOf(String.class))
                .output("output", typeOf(String.class))
                .build());
        assertThat(node.getId(), is("testing"));
        assertThat(node.getInputs().keySet(), contains("input"));
        assertThat(node.getOutputs().keySet(), contains("output"));
        assertThat(node.getElements().keySet(), hasSize(3));
        validateConnections(node.getElements());
    }

    /**
     * inspects operator graph.
     */
    @Test
    public void inspect_graph() {
        OperatorGraph graph = new MockOperators()
            .input("input")
            .operator("operator")
            .output("output")
            .connect("input", "operator")
            .connect("operator", "output")
            .toGraph();

        InspectionNode node = driver.inspect("testing", graph);
        assertThat(node.getId(), is("testing"));
        assertThat(node.getElements().keySet(), hasSize(3));
        validateConnections(node.getElements());
    }

    /**
     * inspects jobflow.
     */
    @Test
    public void inspect_jobflow() {
        OperatorGraph graph = new MockOperators()
            .input("input")
            .operator("operator")
            .marker("marker")
            .output("output")
            .connect("input", "operator")
            .connect("operator", "marker")
            .connect("marker", "output")
            .toGraph();

        InspectionNode node = driver.inspect(new Jobflow("f", classOf(String.class), graph));
        assertThat(node.getId(), is("f"));
        assertThat(node.getInputs().keySet(), hasSize(0));
        assertThat(node.getOutputs().keySet(), hasSize(0));
        assertThat(node.getElements().keySet(), hasSize(4));
        validateConnections(node.getElements());
    }

    /**
     * inspects batch.
     */
    @Test
    public void inspect_batch() {
        Jobflow a = jobflow("a");
        Jobflow b = jobflow("b");
        Jobflow c = jobflow("c");
        Jobflow d = jobflow("d");

        Batch batch = new Batch(new BatchInfo.Basic("B", new ClassDescription("B")));
        BatchElement eA = batch.addElement(a);
        BatchElement eB = batch.addElement(b);
        BatchElement eC = batch.addElement(c);
        BatchElement eD = batch.addElement(d);
        eB.addBlockerElement(eA);
        eC.addBlockerElement(eA);
        eD.addBlockerElement(eB);
        eD.addBlockerElement(eC);

        InspectionNode node = driver.inspect(batch);
        assertThat(node.getId(), is("B"));
        assertThat(node.getInputs().keySet(), hasSize(0));
        assertThat(node.getOutputs().keySet(), hasSize(0));
        assertThat(node.getElements().keySet(), containsInAnyOrder("a", "b", "c", "d"));
        validateConnections(node.getElements());

        assertThat(getPreds(node.getElements().get("a")), hasSize(0));
        assertThat(getPreds(node.getElements().get("b")), containsInAnyOrder(succ("a")));
        assertThat(getPreds(node.getElements().get("c")), containsInAnyOrder(succ("a")));
        assertThat(getPreds(node.getElements().get("d")), containsInAnyOrder(succ("b"), succ("c")));

        assertThat(getSuccs(node.getElements().get("a")), containsInAnyOrder(pred("b"), pred("c")));
        assertThat(getSuccs(node.getElements().get("b")), containsInAnyOrder(pred("d")));
        assertThat(getSuccs(node.getElements().get("c")), containsInAnyOrder(pred("d")));
        assertThat(getSuccs(node.getElements().get("d")), hasSize(0));
    }

    /**
     * inspects operator graph via {@link BasicObjectInspector}.
     */
    @Test
    public void inspect_graph_bridge() {
        OperatorGraph graph = new MockOperators()
            .input("input")
            .operator("operator")
            .output("output")
            .connect("input", "operator")
            .connect("operator", "output")
            .toGraph();

        ObjectInspector inspector = new BasicObjectInspector();
        assertThat(inspector.isSupported(graph), is(true));
        InspectionNode node = inspector.inspect(graph);
        assertThat(node.getElements().keySet(), hasSize(3));
        validateConnections(node.getElements());
    }

    /**
     * inspects jobflow via {@link BasicObjectInspector}.
     */
    @Test
    public void inspect_jobflow_bridge() {
        OperatorGraph graph = new MockOperators()
            .input("input")
            .operator("operator")
            .marker("marker")
            .output("output")
            .connect("input", "operator")
            .connect("operator", "marker")
            .connect("marker", "output")
            .toGraph();

        Jobflow object = new Jobflow("f", classOf(String.class), graph);
        ObjectInspector inspector = new BasicObjectInspector();
        assertThat(inspector.isSupported(object), is(true));
        InspectionNode node = inspector.inspect(object);
        assertThat(node.getId(), is("f"));
        assertThat(node.getInputs().keySet(), hasSize(0));
        assertThat(node.getOutputs().keySet(), hasSize(0));
        assertThat(node.getElements().keySet(), hasSize(4));
        validateConnections(node.getElements());
    }

    /**
     * inspects batch via {@link BasicObjectInspector}.
     */
    @Test
    public void inspect_batch_bridge() {
        Jobflow a = jobflow("a");
        Jobflow b = jobflow("b");
        Jobflow c = jobflow("c");
        Jobflow d = jobflow("d");

        Batch batch = new Batch(new BatchInfo.Basic("B", new ClassDescription("B")));
        BatchElement eA = batch.addElement(a);
        BatchElement eB = batch.addElement(b);
        BatchElement eC = batch.addElement(c);
        BatchElement eD = batch.addElement(d);
        eB.addBlockerElement(eA);
        eC.addBlockerElement(eA);
        eD.addBlockerElement(eB);
        eD.addBlockerElement(eC);

        ObjectInspector inspector = new BasicObjectInspector();
        assertThat(inspector.isSupported(batch), is(true));
        InspectionNode node = inspector.inspect(batch);
        assertThat(node.getId(), is("B"));
        assertThat(node.getInputs().keySet(), hasSize(0));
        assertThat(node.getOutputs().keySet(), hasSize(0));
        assertThat(node.getElements().keySet(), containsInAnyOrder("a", "b", "c", "d"));
        validateConnections(node.getElements());

        assertThat(getPreds(node.getElements().get("a")), hasSize(0));
        assertThat(getPreds(node.getElements().get("b")), containsInAnyOrder(succ("a")));
        assertThat(getPreds(node.getElements().get("c")), containsInAnyOrder(succ("a")));
        assertThat(getPreds(node.getElements().get("d")), containsInAnyOrder(succ("b"), succ("c")));

        assertThat(getSuccs(node.getElements().get("a")), containsInAnyOrder(pred("b"), pred("c")));
        assertThat(getSuccs(node.getElements().get("b")), containsInAnyOrder(pred("d")));
        assertThat(getSuccs(node.getElements().get("c")), containsInAnyOrder(pred("d")));
        assertThat(getSuccs(node.getElements().get("d")), hasSize(0));
    }

    private void validateConnections(Map<String, InspectionNode> elements) {
        for (InspectionNode element : elements.values()) {
            for (Port port : element.getInputs().values()) {
                PortReference oRef = ref(element.getId(), port.getId());
                for (PortReference ref : port.getOpposites()) {
                    InspectionNode oNode = elements.get(ref.getNodeId());
                    assertThat(oNode, is(notNullValue()));
                    Port oPort = oNode.getOutputs().get(ref.getPortId());
                    assertThat(oPort, is(notNullValue()));
                    assertThat(oPort.getOpposites(), hasItem(oRef));
                }
            }
            for (Port port : element.getOutputs().values()) {
                PortReference oRef = ref(element.getId(), port.getId());
                for (PortReference ref : port.getOpposites()) {
                    InspectionNode oNode = elements.get(ref.getNodeId());
                    assertThat(oNode, is(notNullValue()));
                    Port oPort = oNode.getInputs().get(ref.getPortId());
                    assertThat(oPort, is(notNullValue()));
                    assertThat(oPort.getOpposites(), hasItem(oRef));
                }
            }
        }
    }

    private PortReference ref(String node, String port) {
        return new PortReference(node, port);
    }

    private PortReference succ(String node) {
        return ref(node, Util.NAME_SUCCESSORS);
    }

    private PortReference pred(String node) {
        return ref(node, Util.NAME_PREDECESSORS);
    }

    private Set<PortReference> getPreds(InspectionNode node) {
        Port port = node.getInputs().get(Util.NAME_PREDECESSORS);
        assertThat(port, is(notNullValue()));
        return port.getOpposites();
    }

    private Set<PortReference> getSuccs(InspectionNode node) {
        Port port = node.getOutputs().get(Util.NAME_SUCCESSORS);
        assertThat(port, is(notNullValue()));
        return port.getOpposites();
    }

    private Jobflow jobflow(String id) {
        OperatorGraph graph = new MockOperators()
            .input("input")
            .output("output")
            .connect("input", "output")
            .toGraph();
        return new Jobflow(id, new ClassDescription(id), graph);
    }
}
