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
package com.asakusafw.lang.compiler.inspection.driver;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.asakusafw.lang.compiler.inspection.InspectionNode;
import com.asakusafw.lang.compiler.model.testing.MockOperators;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.PlanBuilder;
import com.asakusafw.lang.compiler.planning.PlanDetail;
import com.asakusafw.lang.compiler.planning.PlanMarker;
import com.asakusafw.lang.compiler.planning.SubPlan;

/**
 * Test for {@link PlanDriver}.
 */
public class PlanDriverTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("end"))
            .build();

        InspectionNode node = inspect(detail);
        validateMappings(detail.getPlan(), node);
    }

    /**
     * multiple plans.
     */
    @Test
    public void multiple() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("cp", PlanMarker.CHECKPOINT).connect("a", "cp")
            .operator("b").connect("cp", "b")
            .marker("end", PlanMarker.END).connect("b", "end");

        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("cp"))
            .add(mock.getAsSet("cp"), mock.getAsSet("end"))
            .build();

        InspectionNode node = inspect(detail);
        validateMappings(detail.getPlan(), node);
    }

    /**
     * diamond plans w/ difference checkpoints.
     */
    @Test
    public void diamond_diff_checkpoints() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("o0")
            .operator("o1")
            .operator("o2")
            .operator("o3")
            .marker("cp0", PlanMarker.CHECKPOINT)
            .marker("cp1", PlanMarker.CHECKPOINT)
            .marker("cp2", PlanMarker.CHECKPOINT)
            .marker("cp3", PlanMarker.CHECKPOINT)
            .marker("end", PlanMarker.END)
            .connect("begin", "o0")
            .connect("o0", "cp0")
            .connect("o0", "cp1")
            .connect("cp0", "o1")
            .connect("cp1", "o2")
            .connect("o1", "cp2")
            .connect("o2", "cp3")
            .connect("cp2", "o3")
            .connect("cp3", "o3")
            .connect("o3", "end");

        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("cp0", "cp1"))
            .add(mock.getAsSet("cp0"), mock.getAsSet("cp2"))
            .add(mock.getAsSet("cp1"), mock.getAsSet("cp3"))
            .add(mock.getAsSet("cp2", "cp3"), mock.getAsSet("end"))
            .build();

        InspectionNode node = inspect(detail);
        validateMappings(detail.getPlan(), node);
    }

    /**
     * diamond plans w/ sharing input checkpoints.
     */
    @Test
    public void diamond_shared_input() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("o0")
            .operator("o1")
            .operator("o2")
            .operator("o3")
            .marker("cp0", PlanMarker.CHECKPOINT)
            .marker("cp1", PlanMarker.CHECKPOINT)
            .marker("cp2", PlanMarker.CHECKPOINT)
            .marker("end", PlanMarker.END)
            .connect("begin", "o0")
            .connect("o0", "cp0")
            .connect("cp0", "o1")
            .connect("cp0", "o2")
            .connect("o1", "cp1")
            .connect("o2", "cp2")
            .connect("cp1", "o3")
            .connect("cp2", "o3")
            .connect("o3", "end");

        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("cp0"))
            .add(mock.getAsSet("cp0"), mock.getAsSet("cp1"))
            .add(mock.getAsSet("cp0"), mock.getAsSet("cp2"))
            .add(mock.getAsSet("cp1", "cp2"), mock.getAsSet("end"))
            .build();

        InspectionNode node = inspect(detail);
        validateMappings(detail.getPlan(), node);
    }

    /**
     * via {@link ObjectInspector}.
     */
    @Test
    public void bridge() {
        MockOperators mock = new MockOperators()
            .marker("begin", PlanMarker.BEGIN)
            .operator("a").connect("begin", "a")
            .marker("end", PlanMarker.END).connect("a", "end");

        PlanDetail detail = PlanBuilder.from(mock.all())
            .add(mock.getAsSet("begin"), mock.getAsSet("end"))
            .build();
        bless(detail);

        Plan plan = detail.getPlan();
        ObjectInspector inspector = new ObjectInspector();
        assertThat(inspector.isSupported(plan), is(true));
        InspectionNode node = inspector.inspect(plan);

        validateMappings(plan, node);
    }

    private InspectionNode inspect(PlanDetail detail) {
        bless(detail);
        return new PlanDriver().inspect("plan", detail.getPlan());
    }

    private void bless(PlanDetail detail) {
        int index = 0;
        for (SubPlan sub : detail.getPlan().getElements()) {
            sub.putAttribute(Bless.class, new Bless(index++));
            for (SubPlan.Input port : sub.getInputs()) {
                port.putAttribute(Bless.class, new Bless(index++));
            }
            for (SubPlan.Output port : sub.getOutputs()) {
                port.putAttribute(Bless.class, new Bless(index++));
            }
        }
    }

    private void validateMappings(Plan plan, InspectionNode node) {
        for (SubPlan sub : plan.getElements()) {
            InspectionNode element = resolve(node, sub);
            assertThat(sub.getOperators(), hasSize(element.getElements().size()));
            for (SubPlan.Input port : sub.getInputs()) {
                InspectionNode.Port p = resolve(element, port);
                Set<InspectionNode.PortReference> opposites = new HashSet<>();
                for (SubPlan.Output opposite : port.getOpposites()) {
                    InspectionNode oNode = resolve(node, opposite.getOwner());
                    InspectionNode.Port oPort = resolve(oNode, opposite);
                    opposites.add(new InspectionNode.PortReference(oNode.getId(), oPort.getId()));
                }
                assertThat(p.getOpposites(), is(opposites));
            }
            for (SubPlan.Output port : sub.getOutputs()) {
                InspectionNode.Port p = resolve(element, port);
                Set<InspectionNode.PortReference> opposites = new HashSet<>();
                for (SubPlan.Input opposite : port.getOpposites()) {
                    InspectionNode oNode = resolve(node, opposite.getOwner());
                    InspectionNode.Port oPort = resolve(oNode, opposite);
                    opposites.add(new InspectionNode.PortReference(oNode.getId(), oPort.getId()));
                }
                assertThat(p.getOpposites(), is(opposites));
            }
        }
    }

    private InspectionNode resolve(InspectionNode node, SubPlan sub) {
        Bless bless = sub.getAttribute(Bless.class);
        assertThat(bless, is(notNullValue()));
        for (InspectionNode element : node.getElements().values()) {
            String value = element.getProperties().get(Util.getAttributeKey(Bless.class));
            if (value != null && value.equals(Util.getAttributeValue(bless))) {
                return element;
            }
        }
        throw new AssertionError(sub);
    }

    private InspectionNode.Port resolve(InspectionNode node, SubPlan.Input port) {
        Bless bless = port.getAttribute(Bless.class);
        assertThat(bless, is(notNullValue()));
        for (InspectionNode.Port element : node.getInputs().values()) {
            String value = element.getProperties().get(Util.getAttributeKey(Bless.class));
            if (value != null && value.equals(Util.getAttributeValue(bless))) {
                return element;
            }
        }
        throw new AssertionError(port);
    }

    private InspectionNode.Port resolve(InspectionNode node, SubPlan.Output port) {
        Bless bless = port.getAttribute(Bless.class);
        assertThat(bless, is(notNullValue()));
        for (InspectionNode.Port element : node.getOutputs().values()) {
            String value = element.getProperties().get(Util.getAttributeKey(Bless.class));
            if (value != null && value.equals(Util.getAttributeValue(bless))) {
                return element;
            }
        }
        throw new AssertionError(port);
    }

    private static class Bless {

        final int index;

        Bless(int index) {
            this.index = index;
        }

        @Override
        public String toString() {
            return String.format("Bless(%d)", index);
        }
    }
}
