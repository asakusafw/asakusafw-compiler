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
package com.asakusafw.lang.compiler.inspection;

import static com.asakusafw.lang.compiler.inspection.Util.*;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.inspection.InspectionNode;

/**
 * Inspects execution plans.
 */
public class PlanDriver {

    private final DslDriver driver = new DslDriver();

    /**
     * Inspects the target plan.
     * @param id the target ID
     * @param object the target plan
     * @return the inspection object
     */
    public InspectionNode inspect(String id, Plan object) {
        String title = "Plan"; //$NON-NLS-1$
        InspectionNode result = new InspectionNode(id, title);
        result.withProperty(PROPERTY_KIND, "Plan"); //$NON-NLS-1$
        result.getProperties().putAll(extractAttributes(object));
        for (InspectionNode element : inspect(object.getElements()).values()) {
            result.withElement(element);
        }
        return result;
    }

    Map<SubPlan, InspectionNode> inspect(Collection<? extends SubPlan> elements) {
        Map<SubPlan, SubPlanInfo> map = new LinkedHashMap<>();
        for (SubPlan element : elements) {
            String id = String.format("sub-%d", map.size()); //$NON-NLS-1$
            SubPlanInfo info = inspect(id, element);
            map.put(element, info);
        }
        for (SubPlanInfo element : map.values()) {
            for (Map.Entry<SubPlan.Input, InspectionNode.Port> entry : element.inputs.entrySet()) {
                InspectionNode.Port port = entry.getValue();
                InspectionNode.PortReference ref =
                        new InspectionNode.PortReference(element.node.getId(), port.getId());
                for (SubPlan.Output opposite : entry.getKey().getOpposites()) {
                    SubPlanInfo oNode = map.get(opposite.getOwner());
                    assert oNode != null;
                    InspectionNode.Port oPort = oNode.outputs.get(opposite);
                    assert oPort != null;
                    InspectionNode.PortReference target =
                            new InspectionNode.PortReference(oNode.node.getId(), oPort.getId());

                    port.withOpposite(target);
                    oPort.withOpposite(ref);
                }
            }
        }
        Map<SubPlan, InspectionNode> results = new LinkedHashMap<>();
        for (SubPlanInfo info : map.values()) {
            results.put(info.target, info.node);
        }
        return results;
    }

    SubPlanInfo inspect(String id, SubPlan object) {
        String title = "SubPlan"; //$NON-NLS-1$
        InspectionNode node = new InspectionNode(id, title);
        node.withProperty(PROPERTY_KIND, "SubPlan"); //$NON-NLS-1$
        SubPlanInfo info = new SubPlanInfo(object, node, driver.inspect(object.getOperators()));
        node.getProperties().putAll(extractAttributes(object));
        for (SubPlan.Input port : object.getInputs()) {
            InspectionNode operator = info.elements.get(port.getOperator());
            assert operator != null;
            InspectionNode.Port p = new InspectionNode.Port(operator.getId());
            node.withInput(p);
            info.inputs.put(port, p);
            p.getProperties().putAll(extractAttributes(port));
        }
        for (SubPlan.Output port : object.getOutputs()) {
            InspectionNode operator = info.elements.get(port.getOperator());
            assert operator != null;
            InspectionNode.Port p = new InspectionNode.Port(operator.getId());
            node.withOutput(p);
            info.outputs.put(port, p);
            p.getProperties().putAll(extractAttributes(port));
        }
        for (InspectionNode element : info.elements.values()) {
            node.withElement(element);
        }
        return info;
    }

    static class SubPlanInfo {

        final SubPlan target;

        final InspectionNode node;

        final Map<Operator, InspectionNode> elements;

        final Map<SubPlan.Input, InspectionNode.Port> inputs = new LinkedHashMap<>();

        final Map<SubPlan.Output, InspectionNode.Port> outputs = new LinkedHashMap<>();

        public SubPlanInfo(SubPlan target, InspectionNode node, Map<Operator, InspectionNode> elements) {
            this.target = target;
            this.node = node;
            this.elements = elements;
        }
    }
}
