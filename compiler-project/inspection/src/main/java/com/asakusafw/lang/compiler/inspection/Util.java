/**
 * Copyright 2011-2017 Asakusa Framework Team.
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

import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.common.AttributeContainer;
import com.asakusafw.lang.compiler.common.ComplexAttribute;
import com.asakusafw.lang.compiler.model.graph.OperatorPort;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.inspection.InspectionNode;

final class Util {

    static final String NAME_SUCCESSORS = "successors"; //$NON-NLS-1$

    static final String NAME_PREDECESSORS = "predecessors"; //$NON-NLS-1$

    static final String PROPERTY_KIND = "kind"; //$NON-NLS-1$

    static final String PROPERTY_DESCRIPTION = "description"; //$NON-NLS-1$

    private Util() {
        return;
    }

    static String id(BatchInfo batch) {
        return batch.getBatchId();
    }

    static String id(JobflowInfo jobflow) {
        return jobflow.getFlowId();
    }

    static String id(TaskReference.Phase phase) {
        return phase.getSymbol();
    }

    static String id(OperatorPort port) {
        return port.getName();
    }

    static void addDependencyPorts(InspectionNode node) {
        node.withInput(new InspectionNode.Port(NAME_PREDECESSORS));
        node.withOutput(new InspectionNode.Port(NAME_SUCCESSORS));
    }

    static void addDependency(InspectionNode pred, InspectionNode succ) {
        InspectionNode.Port upstream = pred.getOutputs().get(NAME_SUCCESSORS);
        InspectionNode.Port downstream = succ.getInputs().get(NAME_PREDECESSORS);
        assert upstream != null;
        assert downstream != null;
        upstream.withOpposite(new InspectionNode.PortReference(succ.getId(), downstream.getId()));
        downstream.withOpposite(new InspectionNode.PortReference(pred.getId(), upstream.getId()));
    }

    static Map<String, String> extract(BatchInfo batch) {
        Map<String, String> results = new LinkedHashMap<>();
        results.put("batchId", batch.getBatchId()); //$NON-NLS-1$
        results.put(PROPERTY_DESCRIPTION, batch.getDescriptionClass().getClassName());
        results.put("comment", comment(batch.getComment())); //$NON-NLS-1$
        for (BatchInfo.Parameter parameter : batch.getParameters()) {
            results.put(
                    String.format("parameters.%s", parameter.getKey()), //$NON-NLS-1$
                    comment(parameter.getComment()));
        }
        return results;
    }

    private static String comment(String comment) {
        if (comment == null) {
            return "(no comments)"; //$NON-NLS-1$
        }
        return comment;
    }

    static Map<String, String> extract(JobflowInfo jobflow) {
        Map<String, String> results = new LinkedHashMap<>();
        results.put("flowId", jobflow.getFlowId()); //$NON-NLS-1$
        results.put(PROPERTY_DESCRIPTION, jobflow.getDescriptionClass().getClassName());
        return results;
    }

    static Map<String, String> extractAttributes(AttributeContainer attributes) {
        Map<String, String> results = new LinkedHashMap<>();
        for (Class<?> type : attributes.getAttributeTypes()) {
            String key = getAttributeKey(type);
            Object value = attributes.getAttribute(type);
            if (value instanceof ComplexAttribute) {
                results.putAll(extractComplexAttributes(key, ((ComplexAttribute) value).toMap()));
            } else {
                results.put(key, getAttributeValue(value));
            }
        }
        return results;
    }

    private static Map<String, String> extractComplexAttributes(String prefix, Map<String, ?> nested) {
        Map<String, String> results = new LinkedHashMap<>();
        for (Map.Entry<String, ?> entry : nested.entrySet()) {
            String key = String.format("%s.%s", prefix, entry.getKey()); //$NON-NLS-1$
            Object value = entry.getValue();
            results.put(key, getAttributeValue(value));
        }
        return results;
    }

    static String getAttributeKey(Class<?> attributeType) {
        return String.format("attribute.%s", attributeType.getSimpleName()); //$NON-NLS-1$
    }

    static String getAttributeValue(Object value) {
        return String.valueOf(value);
    }
}
