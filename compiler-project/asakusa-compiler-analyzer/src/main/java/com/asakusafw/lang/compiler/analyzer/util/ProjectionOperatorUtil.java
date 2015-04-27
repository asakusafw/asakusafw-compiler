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
package com.asakusafw.lang.compiler.analyzer.util;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.vocabulary.operator.Extend;
import com.asakusafw.vocabulary.operator.Project;
import com.asakusafw.vocabulary.operator.Restructure;

/**
 * Utilities for <em>projection kind</em> operators.
 */
public final class ProjectionOperatorUtil {

    private static final Set<CoreOperatorKind> SUPPORTED = EnumUtil.freeze(new CoreOperatorKind[] {
            CoreOperatorKind.PROJECT,
            CoreOperatorKind.EXTEND,
            CoreOperatorKind.RESTRUCTURE,
    });

    private ProjectionOperatorUtil() {
        return;
    }

    /**
     * Returns whether the target operator is <em>projection kind</em> or not.
     * @param operator the target operator
     * @return {@code true} if the target operator is <em>projection kind</em>, otherwise {@code false}
     */
    public static boolean isSupported(Operator operator) {
        if (operator.getOperatorKind() != OperatorKind.CORE) {
            return false;
        }
        CoreOperator op = (CoreOperator) operator;
        return SUPPORTED.contains(op.getCoreOperatorKind());
    }

    /**
     * Extracts property mappings from a <em>projection kind</em> operator.
     * @param dataModelLoader the data model loader to inspect data types in the operator
     * @param operator the target operator
     * @return the property mappings
     * @throws DiagnosticException if the mapping is inconsistent between the operator input and output
     * @throws IllegalArgumentException if the target operator is not supported
     */
    public static List<PropertyMapping> getPropertyMappings(DataModelLoader dataModelLoader, Operator operator) {
        if (isSupported(operator) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "operator must be project kind: {0}",
                    operator));
        }
        OperatorInput input;
        OperatorOutput output;
        MappingKind kind;
        switch (((CoreOperator) operator).getCoreOperatorKind()) {
        case PROJECT: {
            OperatorUtil.checkOperatorPorts(operator, 1, 1);
            input = operator.getInputs().get(Project.ID_INPUT);
            output = operator.getOutputs().get(Project.ID_OUTPUT);
            kind = MappingKind.NARROWING;
            break;
        }
        case EXTEND: {
            OperatorUtil.checkOperatorPorts(operator, 1, 1);
            input = operator.getInputs().get(Extend.ID_INPUT);
            output = operator.getOutputs().get(Extend.ID_OUTPUT);
            kind = MappingKind.WIDENING;
            break;
        }
        case RESTRUCTURE: {
            OperatorUtil.checkOperatorPorts(operator, 1, 1);
            input = operator.getInputs().get(Restructure.ID_INPUT);
            output = operator.getOutputs().get(Restructure.ID_OUTPUT);
            kind = MappingKind.ELASTIC;
            break;
        }
        default:
            throw new AssertionError(operator);
        }
        return extract(dataModelLoader, operator, input, output, kind);
    }

    private static List<PropertyMapping> extract(
            DataModelLoader loader,
            Operator operator, OperatorInput input, OperatorOutput output,
            MappingKind kind) {
        DataModelReference source = loader.load(input.getDataType());
        DataModelReference destination = loader.load(output.getDataType());
        List<MappingElement> elements = collectElements(source, destination);
        validateElements(operator, elements, kind);

        List<PropertyMapping> results = new ArrayList<>();
        for (MappingElement element : elements) {
            if (element.source == null || element.destination == null) {
                continue;
            }
            PropertyMapping mapping = new PropertyMapping(
                    input, element.source.getName(),
                    output, element.destination.getName());
            results.add(mapping);
        }
        return results;
    }

    private static List<MappingElement> collectElements(DataModelReference source, DataModelReference destination) {
        Map<PropertyName, PropertyReference> destinations = extractPropertyMap(destination);
        List<MappingElement> results = new ArrayList<>();
        for (PropertyReference from : source.getProperties()) {
            PropertyReference to = destinations.remove(from.getName());
            results.add(new MappingElement(from, to));
        }
        for (PropertyReference to : destinations.values()) {
            results.add(new MappingElement(null, to));
        }
        return results;
    }

    private static void validateElements(Object location, List<MappingElement> elements, MappingKind kind) {
        List<Diagnostic> diagnostics = new ArrayList<>();
        for (MappingElement element : elements) {
            assert element.source != null || element.destination != null;
            if (element.source == null) {
                if (kind == MappingKind.NARROWING) {
                    diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                            "missing mapping source property ({0}): {1}",
                            location,
                            element.destination)));
                }
            } else if (element.destination == null) {
                if (kind == MappingKind.WIDENING) {
                    diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                            "missing mapping destination property ({0}): {1}",
                            location,
                            element.source)));
                }
            } else {
                if (element.source.getType().equals(element.destination.getType()) == false) {
                    diagnostics.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                            "inconsistent type in property mapping ({0}): {1} <=> {2}",
                            location,
                            element.source, element.destination)));
                }
            }
        }
        if (diagnostics.isEmpty() == false) {
            throw new DiagnosticException(diagnostics);
        }
    }

    private static Map<PropertyName, PropertyReference> extractPropertyMap(DataModelReference dataModel) {
        Map<PropertyName, PropertyReference> results = new LinkedHashMap<>();
        for (PropertyReference property : dataModel.getProperties()) {
            results.put(property.getName(), property);
        }
        return results;
    }

    private static enum MappingKind {

        WIDENING,

        NARROWING,

        ELASTIC,
    }

    private static class MappingElement {

        final PropertyReference source;

        final PropertyReference destination;

        public MappingElement(PropertyReference source, PropertyReference destination) {
            this.source = source;
            this.destination = destination;
        }
    }
}
