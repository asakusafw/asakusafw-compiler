/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.parameter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.BatchElement;
import com.asakusafw.lang.compiler.model.graph.ExternalPort;
import com.asakusafw.lang.compiler.model.graph.FlowOperator;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;

/**
 * Represents a list of batch parameters which are used in applications.
 * @since 0.5.0
 */
public class ImplicitParameterList {

    private final Collection<BatchInfo.Parameter> parameters;

    /**
     * Creates a new instance.
     * @param parameters the parameters
     */
    public ImplicitParameterList(Collection<? extends BatchInfo.Parameter> parameters) {
        Objects.requireNonNull(parameters);
        this.parameters = Collections.unmodifiableList(new ArrayList<>(parameters));
    }

    /**
     * Returns the batch parameters for this.
     * @return the batch parameters
     */
    public Collection<BatchInfo.Parameter> getParameters() {
        return parameters;
    }

    /**
     * Collects implicitly required parameter list of the given batch.
     * @param batch the target batch
     * @return the parameter list
     */
    public static ImplicitParameterList of(Batch batch) {
        Map<String, BatchInfo.Parameter> parameters = batch.getElements().stream()
                .map(BatchElement::getJobflow)
                .flatMap(it -> collect(it).stream())
                .collect(Collectors.toMap(
                        BatchInfo.Parameter::getKey,
                        Function.identity(),
                        (a, b) -> a.getComment() != null ? a : b));
        return new ImplicitParameterList(parameters.values());
    }

    /**
     * Collects implicitly required parameter list of the given jobflow.
     * @param jobflow the target jobflow
     * @return the parameter list
     */
    public static ImplicitParameterList of(Jobflow jobflow) {
        Map<String, BatchInfo.Parameter> parameters = collect(jobflow).stream()
                .collect(Collectors.toMap(
                        BatchInfo.Parameter::getKey,
                        Function.identity(),
                        (a, b) -> a.getComment() != null ? a : b));
        return new ImplicitParameterList(parameters.values());
    }

    private static List<BatchInfo.Parameter> collect(Jobflow jobflow) {
        List<BatchInfo.Parameter> results = new ArrayList<>();
        collect(jobflow, jobflow.getOperatorGraph(), results::add);
        return results;
    }

    private static void collect(Jobflow jobflow, OperatorGraph current, Consumer<BatchInfo.Parameter> sink) {
        for (Operator operator : current.getOperators(false)) {
            switch (operator.getOperatorKind()) {
            case CORE:
            case MARKER:
                // never use
                break;
            case INPUT:
            case OUTPUT:
                Optional.ofNullable(((ExternalPort) operator).getInfo())
                        .ifPresent(it -> collect(it, sink));
                break;
            case USER:
            case CUSTOM:
                // cannot detect
                break;
            case FLOW:
                collect(jobflow, ((FlowOperator) operator).getOperatorGraph(), sink);
                break;
            default:
                throw new AssertionError(operator);
            }
        }
    }

    private static void collect(ExternalPortInfo info, Consumer<BatchInfo.Parameter> sink) {
        info.getParameterNames().stream()
            .map(it -> new BatchInfo.Parameter(
                    it,
                    Optional.ofNullable(info.getDescriptionClass())
                            .map(desc -> String.format("(%s)", desc.getSimpleName()))
                            .orElse(null),
                    true,
                    null))
            .forEach(sink);
    }

    /**
     * Merges parameters of this into the given parameter list.
     * @param others the target parameter list
     * @return the merged parameter list
     */
    public Collection<BatchInfo.Parameter> merge(Collection<? extends BatchInfo.Parameter> others) {
        Objects.requireNonNull(others);
        return merge(others, parameters);
    }

    private static List<BatchInfo.Parameter> merge(
            Collection<? extends BatchInfo.Parameter> a,
            Collection<? extends BatchInfo.Parameter> b) {
        Map<String, BatchInfo.Parameter> rights = new LinkedHashMap<>();
        b.forEach(it -> rights.put(it.getKey(), it));
        List<BatchInfo.Parameter> results = new ArrayList<>();
        for (BatchInfo.Parameter left : a) {
            BatchInfo.Parameter right = rights.remove(left.getKey());
            results.add(merge(left, right));
        }
        results.addAll(rights.values());
        return results;
    }

    private static BatchInfo.Parameter merge(BatchInfo.Parameter left, BatchInfo.Parameter right) {
        assert left != null || right != null;
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        assert Objects.equals(left.getKey(), right.getKey());
        return new BatchInfo.Parameter(
                left.getKey(),
                Optional.ofNullable(left.getComment()).orElseGet(right::getComment),
                left.isMandatory() & right.isMandatory(),
                Optional.ofNullable(left.getPattern()).orElseGet(right::getPattern)); // NOTE: intersection
    }
}
