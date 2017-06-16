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
package com.asakusafw.lang.info.cli;

import static com.asakusafw.lang.info.cli.DrawUtil.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.asakusafw.lang.info.BatchInfo;
import com.asakusafw.lang.info.JobflowInfo;
import com.asakusafw.lang.info.operator.CoreOperatorSpec;
import com.asakusafw.lang.info.operator.CustomOperatorSpec;
import com.asakusafw.lang.info.operator.FlowOperatorSpec;
import com.asakusafw.lang.info.operator.InputGroup;
import com.asakusafw.lang.info.operator.InputOperatorSpec;
import com.asakusafw.lang.info.operator.MarkerOperatorSpec;
import com.asakusafw.lang.info.operator.OperatorGraphAttribute;
import com.asakusafw.lang.info.operator.OperatorSpec.OperatorKind;
import com.asakusafw.lang.info.operator.OutputOperatorSpec;
import com.asakusafw.lang.info.operator.UserOperatorSpec;
import com.asakusafw.lang.info.operator.view.InputView;
import com.asakusafw.lang.info.operator.view.OperatorGraphView;
import com.asakusafw.lang.info.operator.view.OperatorView;
import com.asakusafw.lang.info.operator.view.OutputView;
import com.asakusafw.lang.info.value.ClassInfo;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

/**
 * A command for generating DOT script about operator graphs.
 * @since 0.4.2
 */
@Command(
        name = "operator",
        description = "Generates operator graph as Graphviz DOT script",
        hidden = false
)
public class DrawOperatorCommand extends SingleJobflowInfoCommand {

    @Option(
            name = { "--depth", },
            title = "depth",
            description = "limit number of depth",
            arity = 1,
            required = false)
    int limitDepth = Integer.MAX_VALUE;

    @Option(
            name = { "--flow-part", },
            title = "class name",
            description = "only displays in the ",
            arity = 1,
            required = false)
    String flowPart;

    @Option(
            name = { "--show-argument", },
            title = "display operator argument",
            description = "display operator argument",
            arity = 0,
            required = false)
    boolean showArgument = false;

    @Option(
            name = { "--show-io", },
            title = "display external I/O class",
            description = "display external I/O class",
            arity = 0,
            required = false)
    boolean showExternalIo = false;

    @Option(
            name = { "--show-name", },
            title = "display port name",
            description = "display port name",
            arity = 0,
            required = false)
    boolean showPortName = false;

    @Option(
            name = { "--show-key", },
            title = "display port key",
            description = "display port key",
            arity = 0,
            required = false)
    boolean showPortKey = false;

    @Option(
            name = { "--show-type", },
            title = "display data type",
            description = "display data type",
            arity = 0,
            required = false)
    boolean showPortType = false;

    @Option(
            name = { "--show-all", "-a", },
            title = "display all information",
            description = "display all information",
            arity = 0,
            required = false)
    boolean showAll = false;

    @Override
    protected void process(PrintWriter writer, BatchInfo batch, JobflowInfo jobflow) throws IOException {
        OperatorGraphView graph = jobflow.findAttribute(OperatorGraphAttribute.class)
                .map(OperatorGraphView::new)
                .orElseThrow(() -> new IllegalStateException("there are no available operators"));
        String label = Optional.ofNullable(jobflow.getDescriptionClass())
                .map(ClassInfo::of)
                .map(ClassInfo::getSimpleName)
                .orElse(jobflow.getId());
        if (flowPart != null) {
            OperatorView fp = findFlowPart(graph)
                    .orElseThrow(() -> new IOException(MessageFormat.format(
                            "there are no flow part named \"{1}\" in jobflow {0}",
                            Optional.ofNullable(jobflow.getDescriptionClass())
                                .orElse(jobflow.getId()),
                            flowPart)));
            graph = fp.getElementGraph();
            label = Optional.of(((FlowOperatorSpec) fp.getSpec()).getDescriptionClass())
                    .map(ClassInfo::getSimpleName)
                    .orElse(null);
        }
        new Engine(writer, graph).print(label);
    }

    private Optional<OperatorView> findFlowPart(OperatorGraphView graph) {
        Queue<OperatorView> queue = new LinkedList<>();
        getFlowParts(graph).forEach(queue::offer);
        while (queue.isEmpty() == false) {
            OperatorView next = queue.poll();
            assert next.getSpec() instanceof FlowOperatorSpec;
            if (Optional.ofNullable(((FlowOperatorSpec) next.getSpec()).getDescriptionClass())
                    .filter(it -> flowPart.equals(it.getClassName()) || flowPart.equals(it.getSimpleName()))
                    .isPresent()) {
                return Optional.of(next);
            } else {
                getFlowParts(next.getElementGraph()).forEach(queue::offer);
            }
        }
        return Optional.empty();
    }

    private static Stream<OperatorView> getFlowParts(OperatorGraphView graph) {
        return graph.getOperators().stream().filter(it -> it.getSpec() instanceof FlowOperatorSpec);
    }

    private class Engine {

        private final PrintWriter writer;

        private final OperatorGraphView root;

        private final Map<Object, Id> ids = new HashMap<>();

        private int idCount = 0;

        private final boolean record;

        Engine(PrintWriter writer, OperatorGraphView graph) {
            this.writer = writer;
            this.root = graph;
            this.record = showAll || showPortName || showPortType || showPortType;
            computeIds(root, 1);
        }

        private void computeIds(OperatorGraphView graph, int currentDepth) {
            for (OperatorView operator : graph.getOperators()) {
                switch (operator.getSpec().getOperatorKind()) {
                case INPUT:
                case OUTPUT:
                case MARKER:
                    addSimpleIds(operator);
                    break;
                case FLOW:
                    if (currentDepth < limitDepth) {
                        computeIds(operator.getElementGraph(), currentDepth + 1);
                        addFlowIds(operator);
                    } else {
                        addGeneralIds(operator);
                    }
                    break;
                default:
                    addGeneralIds(operator);
                    break;
                }
            }
        }

        private void addSimpleIds(OperatorView operator) {
            ids.put(operator, getNextId());
        }

        private void addGeneralIds(OperatorView operator) {
            if (record == false) {
                addSimpleIds(operator);
                return;
            }
            Id id = getNextId();
            ids.put(operator, id);
            int index = 0;
            for (InputView port : operator.getInputs()) {
                ids.put(port, new Id(id, index++));
            }
            for (OutputView port : operator.getOutputs()) {
                ids.put(port, new Id(id, index++));
            }
        }

        private void addFlowIds(OperatorView operator) {
            OperatorGraphView graph = operator.getElementGraph();
            ids.put(operator, getNextId());

            Map<String, OperatorView> inputs = graph.getInputs();
            operator.getInputs().stream().forEach(it -> ids.put(
                    it,
                    Optional.ofNullable(inputs.get(it.getName()))
                        .flatMap(v -> Optional.ofNullable(ids.get(v)))
                        .orElseThrow(IllegalStateException::new)));

            Map<String, OperatorView> outputs = graph.getOutputs();
            operator.getOutputs().stream().forEach(it -> ids.put(
                    it,
                    Optional.ofNullable(outputs.get(it.getName()))
                        .flatMap(v -> Optional.ofNullable(ids.get(v)))
                        .orElseThrow(IllegalStateException::new)));
        }

        private Id getNextId() {
            return new Id(null, idCount++);
        }

        String getId(OperatorView operator) {
            return Optional.ofNullable(ids.get(operator))
                    .map(Id::asSimple)
                    .orElseThrow(IllegalStateException::new);
        }

        String getSimpleId(InputView port) {
            return Optional.ofNullable(ids.get(port))
                    .map(Id::asSimple)
                    .orElseThrow(IllegalStateException::new);
        }

        String getSimpleId(OutputView port) {
            return Optional.ofNullable(ids.get(port))
                    .map(Id::asSimple)
                    .orElseThrow(IllegalStateException::new);
        }

        String getQualifiedId(InputView port) {
            return Optional.ofNullable(ids.get(port))
                    .map(Id::asQualified)
                    .orElseGet(() -> getId(port.getOwner()));
        }

        String getQualifiedId(OutputView port) {
            return Optional.ofNullable(ids.get(port))
                    .map(Id::asQualified)
                    .orElseGet(() -> getId(port.getOwner()));
        }

        void print(String label) {
            writer.println("digraph {");
            Optional.ofNullable(label)
                .ifPresent(it -> writer.printf("label=%s;%n", literal(it)));
            printVertices(root, 1);
            printEdges(root, 1);
            writer.println("}");
        }

        private void printVertices(OperatorGraphView graph, int currentDepth) {
            for (OperatorView operator : graph.getOperators()) {
                if (operator.getSpec().getOperatorKind() == OperatorKind.FLOW
                        && currentDepth < limitDepth) {
                    FlowOperatorSpec spec = (FlowOperatorSpec) operator.getSpec();
                    writer.printf("subgraph cluster_%s {%n", getId(operator));
                    Optional.ofNullable(spec.getDescriptionClass())
                        .map(ClassInfo::getSimpleName)
                        .ifPresent(it -> writer.printf("label=%s;%n", literal(it)));
                    printVertices(operator.getElementGraph(), currentDepth + 1);
                    writer.println("}");
                } else {
                    printVertex(operator);
                }
            }
        }

        private void printVertex(OperatorView operator) {
            switch (operator.getSpec().getOperatorKind()) {
            case CORE:
                printCoreVertex(operator, (CoreOperatorSpec) operator.getSpec());
                break;
            case USER:
                printUserVertex(operator, (UserOperatorSpec) operator.getSpec());
                break;
            case INPUT:
                printInputVertex(operator, (InputOperatorSpec) operator.getSpec());
                break;
            case OUTPUT:
                printOutputVertex(operator, (OutputOperatorSpec) operator.getSpec());
                break;
            case FLOW:
                addFlowVertex(operator, (FlowOperatorSpec) operator.getSpec());
                break;
            case MARKER:
                addMarkerVertex(operator, (MarkerOperatorSpec) operator.getSpec());
                break;
            case CUSTOM:
                addCustomVertex(operator, (CustomOperatorSpec) operator.getSpec());
                break;
            default:
                addGeneralIds(operator);
                break;
            }
        }

        private void printCoreVertex(OperatorView operator, CoreOperatorSpec spec) {
            List<String> body = new ArrayList<>();
            body.add('@' + spec.getCategory().getAnnotationType().getSimpleName());
            body.addAll(analyzeBody(operator));
            if (record) {
                printRecord(operator, body);
            } else {
                printGeneralVertex(operator, "box", body);
            }
        }

        private void printUserVertex(OperatorView operator, UserOperatorSpec spec) {
            List<String> body = new ArrayList<>();
            body.add('@' + spec.getAnnotation().getDeclaringClass().getSimpleName());
            body.add(spec.getDeclaringClass().getSimpleName());
            body.add(spec.getMethodName());
            body.addAll(analyzeBody(operator));
            if (record) {
                printRecord(operator, body);
            } else {
                printGeneralVertex(operator, "box", body);
            }
        }

        private void printInputVertex(OperatorView operator, InputOperatorSpec spec) {
            List<String> body = new ArrayList<>();
            Optional.ofNullable(spec.getDescriptionClass())
                .ifPresent(it -> body.add("@Import"));
            body.add(spec.getName());
            if (showAll || showPortType) {
                operator.getOutputs().stream()
                    .findAny()
                    .map(OutputView::getDataType)
                    .map(ClassInfo::getSimpleName)
                    .ifPresent(body::add);
            }
            if (showAll || showExternalIo) {
                Optional.ofNullable(spec.getDescriptionClass())
                    .map(ClassInfo::getSimpleName)
                    .ifPresent(body::add);
            }
            body.addAll(analyzeBody(operator));
            printGeneralVertex(operator, "invhouse", body);
        }

        private void printOutputVertex(OperatorView operator, OutputOperatorSpec spec) {
            List<String> body = new ArrayList<>();
            Optional.ofNullable(spec.getDescriptionClass())
                .ifPresent(it -> body.add("@Export"));
            body.add(spec.getName());
            if (showAll || showPortType) {
                operator.getInputs().stream()
                    .findAny()
                    .map(InputView::getDataType)
                    .map(ClassInfo::getSimpleName)
                    .ifPresent(body::add);
            }
            if (showAll || showExternalIo) {
                Optional.ofNullable(spec.getDescriptionClass())
                    .map(ClassInfo::getSimpleName)
                    .ifPresent(body::add);
            }
            body.addAll(analyzeBody(operator));
            printGeneralVertex(operator, "invhouse", body);
        }

        private void addFlowVertex(OperatorView operator, FlowOperatorSpec spec) {
            List<String> body = new ArrayList<>();
            body.add("@FlowPart");
            Optional.ofNullable(spec.getDescriptionClass())
                .map(ClassInfo::getSimpleName)
                .ifPresent(body::add);
            body.addAll(analyzeBody(operator));
            if (record) {
                printRecord(operator, body);
            } else {
                printGeneralVertex(operator, "box", body);
            }
        }

        private void addMarkerVertex(OperatorView operator, MarkerOperatorSpec spec) {
            List<String> body = new ArrayList<>();
            body.add("(marker)");
            body.addAll(analyzeBody(operator));
            printGeneralVertex(operator, "box", body);
        }

        private void addCustomVertex(OperatorView operator, CustomOperatorSpec spec) {
            List<String> body = new ArrayList<>();
            body.add(spec.getCategory());
            body.addAll(analyzeBody(operator));
            if (record) {
                printRecord(operator, body);
            } else {
                printGeneralVertex(operator, "box", body);
            }
        }

        private void printRecord(OperatorView operator, List<String> body) {
            String label = String.format(
                    "{{%s}|%s|{%s}}",
                    operator.getInputs().stream()
                        .map(it -> String.format("<%s>%s",
                                getSimpleId(it),
                                analyzeInput(it).stream()
                                    .map(DrawUtil::escapeForRecord)
                                    .collect(Collectors.joining("\n"))))
                        .collect(Collectors.joining("|")),
                    body.stream()
                        .map(DrawUtil::escapeForRecord)
                        .collect(Collectors.joining("\n")),
                    operator.getOutputs().stream()
                        .map(it -> String.format("<%s>%s",
                                getSimpleId(it),
                                analyzeOutput(it).stream()
                                    .map(DrawUtil::escapeForRecord)
                                    .collect(Collectors.joining("\n"))))
                        .collect(Collectors.joining("|")));
            printGeneralVertex(operator, "record", Collections.singletonList(label));
        }

        private List<String> analyzeBody(OperatorView operator) {
            List<String> results = new ArrayList<>();
            if (showAll || showArgument) {
                operator.getParameters().stream()
                    .map(it -> String.format("%s: %s", it.getName(), it.getValue().getObject()))
                    .forEachOrdered(results::add);
            }
            return results;
        }

        private List<String> analyzeInput(InputView port) {
            List<String> results = new ArrayList<>();
            if (showAll || showPortName) {
                results.add(port.getName());
            }
            if (showAll || showPortType) {
                results.add(port.getDataType().getSimpleName());
            }
            if (showAll || showPortKey) {
                results.add(port.getGranulatity().toString());
                Optional.ofNullable(port.getGroup())
                    .map(InputGroup::toString)
                    .ifPresent(results::add);
            }
            return results;
        }

        private List<String> analyzeOutput(OutputView port) {
            List<String> results = new ArrayList<>();
            if (showAll || showPortName) {
                results.add(port.getName());
            }
            if (showAll || showPortType) {
                results.add(port.getDataType().getSimpleName());
            }
            return results;
        }

        private void printGeneralVertex(OperatorView operator, String shape, List<String> label) {
            writer.printf("%s [shape=%s, label=%s];%n",
                    getId(operator),
                    literal(shape),
                    literal(String.join("\n", label)));
        }

        private void printEdges(OperatorGraphView graph, int currentDepth) {
            for (OperatorView operator : graph.getOperators()) {
                printEdge(operator);
                if (operator.getSpec().getOperatorKind() == OperatorKind.FLOW
                        && currentDepth < limitDepth) {
                    printEdges(operator.getElementGraph(), currentDepth + 1);
                }
            }
        }

        private void printEdge(OperatorView operator) {
            // NOTE: we only put upstream -> downstream edges
            for (OutputView out : operator.getOutputs()) {
                String upstream = getQualifiedId(out);
                for (InputView in : out.getOpposites()) {
                    String downstream = getQualifiedId(in);
                    writer.printf("%s -> %s;%n",
                            upstream,
                            downstream);
                }
            }
        }
    }

    private static class Id {

        final Id parent;

        final int value;

        Id(Id parent, int value) {
            this.parent = parent;
            this.value = value;
        }

        String asSimple() {
            return '_' + Integer.toString(value, 36);
        }

        String asQualified() {
            if (parent == null) {
                return asSimple();
            } else {
                return parent.asSimple() + ':' + asSimple();
            }
        }
    }
}
