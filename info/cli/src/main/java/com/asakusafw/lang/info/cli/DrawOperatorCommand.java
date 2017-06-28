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

import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import com.asakusafw.lang.info.BatchInfo;
import com.asakusafw.lang.info.JobflowInfo;
import com.asakusafw.lang.info.cli.DrawEngine.Feature;
import com.asakusafw.lang.info.operator.FlowOperatorSpec;
import com.asakusafw.lang.info.operator.OperatorGraphAttribute;
import com.asakusafw.lang.info.operator.OperatorSpec.OperatorKind;
import com.asakusafw.lang.info.operator.view.OperatorGraphView;
import com.asakusafw.lang.info.operator.view.OperatorView;
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
public class DrawOperatorCommand extends DrawCommand {

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
            description = "only displays in the flow-part",
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
        List<String> label = new ArrayList<>();
        label.add(Optional.ofNullable(jobflow.getDescriptionClass())
                    .map(ClassInfo::of)
                    .map(ClassInfo::getSimpleName)
                    .orElse(jobflow.getId()));
        if (flowPart != null) {
            OperatorView fp = findFlowPart(graph)
                    .orElseThrow(() -> new IOException(MessageFormat.format(
                            "there are no flow part named \"{1}\" in jobflow {0}",
                            Optional.ofNullable(jobflow.getDescriptionClass())
                                .orElse(jobflow.getId()),
                            flowPart)));
            graph = fp.getElementGraph();
            FlowOperatorSpec spec = (FlowOperatorSpec) fp.getSpec();
            Optional.of(spec.getDescriptionClass())
                    .map(ClassInfo::getSimpleName)
                    .ifPresent(label::add);
        }
        Set<Feature> features = extractFeatures();
        DrawEngine engine = new DrawEngine(features);
        engine.draw(writer, graph, limitDepth, label, getGraphOptions(), null);
    }

    private Set<Feature> extractFeatures() {
        if (showAll) {
            return EnumSet.allOf(Feature.class);
        }
        Set<DrawEngine.Feature> results = EnumSet.noneOf(Feature.class);
        if (showArgument) {
            results.add(Feature.ARGUMENT);
        }
        if (showExternalIo) {
            results.add(Feature.EXTERNAL_IO_CLASS);
        }
        if (showPortName) {
            results.add(Feature.PORT_NAME);
        }
        if (showPortKey) {
            results.add(Feature.PORT_KEY);
        }
        if (showPortType) {
            results.add(Feature.PORT_TYPE);
        }
        return results;
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
        return graph.getOperators(OperatorKind.FLOW).stream();
    }
}
