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
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.asakusafw.lang.info.BatchInfo;
import com.asakusafw.lang.info.JobflowInfo;
import com.asakusafw.lang.info.cli.DrawEngine.Feature;
import com.asakusafw.lang.info.operator.OperatorSpec.OperatorKind;
import com.asakusafw.lang.info.operator.view.OperatorGraphView;
import com.asakusafw.lang.info.operator.view.OperatorView;
import com.asakusafw.lang.info.plan.PlanAttribute;
import com.asakusafw.lang.info.plan.PlanVertexSpec;
import com.asakusafw.lang.info.value.ClassInfo;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

/**
 * A command for generating DOT script about operator graphs.
 * @since 0.4.2
 */
@Command(
        name = "plan",
        description = "Generates execution plan as Graphviz DOT script",
        hidden = false
)
public class DrawPlanCommand extends DrawCommand {

    @Option(
            name = { "--vertex", },
            title = "vertex name",
            description = "only displays elements in the plan vertex",
            arity = 1,
            required = false)
    String vertex;

    @Option(
            name = { "--show-operator", },
            title = "display operator",
            description = "display operators in vertices",
            arity = 0,
            required = false)
    boolean showOperator = false;

    @Option(
            name = { "--show-argument", },
            title = "display operator argument",
            description = "display operator argument",
            arity = 0,
            required = false)
    boolean showArgument = false;

    @Option(
            name = { "--show-edge-operation", },
            title = "display operations on edge",
            description = "display operations on edge",
            arity = 0,
            required = false)
    boolean showEdgeOperation = false;

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
            name = { "--show-type", },
            title = "display data type",
            description = "display data type",
            arity = 0,
            required = false)
    boolean showType = false;

    @Option(
            name = { "--show-key", },
            title = "display group key",
            description = "display group key",
            arity = 0,
            required = false)
    boolean showKey = false;

    @Option(
            name = { "--show-all", "-a", },
            title = "display all information",
            description = "display all information",
            arity = 0,
            required = false)
    boolean showAll = false;

    @Override
    protected void process(PrintWriter writer, BatchInfo batch, JobflowInfo jobflow) throws IOException {
        OperatorGraphView graph = jobflow.findAttribute(PlanAttribute.class)
                .map(OperatorGraphView::new)
                .orElseThrow(() -> new IllegalStateException("there are no available execution plans"));
        List<String> label = new ArrayList<>();
        label.add(Optional.ofNullable(jobflow.getDescriptionClass())
                .map(ClassInfo::of)
                .map(ClassInfo::getSimpleName)
                .orElse(jobflow.getId()));
        int depth;
        if (vertex == null) {
            depth = showOperator
                    || showArgument
                    || showExternalIo
                    || showPortName
                    || showType
                    || showKey
                    || showAll ? 2 : 1;
        } else  {
            OperatorView v = graph.getOperators(OperatorKind.PLAN_VERTEX).stream()
                    .filter(it -> ((PlanVertexSpec) it.getSpec()).getName().equals(vertex))
                    .findFirst()
                    .orElseThrow(() -> new IOException(MessageFormat.format(
                            "there are no vertex named \"{1}\" in jobflow {0}",
                            Optional.ofNullable(jobflow.getDescriptionClass())
                                .orElse(jobflow.getId()),
                            vertex)));
            graph = v.getElementGraph();
            if (graph.getOperators().isEmpty()) {
                throw new IOException(MessageFormat.format(
                        "there are no available operators in vertex \"{1}\" in jobflow {0}",
                        Optional.ofNullable(jobflow.getDescriptionClass())
                            .orElse(jobflow.getId()),
                        vertex));
            }
            PlanVertexSpec spec = (PlanVertexSpec) v.getSpec();
            label.add(spec.getName());
            depth = 1;
        }
        Set<Feature> features = extractFeatures();
        DrawEngine engine = new DrawEngine(features);
        engine.draw(writer, graph, depth, label, getGraphOptions(), null);
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
        if (showEdgeOperation) {
            results.add(Feature.EDGE_OPERATION);
        }
        if (showPortName) {
            results.add(Feature.PORT_NAME);
        }
        if (showType) {
            results.add(Feature.PORT_TYPE);
            results.add(Feature.EDGE_TYPE);
        }
        if (showKey) {
            results.add(Feature.PORT_KEY);
            results.add(Feature.EDGE_KEY);
        }
        return results;
    }
}
