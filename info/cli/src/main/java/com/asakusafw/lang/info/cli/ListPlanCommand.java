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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.asakusafw.lang.info.BatchInfo;
import com.asakusafw.lang.info.JobflowInfo;
import com.asakusafw.lang.info.operator.OperatorSpec.OperatorKind;
import com.asakusafw.lang.info.operator.UserOperatorSpec;
import com.asakusafw.lang.info.operator.view.OperatorGraphView;
import com.asakusafw.lang.info.operator.view.OperatorView;
import com.asakusafw.lang.info.operator.view.OutputView;
import com.asakusafw.lang.info.plan.PlanAttribute;
import com.asakusafw.lang.info.plan.PlanVertexSpec;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

/**
 * A command for printing operators.
 * @since 0.4.2
 */
@Command(
        name = "plan",
        description = "Displays execution plan",
        hidden = false
)
public class ListPlanCommand extends SingleJobflowInfoCommand {

    @Option(
            name = { "--vertex", },
            title = "vertex name",
            description = "only displays elements in the plan vertex",
            arity = 1,
            required = false)
    String vertex;

    @Option(
            name = { "--verbose", "-v", },
            title = "verbose mode",
            description = "verbose mode",
            arity = 0,
            required = false)
    boolean showVerbose = false;

    @Override
    protected void process(PrintWriter writer, BatchInfo batch, JobflowInfo jobflow) throws IOException {
        List<OperatorView> vertices = jobflow.findAttribute(PlanAttribute.class)
                .map(OperatorGraphView::new)
                .filter(it -> it.getOperators().isEmpty() == false)
                .orElseThrow(() -> new IllegalStateException("there are no available execution plans"))
                .getOperators(OperatorKind.PLAN_VERTEX)
                .stream()
                .sorted(Comparator.comparing((OperatorView it) -> ((PlanVertexSpec) it.getSpec()).getName()))
                .collect(Collectors.toList());
        if (vertex == null) {
            vertices.forEach(it -> {
                PlanVertexSpec spec = (PlanVertexSpec) it.getSpec();
                if (showVerbose) {
                    writer.printf("%s:%n", spec.getName());
                    printVertex(writer, 4, it);
                } else {
                    writer.println(spec.getName());
                }
            });
        } else {
            OperatorView target = vertices.stream()
                    .filter(it -> ((PlanVertexSpec) it.getSpec()).getName().equals(vertex))
                    .findFirst()
                    .orElseThrow(() -> new IOException(MessageFormat.format(
                            "there are no vertex named \"{1}\" in jobflow {0}",
                            Optional.ofNullable(jobflow.getDescriptionClass())
                                .orElse(jobflow.getId()),
                            vertex)));
            printVertex(writer, 0, target);
        }
    }

    private static void printVertex(PrintWriter writer, int indent, OperatorView vertex) {
        PlanVertexSpec spec = (PlanVertexSpec) vertex.getSpec();
        writer.printf("%slabel: %s%n", ListUtil.padding(indent), ListUtil.normalize(spec.getLabel()));
        writer.printf("%sblockers: %s%n", ListUtil.padding(indent), Stream.concat(
                    spec.getDependencies().stream(),
                    vertex.getInputs().stream()
                        .flatMap(p -> p.getOpposites().stream())
                        .map(OutputView::getOwner)
                        .filter(v -> v.getSpec().getOperatorKind() == OperatorKind.PLAN_VERTEX)
                        .map(v -> ((PlanVertexSpec) v.getSpec()).getName()))
                .sorted()
                .distinct()
                .collect(Collectors.joining(", ", "{", "}")));
        ListUtil.printBlock(writer, indent, "operators", vertex.getElementGraph()
                .getOperators(OperatorKind.USER)
                .stream()
                .map(op -> (UserOperatorSpec) op.getSpec())
                .map(it -> String.format("%s#%s(@%s)",
                        it.getDeclaringClass().getName(),
                        it.getMethodName(),
                        it.getAnnotation().getDeclaringClass().getSimpleName()))
                .distinct()
                .collect(Collectors.toList()));
    }
}
