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
package com.asakusafw.dag.compiler.extension.directio;

import static com.asakusafw.dag.compiler.flow.DataFlowUtil.*;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.directio.DirectFileInputAdapterGenerator;
import com.asakusafw.dag.compiler.directio.DirectFileOutputCommitGenerator;
import com.asakusafw.dag.compiler.directio.DirectFileOutputPrepareGenerator;
import com.asakusafw.dag.compiler.directio.DirectFileOutputSetupGenerator;
import com.asakusafw.dag.compiler.directio.OutputPatternSerDeGenerator;
import com.asakusafw.dag.compiler.flow.DagDescriptorFactory;
import com.asakusafw.dag.compiler.flow.ExternalPortDriver;
import com.asakusafw.dag.compiler.flow.ExternalPortDriverProvider;
import com.asakusafw.dag.compiler.model.build.GraphInfoBuilder;
import com.asakusafw.dag.compiler.model.build.ResolvedEdgeInfo;
import com.asakusafw.dag.compiler.model.build.ResolvedInputInfo;
import com.asakusafw.dag.compiler.model.build.ResolvedVertexInfo;
import com.asakusafw.dag.compiler.model.plan.VertexSpec;
import com.asakusafw.dag.runtime.directio.DirectFileOutputPrepare;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.extension.directio.DirectFileInputModel;
import com.asakusafw.lang.compiler.extension.directio.DirectFileIoConstants;
import com.asakusafw.lang.compiler.extension.directio.DirectFileIoPortProcessor;
import com.asakusafw.lang.compiler.extension.directio.DirectFileOutputModel;
import com.asakusafw.lang.compiler.extension.directio.OutputPattern;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * An implementation of {@link ExternalPortDriver} for Direct file I/O ports.
 * @since 0.4.0
 */
public class DirectFilePortDriver implements ExternalPortDriver {

    private static final String ID_OUTPUT_SETUP = "_directio-setup";

    private static final String ID_OUTPUT_COMMIT = "_directio-commit";

    static final Logger LOG = LoggerFactory.getLogger(DirectFilePortDriver.class);

    private final Plan plan;

    private final CompilerOptions options;

    private final ClassGeneratorContext context;

    private final DagDescriptorFactory descriptors;

    private final Map<ExternalInput, DirectFileInputModel> inputModels;

    private final Map<ExternalOutput, DirectFileOutputModel> outputModels;

    private final Map<ExternalInput, VertexSpec> inputOwners;

    private final Map<ExternalOutput, VertexSpec> outputOwners;

    DirectFilePortDriver(ExternalPortDriverProvider.Context context) {
        Arguments.requireNonNull(context);
        this.plan = context.getSourcePlan();
        this.options = context.getOptions();
        this.context = context.getGeneratorContext();
        this.descriptors = context.getDescriptorFactory();
        this.inputModels = collectModels(
                collectOperators(plan, ExternalInput.class),
                DirectFileIoConstants.MODULE_NAME, DirectFileInputModel.class);
        this.outputModels = collectModels(
                collectOperators(plan, ExternalOutput.class),
                DirectFileIoConstants.MODULE_NAME, DirectFileOutputModel.class);
        this.inputOwners = collectOwners(plan, inputModels.keySet());
        this.outputOwners = collectOwners(plan, outputModels.keySet());
    }

    @Override
    public boolean accepts(ExternalInput port) {
        return inputModels.containsKey(port);
    }

    @Override
    public boolean accepts(ExternalOutput port) {
        return outputModels.containsKey(port);
    }

    @Override
    public ClassDescription processInput(ExternalInput port) {
        VertexSpec vertex = Invariants.requireNonNull(inputOwners.get(port));
        DirectFileInputModel model = Invariants.requireNonNull(inputModels.get(port));
        ClassDescription filterClass = model.getFilterClass();
        if (filterClass != null && isInputFilterEnabled() == false) {
            LOG.info(MessageFormat.format(
                    "Direct I/O input filter is disabled in current setting: {0} ({1})",
                    port.getInfo().getDescriptionClass().getClassName(),
                    filterClass.getClassName()));
            filterClass = null;
        }
        DirectFileInputAdapterGenerator.Spec spec = new DirectFileInputAdapterGenerator.Spec(
                port.getName(),
                model.getBasePath(),
                model.getResourcePattern(),
                model.getFormatClass(),
                filterClass,
                model.isOptional());
        return generate(context, vertex, "input.directio", c -> {
            return new DirectFileInputAdapterGenerator().generate(context, spec, c);
        });
    }

    @Override
    public void processOutputs(GraphInfoBuilder target) {
        ResolvedVertexInfo setup = registerOutputSetup(target);
        if (setup == null) {
            return;
        }
        List<ResolvedVertexInfo> prepares = new ArrayList<>();
        for (Map.Entry<ExternalOutput, VertexSpec> entry : outputOwners.entrySet()) {
            ExternalOutput port = entry.getKey();
            VertexSpec vertex = entry.getValue();
            if (isEmptyOutput(vertex)) {
                continue;
            }
            DirectFileOutputModel model = Invariants.requireNonNull(outputModels.get(port));
            ResolvedVertexInfo v = registerOutputPrepare(target, vertex, port, model, setup);
            prepares.add(v);
        }
        registerOutputCommit(target, prepares);
    }

    private ResolvedVertexInfo registerOutputSetup(GraphInfoBuilder builder) {
        List<DirectFileOutputSetupGenerator.Spec> specs = outputModels.entrySet().stream()
                .map(e -> new Tuple<>(e.getKey().getName(), e.getValue()))
                .map(t -> new DirectFileOutputSetupGenerator.Spec(
                        t.left(),
                        t.right().getBasePath(),
                        t.right().getDeletePatterns()))
                .collect(Collectors.toList());
        if (specs.isEmpty()) {
            return null;
        }
        LOG.debug("resolving Direct I/O file output setup vertex: {} ({})", //$NON-NLS-1$
                ID_OUTPUT_SETUP, specs.size());
        ClassDescription proc = generate(context, "directio.setup", c -> {
            return new DirectFileOutputSetupGenerator().generate(context, specs, c);
        });
        ResolvedVertexInfo info = new ResolvedVertexInfo(
                ID_OUTPUT_SETUP,
                descriptors.newVertex(proc),
                null,
                Collections.emptyMap(),
                Collections.emptyMap());
        return register(builder, plan, info, proc);
    }

    private ResolvedVertexInfo registerOutputPrepare(
            GraphInfoBuilder builder,
            VertexSpec vertex, ExternalOutput output,
            DirectFileOutputModel model, ResolvedVertexInfo setup) {
        LOG.debug("resolving Direct I/O file output prepare vertex: {} ({})", //$NON-NLS-1$
                vertex.getId(), vertex.getLabel());
        DataModelReference ref = context.getDataModelLoader().load(output.getDataType());
        OutputPattern pattern = OutputPattern.compile(ref, model.getResourcePattern(), model.getOrder());
        boolean gather = pattern.isGatherRequired();
        List<DirectFileOutputPrepareGenerator.Spec> specs = new ArrayList<>();
        specs.add(new DirectFileOutputPrepareGenerator.Spec(
                output.getName(),
                model.getBasePath(),
                gather ? null : model.getResourcePattern(),
                model.getFormatClass()));
        ResolvedEdgeInfo edge;
        if (gather) {
            ClassDescription serde = generate(context, vertex, "serde.directio", c -> { //$NON-NLS-1$
                return new OutputPatternSerDeGenerator().generate(ref, pattern, c);
            });
            Group group = new Group(Collections.emptyList(), pattern.getOrders().stream()
                    .map(o -> new Group.Ordering(
                            o.getTarget().getName(),
                            o.isAscend() ? Group.Direction.ASCENDANT : Group.Direction.DESCENDANT))
                    .collect(Collectors.toList()));
            edge = new ResolvedEdgeInfo(
                    descriptors.newScatterGatherEdge(output.getDataType(), serde, group),
                    ResolvedEdgeInfo.Movement.SCATTER_GATHER,
                    output.getDataType(),
                    group);
        } else {
            edge = new ResolvedEdgeInfo(
                    descriptors.newOneToOneEdge(output.getDataType()),
                    ResolvedEdgeInfo.Movement.ONE_TO_ONE,
                    output.getDataType(),
                    null);
        }
        ClassDescription vertexClass = generate(context, vertex, "output.directio", c -> { //$NON-NLS-1$
            return new DirectFileOutputPrepareGenerator().generate(context, specs, c);
        });
        SubPlan.Input entry = getOutputSource(vertex);
        ResolvedInputInfo input = new ResolvedInputInfo(
                DirectFileOutputPrepare.INPUT_NAME,
                edge);
        ResolvedVertexInfo info = new ResolvedVertexInfo(
                vertex.getId(),
                descriptors.newVertex(vertexClass),
                String.valueOf(output),
                Collections.singletonMap(entry, input),
                Collections.emptyMap(),
                Collections.singleton(setup));
        return register(builder, vertex, info, vertexClass);
    }

    private ResolvedVertexInfo registerOutputCommit(
            GraphInfoBuilder builder,
            Collection<? extends ResolvedVertexInfo> prepares) {
        List<DirectFileOutputCommitGenerator.Spec> specs = outputModels.entrySet().stream()
                .map(e -> new Tuple<>(e.getKey().getName(), e.getValue()))
                .map(t -> new DirectFileOutputCommitGenerator.Spec(t.left(), t.right().getBasePath()))
                .collect(Collectors.toList());
        if (specs.isEmpty()) {
            return null;
        }
        LOG.debug("resolving Direct I/O file output commit vertex: {} ({})", //$NON-NLS-1$
                ID_OUTPUT_COMMIT, specs.size());
        ClassDescription proc = generate(context, "directio.commit", c -> {
            return new DirectFileOutputCommitGenerator().generate(context, specs, c);
        });
        ResolvedVertexInfo info = new ResolvedVertexInfo(
                ID_OUTPUT_COMMIT,
                descriptors.newVertex(proc),
                null,
                Collections.emptyMap(),
                Collections.emptyMap(),
                prepares);
        return register(builder, plan, info, proc);
    }

    private boolean isInputFilterEnabled() {
        return options.get(
                DirectFileIoPortProcessor.OPTION_FILTER_ENABLED,
                DirectFileIoPortProcessor.DEFAULT_FILTER_ENABLED);
    }
}
