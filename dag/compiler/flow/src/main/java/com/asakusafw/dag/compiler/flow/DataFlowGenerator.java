/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.flow;

import static com.asakusafw.dag.compiler.flow.DataFlowUtil.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.compiler.codegen.BufferOperatorGenerator;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.CoGroupInputAdapterGenerator;
import com.asakusafw.dag.compiler.codegen.CompositeOperatorNodeGenerator;
import com.asakusafw.dag.compiler.codegen.EdgeDataTableAdapterGenerator;
import com.asakusafw.dag.compiler.codegen.EdgeOutputAdapterGenerator;
import com.asakusafw.dag.compiler.codegen.ExtractAdapterGenerator;
import com.asakusafw.dag.compiler.codegen.ExtractInputAdapterGenerator;
import com.asakusafw.dag.compiler.codegen.OperationAdapterGenerator;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.AggregateNodeInfo;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.OperatorNodeInfo;
import com.asakusafw.dag.compiler.codegen.VertexAdapterGenerator;
import com.asakusafw.dag.compiler.flow.adapter.OperatorNodeGeneratorContextAdapter;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.build.GraphInfoBuilder;
import com.asakusafw.dag.compiler.model.build.ResolvedInputInfo;
import com.asakusafw.dag.compiler.model.build.ResolvedOutputInfo;
import com.asakusafw.dag.compiler.model.build.ResolvedVertexInfo;
import com.asakusafw.dag.compiler.model.graph.AggregateNode;
import com.asakusafw.dag.compiler.model.graph.DataTableNode;
import com.asakusafw.dag.compiler.model.graph.InputNode;
import com.asakusafw.dag.compiler.model.graph.OperationSpec;
import com.asakusafw.dag.compiler.model.graph.OperatorNode;
import com.asakusafw.dag.compiler.model.graph.OutputNode;
import com.asakusafw.dag.compiler.model.graph.ValueElement;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.compiler.model.graph.VertexElement.ElementKind;
import com.asakusafw.dag.compiler.model.plan.InputSpec;
import com.asakusafw.dag.compiler.model.plan.InputSpec.InputType;
import com.asakusafw.dag.compiler.model.plan.OutputSpec;
import com.asakusafw.dag.compiler.model.plan.OutputSpec.OutputType;
import com.asakusafw.dag.compiler.model.plan.VertexSpec;
import com.asakusafw.dag.compiler.model.plan.VertexSpec.OperationOption;
import com.asakusafw.dag.compiler.model.plan.VertexSpec.OperationType;
import com.asakusafw.dag.runtime.adapter.DataTable;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.MarkerOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorArgument;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorPort;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.Planning;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.flow.VoidResult;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * Generates data flow classes.
 * @since 0.4.0
 */
public final class DataFlowGenerator {

    static final Logger LOG = LoggerFactory.getLogger(DataFlowGenerator.class);

    private static final TypeDescription TYPE_RESULT = Descriptions.typeOf(Result.class);

    private static final TypeDescription TYPE_DATATABLE = Descriptions.typeOf(DataTable.class);

    private static final ClassDescription TYPE_VOID_RESULT = Descriptions.classOf(VoidResult.class);

    private final Plan plan;

    private final GraphInfoBuilder builder = new GraphInfoBuilder();

    private final JobflowProcessor.Context processorContext;

    private final ClassGeneratorContext generatorContext;

    private final DagDescriptorFactory descriptors;

    private final CompositeOperatorNodeGenerator genericOperators;

    private final ExternalPortDriver externalPortDriver;

    private DataFlowGenerator(
            JobflowProcessor.Context processorContext,
            ClassGeneratorContext generatorContext,
            DagDescriptorFactory descriptors,
            Plan plan) {
        this.plan = plan;
        this.processorContext = processorContext;
        this.generatorContext = generatorContext;
        this.descriptors = descriptors;

        JobflowProcessor.Context root = processorContext;
        this.genericOperators = CompositeOperatorNodeGenerator.load(root.getClassLoader());
        this.externalPortDriver = CompositeExternalPortDriver.load(
                new ExternalPortDriverProvider.Context(root.getOptions(), generatorContext, descriptors, plan));
    }

    /**
     * Generates {@link GraphInfo} and its related classes.
     * @param processorContext the current jobflow processor context
     * @param generatorContext the current class generator context
     * @param descriptors the DAG descriptor factory
     * @param info the target jobflow info
     * @param plan the target plan
     * @return the generated {@link GraphInfo}
     */
    public static GraphInfo generate(
            JobflowProcessor.Context processorContext,
            ClassGeneratorContext generatorContext,
            DagDescriptorFactory descriptors,
            JobflowInfo info,
            Plan plan) {
        return new DataFlowGenerator(processorContext, generatorContext, descriptors, plan).generate();
    }

    private GraphInfo generate() {
        GraphInfo graph = generateGraph();
        return graph;
    }

    private GraphInfo generateGraph() {
        resolveOutputs();
        resolveOperations();
        resolvePlan();
        return builder.build(descriptors::newVoidEdge);
    }

    private void resolveOperations() {
        Graph<SubPlan> graph = Planning.toDependencyGraph(plan);
        Graph<SubPlan> rev = Graphs.transpose(graph);
        for (SubPlan sub : Graphs.sortPostOrder(rev)) {
            VertexSpec spec = VertexSpec.get(sub);
            if (sub.getOperators().stream().anyMatch(o -> o.getOperatorKind() == OperatorKind.OUTPUT)) {
                continue;
            }
            resolveOperation(spec);
        }
    }

    private void resolveOperation(VertexSpec vertex) {
        LOG.debug("compiling operation vertex: {} ({})", vertex.getId(), vertex.getLabel()); //$NON-NLS-1$
        Map<Operator, VertexElement> resolved = resolveVertexElements(vertex);
        ClassDescription inputAdapter = resolveInputAdapter(resolved, vertex);
        List<ClassDescription> dataTableAdapters = resolveDataTableAdapters(resolved, vertex);
        ClassDescription operationAdapter = resolveOperationAdapter(resolved, vertex);
        ClassDescription outputAdapter = resolveOutputAdapter(resolved, vertex);
        ClassDescription vertexClass = generate(vertex, "vertex", c -> {
            return new VertexAdapterGenerator().generate(
                    generatorContext,
                    inputAdapter,
                    dataTableAdapters,
                    operationAdapter,
                    Collections.singletonList(outputAdapter),
                    vertex.getLabel(),
                    c);
        });
        Map<SubPlan.Input, ResolvedInputInfo> inputs = collectInputs(resolved, vertex);
        Map<SubPlan.Output, ResolvedOutputInfo> outputs = collectOutputs(vertex);
        ResolvedVertexInfo info = new ResolvedVertexInfo(
                vertex.getId(),
                descriptors.newVertex(vertexClass),
                inputs,
                outputs);
        register(builder, vertex, info, vertexClass);
    }

    private Map<Operator, VertexElement> resolveVertexElements(VertexSpec vertex) {
        Map<Operator, VertexElement> resolved = new HashMap<>();
        for (SubPlan.Input port : vertex.getOrigin().getInputs()) {
            InputSpec spec = InputSpec.get(port);
            if (spec.getInputType() != InputType.BROADCAST) {
                continue;
            }
            resolved.put(port.getOperator(), new DataTableNode(spec.getId(), TYPE_DATATABLE, spec.getDataType()));
        }
        Graph<Operator> graph = Graphs.transpose(Planning.toDependencyGraph(vertex.getOrigin()));
        for (Operator operator : Graphs.sortPostOrder(graph)) {
            resolveBodyOperator(resolved, vertex, operator);
        }
        return resolved;
    }

    private ClassDescription resolveInputAdapter(Map<Operator, VertexElement> resolved, VertexSpec vertex) {
        Operator primary = vertex.getPrimaryOperator();
        if (vertex.getOperationOptions().contains(OperationOption.EXTERNAL_INPUT)) {
            Invariants.require(primary instanceof ExternalInput);
            ExternalInput input = (ExternalInput) primary;
            VertexElement driver = resolveExtractDriver(resolved, vertex, input.getOperatorPort());
            resolved.put(input, new InputNode(driver));
            return resolveExternalInput(vertex, input);
        } else if (vertex.getOperationType() == OperationType.CO_GROUP) {
            Invariants.requireNonNull(primary);
            VertexElement element = resolved.get(primary);
            Invariants.requireNonNull(element);

            List<InputSpec> inputs = new ArrayList<>();
            for (OperatorInput input : primary.getInputs()) {
                Collection<OperatorOutput> opposites = input.getOpposites();
                if (opposites.isEmpty()) {
                    continue;
                }
                Invariants.require(opposites.size() == 1);
                opposites.stream()
                        .map(OperatorPort::getOwner)
                        .map(p -> Invariants.requireNonNull(vertex.getOrigin().findInput(p)))
                        .map(InputSpec::get)
                        .filter(s -> s.getInputType() == InputType.CO_GROUP)
                        .forEach(inputs::add);
            }
            Invariants.require(inputs.size() >= 1);

            // Note: only add the first input
            resolved.put(inputs.get(0).getOrigin().getOperator(), new InputNode(element));
            return resolveCoGroupInput(vertex, inputs);
        } else {
            List<InputSpec> inputs = vertex.getOrigin().getInputs().stream()
                .map(InputSpec::get)
                .filter(s -> s.getInputType() == InputType.EXTRACT)
                .collect(Collectors.toList());
            Invariants.require(inputs.size() == 1);
            MarkerOperator edge = inputs.get(0).getOrigin().getOperator();
            VertexElement driver = resolveExtractDriver(resolved, vertex, edge.getOutput());
            resolved.put(edge, new InputNode(driver));
            return resolveExtractInput(vertex, inputs.get(0));
        }
    }

    private VertexElement resolveExtractDriver(
            Map<Operator, VertexElement> resolved, VertexSpec vertex, OperatorOutput output) {
        VertexElement succ = resolveSuccessors(resolved, vertex, output);
        ClassDescription gen = generate(vertex, "operator", c -> {
            return new ExtractAdapterGenerator().generate(succ, c);
        });
        return new OperatorNode(gen, TYPE_RESULT, output.getDataType(), succ);
    }

    private ClassDescription resolveExtractInput(VertexSpec vertex, InputSpec spec) {
        ExtractInputAdapterGenerator.Spec s = new ExtractInputAdapterGenerator.Spec(spec.getId(), spec.getDataType());
        return generate(vertex, "adapter.input", c -> {
            return new ExtractInputAdapterGenerator().generate(generatorContext, s, c);
        });
    }

    private ClassDescription resolveCoGroupInput(VertexSpec vertex, List<InputSpec> inputs) {
        List<CoGroupInputAdapterGenerator.Spec> specs = inputs.stream()
                .map(s -> new CoGroupInputAdapterGenerator.Spec(
                        s.getId(),
                        s.getDataType(),
                        s.getInputOptions().contains(InputSpec.InputOption.SPILL_OUT)))
                .collect(Collectors.toList());
        return generate(vertex, "adapter.input", c -> {
            return new CoGroupInputAdapterGenerator().generate(generatorContext, specs, c);
        });
    }

    private ClassDescription resolveExternalInput(VertexSpec vertex, ExternalInput input) {
        if (externalPortDriver.accepts(input)) {
            return externalPortDriver.processInput(input);
        }
        return resolveGenericInput(vertex, input);
    }

    private ClassDescription resolveGenericInput(VertexSpec vertex, ExternalInput input) {
        ExternalInputReference ref = processorContext.addExternalInput(vertex.getId(), input.getInfo());
        return generateInternalInput(generatorContext, vertex, input, ref.getPaths());
    }

    private ClassDescription resolveOutputAdapter(Map<Operator, VertexElement> resolved, VertexSpec vertex) {
        List<EdgeOutputAdapterGenerator.Spec> specs = new ArrayList<>();
        for (SubPlan.Output port : vertex.getOrigin().getOutputs()) {
            if (resolved.containsKey(port.getOperator()) == false) {
                continue;
            }
            OutputSpec spec = OutputSpec.get(port);
            if (spec.getOutputType() == OutputType.DISCARD) {
                continue;
            }
            ClassDescription mapperClass = null;
            ClassDescription copierClass = null;
            ClassDescription combinerClass = null;
            Set<? extends SubPlan.Input> opposites = port.getOpposites();
            if (opposites.isEmpty() == false) {
                SubPlan.Input first = opposites.stream().findFirst().get();
                ResolvedInputInfo downstream = builder.get(first);
                mapperClass = downstream.getMapperType();
                copierClass = downstream.getCopierType();
                combinerClass = downstream.getCombinerType();
            }
            specs.add(new EdgeOutputAdapterGenerator.Spec(
                    spec.getId(), spec.getDataType(),
                    mapperClass, copierClass, combinerClass));
        }
        return generate(vertex, "adapter.output", c -> {
            return new EdgeOutputAdapterGenerator().generate(generatorContext, specs, c);
        });
    }

    private List<ClassDescription> resolveDataTableAdapters(Map<Operator, VertexElement> resolved, VertexSpec vertex) {
        List<EdgeDataTableAdapterGenerator.Spec> specs = new ArrayList<>();
        for (SubPlan.Input port : vertex.getOrigin().getInputs()) {
            if (resolved.containsKey(port.getOperator()) == false) {
                continue;
            }
            InputSpec spec = InputSpec.get(port);
            if (spec.getInputType() != InputType.BROADCAST) {
                continue;
            }
            Group group = Invariants.requireNonNull(spec.getPartitionInfo());
            specs.add(new EdgeDataTableAdapterGenerator.Spec(spec.getId(), spec.getId(), spec.getDataType(), group));
        }
        if (specs.isEmpty()) {
            return Collections.emptyList();
        }
        ClassDescription generated = generate(vertex, "adapter.table", c -> {
            return new EdgeDataTableAdapterGenerator().generate(generatorContext, specs, c);
        });
        return Collections.singletonList(generated);
    }

    private ClassDescription resolveOperationAdapter(Map<Operator, VertexElement> resolved, VertexSpec vertex) {
        return generate(vertex, "adapter.operation", c -> {
            OperationSpec operation = null;
            for (VertexElement element : resolved.values()) {
                if (element instanceof InputNode) {
                    Invariants.require(operation == null);
                    operation = new OperationSpec((InputNode) element);
                }
            }
            Invariants.requireNonNull(operation);
            return new OperationAdapterGenerator().generate(generatorContext, operation, c);
        });
    }

    private void resolveBodyOperator(Map<Operator, VertexElement> resolved, VertexSpec vertex, Operator operator) {
        switch (operator.getOperatorKind()) {
        case CORE:
        case USER:
            resolveGeneralOperator(resolved, vertex, operator);
            break;
        case MARKER:
            if (vertex.getOrigin().findOutput(operator) != null) {
                resolveEdgeOutput(resolved, vertex, vertex.getOrigin().findOutput(operator));
            }
            break;
        default:
            break;
        }
    }

    private void resolveGeneralOperator(Map<Operator, VertexElement> resolved, VertexSpec vertex, Operator operator) {
        Map<OperatorProperty, VertexElement> dependencies = new LinkedHashMap<>();
        // add broadcast inputs as data tables
        for (OperatorInput port : operator.getInputs()) {
            if (port.getOpposites().size() != 1) {
                continue;
            }
            VertexElement upstream = port.getOpposites().stream()
                .map(OperatorPort::getOwner)
                .map(resolved::get)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
            if (upstream != null && upstream.getElementKind() == ElementKind.DATA_TABLE) {
                dependencies.put(port, upstream);
            }
        }
        // add outputs as succeeding result sinks
        for (OperatorOutput port : operator.getOutputs()) {
            VertexElement successor = resolveSuccessors(resolved, vertex, port);
            dependencies.put(port, successor);
        }
        // add arguments as constant values
        for (OperatorArgument arg : operator.getArguments()) {
            VertexElement value = new ValueElement(arg.getValue());
            dependencies.put(arg, value);
        }
        OperatorNodeGeneratorContextAdapter adapter =
                new OperatorNodeGeneratorContextAdapter(generatorContext, vertex.getOrigin(), dependencies);
        NodeInfo info = genericOperators.generate(adapter, operator);
        VertexElement element = resolveNodeInfo(vertex, operator, info);
        resolved.put(operator, element);
        generatorContext.addClassFile(info.getClassData());
    }

    private VertexElement resolveNodeInfo(VertexSpec vertex, Operator operator, NodeInfo info) {
        if (info instanceof OperatorNodeInfo) {
            return new OperatorNode(
                    info.getClassData().getDescription(),
                    TYPE_RESULT,
                    info.getDataType(),
                    info.getDependencies());
        } else if (info instanceof AggregateNodeInfo) {
            AggregateNodeInfo aggregate = (AggregateNodeInfo) info;
            boolean combine = vertex.getOperationOptions().contains(OperationOption.PRE_AGGREGATION);
            return new AggregateNode(
                    info.getClassData().getDescription(),
                    TYPE_RESULT,
                    aggregate.getMapperType(),
                    combine ? aggregate.getCopierType() : null,
                    combine ? aggregate.getCombinerType() : null,
                    aggregate.getInputType(),
                    aggregate.getOutputType(),
                    info.getDependencies());
        } else {
            throw new AssertionError(info);
        }
    }

    private VertexElement resolveSuccessors(
            Map<Operator, VertexElement> resolved, VertexSpec vertex, OperatorOutput port) {
        List<VertexElement> succs = Operators.getSuccessors(Collections.singleton(port)).stream()
                .map(o -> Invariants.requireNonNull(resolved.get(o)))
                .collect(Collectors.toList());
        if (succs.size() == 0) {
            return new OperatorNode(TYPE_VOID_RESULT, TYPE_RESULT, port.getDataType(), Collections.emptyList());
        } else if (succs.size() == 1) {
            return succs.get(0);
        } else {
            ClassDescription buffer = BufferOperatorGenerator.get(generatorContext, succs);
            return new OperatorNode(buffer, TYPE_RESULT, port.getDataType(), succs);
        }
    }

    private void resolveEdgeOutput(Map<Operator, VertexElement> resolved, VertexSpec vertex, SubPlan.Output port) {
        OutputSpec spec = OutputSpec.get(port);
        VertexElement element;
        if (spec.getOutputType() == OutputType.DISCARD) {
            element = new OperatorNode(TYPE_VOID_RESULT, TYPE_RESULT, spec.getSourceType(), Collections.emptyList());
        } else {
            element = new OutputNode(spec.getId(), TYPE_RESULT, spec.getSourceType());
        }
        MarkerOperator operator = port.getOperator();
        resolved.put(operator, element);
    }

    private Map<SubPlan.Input, ResolvedInputInfo> collectInputs(
            Map<Operator, VertexElement> resolved, VertexSpec vertex) {
        Map<SubPlan.Input, ResolvedInputInfo> results = new LinkedHashMap<>();
        for (SubPlan.Input port : vertex.getOrigin().getInputs()) {
            InputSpec spec = InputSpec.get(port);
            InputType type = spec.getInputType();
            if (type == InputType.EXTRACT) {
                ResolvedInputInfo info = new ResolvedInputInfo(
                        spec.getId(),
                        descriptors.newOneToOneEdge(spec.getDataType()));
                results.put(port, info);
            } else if (type == InputType.BROADCAST) {
                ResolvedInputInfo info = new ResolvedInputInfo(
                        spec.getId(),
                        descriptors.newBroadcastEdge(spec.getDataType()));
                results.put(port, info);
            } else if (type == InputType.CO_GROUP) {
                ClassDescription mapperType = null;
                ClassDescription copierType = null;
                ClassDescription combinerType = null;
                Operator operator = Invariants.requireNonNull(vertex.getPrimaryOperator());
                VertexElement element = Invariants.requireNonNull(resolved.get(operator));
                if (element instanceof AggregateNode) {
                    AggregateNode aggregate = (AggregateNode) element;
                    mapperType = aggregate.getMapperType();
                    copierType = aggregate.getCopierType();
                    combinerType = aggregate.getCombinerType();
                }
                ResolvedInputInfo info = new ResolvedInputInfo(
                        spec.getId(),
                        descriptors.newScatterGatherEdge(spec.getDataType(), spec.getPartitionInfo()),
                        mapperType, copierType, combinerType);
                results.put(port, info);
            }
        }
        return results;
    }

    private Map<SubPlan.Output, ResolvedOutputInfo> collectOutputs(VertexSpec vertex) {
        Map<SubPlan.Output, ResolvedOutputInfo> results = new LinkedHashMap<>();
        for (SubPlan.Output port : vertex.getOrigin().getOutputs()) {
            OutputSpec spec = OutputSpec.get(port);
            if (spec.getOutputType() == OutputType.DISCARD) {
                continue;
            }
            Set<ResolvedInputInfo> downstreams = port.getOpposites().stream()
                    .map(p -> Invariants.requireNonNull(builder.get(p)))
                    .collect(Collectors.toSet());
            String tag = getPortTag(port);
            ResolvedOutputInfo info = new ResolvedOutputInfo(spec.getId(), tag, downstreams);
            results.put(port, info);
        }
        return results;
    }

    private void resolveOutputs() {
        externalPortDriver.processOutputs(builder);
        Map<ExternalOutput, VertexSpec> generic = collectOperators(plan, ExternalOutput.class).stream()
            .filter(port -> externalPortDriver.accepts(port) == false)
            .collect(Collectors.collectingAndThen(
                    Collectors.toList(),
                    operators -> collectOwners(plan, operators)));
        for (Map.Entry<ExternalOutput, VertexSpec> entry : generic.entrySet()) {
            ExternalOutput port = entry.getKey();
            VertexSpec vertex = entry.getValue();
            resolveGenericOutput(vertex, port);
        }
    }

    private void resolveGenericOutput(VertexSpec vertex, ExternalOutput port) {
        LOG.debug("resolving generic output vertex: {} ({})", vertex.getId(), vertex.getLabel()); //$NON-NLS-1$
        if (isEmptyOutput(vertex)) {
            processorContext.addExternalOutput(port.getName(), port.getInfo(), Collections.emptyList());
        } else {
            CompilerOptions options = processorContext.getOptions();
            String path = options.getRuntimeWorkingPath(String.format("%s/part-*", port.getName())); //$NON-NLS-1$
            processorContext.addExternalOutput(port.getName(), port.getInfo(), Collections.singletonList(path));
            registerInternalOutput(generatorContext, descriptors, builder, vertex, port, path);
        }
    }

    private void resolvePlan() {
        externalPortDriver.processPlan(builder);
    }

    private ClassDescription generate(
            VertexSpec vertex, String name, Function<ClassDescription, ClassData> generator) {
        return DataFlowUtil.generate(generatorContext, vertex, name, generator);
    }
}
