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
package com.asakusafw.dag.compiler.flow;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.internalio.InternalInputAdapterGenerator;
import com.asakusafw.dag.compiler.internalio.InternalOutputPrepareGenerator;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.build.GraphInfoBuilder;
import com.asakusafw.dag.compiler.model.build.ResolvedEdgeInfo;
import com.asakusafw.dag.compiler.model.build.ResolvedInputInfo;
import com.asakusafw.dag.compiler.model.build.ResolvedVertexInfo;
import com.asakusafw.dag.compiler.model.plan.Implementation;
import com.asakusafw.dag.compiler.model.plan.InputSpec;
import com.asakusafw.dag.compiler.model.plan.InputSpec.InputOption;
import com.asakusafw.dag.compiler.model.plan.InputSpec.InputType;
import com.asakusafw.dag.compiler.model.plan.OutputSpec;
import com.asakusafw.dag.compiler.model.plan.VertexSpec;
import com.asakusafw.dag.compiler.model.plan.VertexSpec.OperationType;
import com.asakusafw.dag.runtime.adapter.InputAdapter;
import com.asakusafw.dag.runtime.internalio.InternalOutputPrepare;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.ExternalPort;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operators;
import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.compiler.planning.SubPlan;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * Code generation utilities for Asakusa DAG compiler.
 * @since 0.4.0
 */
public final class DataFlowUtil {

    static final Logger LOG = LoggerFactory.getLogger(DataFlowUtil.class);

    private DataFlowUtil() {
        return;
    }

    /**
     * Collects external port models from the given ports.
     * @param <P> the port type
     * @param <M> the port model type
     * @param ports the external ports
     * @param resolver resolves each port and optionally returns the corresponded port model
     * @return the collected ports and their models
     */
    public static <P extends ExternalPort, M> Map<P, M> collectModels(
            Collection<P> ports,
            Function<? super P, ? extends Optional<? extends M>> resolver) {
        return ports.stream()
                .map(p -> new Tuple<>(p, resolver.apply(p)))
                .filter(t -> t.right().isPresent())
                .map(t -> new Tuple<>(t.left(), t.right().get()))
                .collect(Collectors.toMap(Tuple::left, Tuple::right));
    }

    /**
     * Collects external port models from the given ports.
     * @param <P> the port type
     * @param <M> the port model type
     * @param ports the external ports
     * @param moduleName the module name of target ports
     * @param modelType the target model type
     * @return the collected ports and their models
     */
    public static <P extends ExternalPort, M> Map<P, M> collectModels(
            Collection<P> ports, String moduleName, Class<M> modelType) {
        ClassLoader classLoader = modelType.getClassLoader();
        Function<? super P, ? extends Optional<? extends M>> resolver = port -> {
            ExternalPortInfo info = port.getInfo();
            if (info != null && info.getModuleName().equals(moduleName)) {
                ValueDescription contents = info.getContents();
                if (contents != null) {
                    try {
                        Object model = contents.resolve(classLoader);
                        if (modelType.isInstance(model)) {
                            return Optionals.of(modelType.cast(model));
                        }
                    } catch (ReflectiveOperationException e) {
                        LOG.warn(MessageFormat.format(
                                "failed to resolve external port: {0} ({1})",
                                port, info.getModuleName()), e);
                    }
                }
            }
            return Optionals.empty();
        };
        return collectModels(ports, resolver);
    }

    /**
     * Returns all operators in the plan, which is a given type.
     * @param <T> the operator type
     * @param plan the plan
     * @param type the operator type
     * @return the operators
     */
    public static <T extends Operator> List<T> collectOperators(Plan plan, Class<T> type) {
        return plan.getElements().stream()
                .flatMap(s -> s.getOperators().stream())
                .filter(type::isInstance)
                .map(type::cast)
                .collect(Collectors.toList());
    }

    /**
     * Returns the sub-plan map of the given operators.
     * @param <T> the operator type
     * @param plan the plan
     * @param operators the operators in the plan
     * @return the sub-plan map for the given operators
     */
    public static <T extends Operator> Map<T, VertexSpec> collectOwners(Plan plan, Collection<T> operators) {
        if (operators.isEmpty()) {
            return Collections.emptyMap();
        }
        Set<T> rest = new HashSet<>(operators);
        Map<T, VertexSpec> results = new LinkedHashMap<>();
        for (SubPlan sub : plan.getElements()) {
            if (rest.isEmpty()) {
                break;
            }
            for (Operator o : sub.getOperators()) {
                if (rest.isEmpty()) {
                    break;
                }
                if (rest.remove(o)) {
                    @SuppressWarnings("unchecked")
                    T found = (T) o;
                    results.put(found, VertexSpec.get(sub));
                }
            }
        }
        Invariants.require(rest.isEmpty());
        return results;
    }

    /**
     * Generates a class and registers it to the current context.
     * @param context the current context
     * @param category the class category
     * @param generator the class generator
     * @return the generated class reference
     */
    public static ClassDescription generate(
            ClassGeneratorContext context,
            String category,
            Function<ClassDescription, ClassData> generator) {
        ClassDescription name = context.getClassName(category);
        return context.addClassFile(generator.apply(name));
    }

    /**
     * Generates a class and registers it to the current context.
     * @param context the current context
     * @param vertex the owner vertex
     * @param category the class category
     * @param generator the class generator
     * @return the generated class reference
     */
    public static ClassDescription generate(
            ClassGeneratorContext context,
            VertexSpec vertex, String category,
            Function<ClassDescription, ClassData> generator) {
        return generate(context, String.format("%s.%s", vertex.getId(), category), generator);
    }

    /**
     * Returns the source input of the given output vertex.
     * @param vertex the output vertex
     * @return the corresponded input
     */
    public static SubPlan.Input getOutputSource(VertexSpec vertex) {
        Invariants.require(vertex.getOperationType() == OperationType.OUTPUT);
        SubPlan sub = vertex.getOrigin();
        List<ExternalOutput> ports = sub.getOperators().stream()
            .filter(o -> o.getOperatorKind() == OperatorKind.OUTPUT)
            .map(ExternalOutput.class::cast)
            .collect(Collectors.toList());
        Invariants.require(ports.size() == 1);
        List<SubPlan.Input> results = Operators.getPredecessors(ports.get(0))
                .stream()
                .map(o -> Invariants.requireNonNull(sub.findInput(o)))
                .collect(Collectors.toList());
        Invariants.require(results.size() == 1);
        Invariants.require(InputSpec.get(results.get(0)).getInputOptions().contains(InputOption.PRIMARY));
        return results.get(0);
    }

    /**
     * Returns whether or not the given output vertex always takes empty data.
     * @param vertex the output vertex
     * @return {@code true} if the given output vertex always takes empty data, otherwise {@code false}
     */
    public static boolean isEmptyOutput(VertexSpec vertex) {
        Invariants.require(vertex.getOperationType() == OperationType.OUTPUT);
        return InputSpec.get(getOutputSource(vertex)).getInputType() == InputType.NO_DATA;
    }

    /**
     * Returns the tag of the given port.
     * @param port the port
     * @return the port tag
     */
    public static String getPortTag(SubPlan.Input port) {
        return String.format("%s.%s",
                VertexSpec.get(port.getOwner()).getId(),
                InputSpec.get(port).getId());
    }

    /**
     * Returns the tag of the given port.
     * @param port the port
     * @return the port tag
     */
    public static String getPortTag(SubPlan.Output port) {
        return String.format("%s.%s",
                VertexSpec.get(port.getOwner()).getId(),
                OutputSpec.get(port).getId());
    }

    /**
     * Registers {@link ResolvedVertexInfo} into the given {@link GraphInfoBuilder}.
     * @param builder the target builder
     * @param owner the vertex owner
     * @param info the target vertex info
     * @param implementation the vertex implementation
     * @return the registered {@link ResolvedVertexInfo}
     */
    public static ResolvedVertexInfo register(
            GraphInfoBuilder builder, Plan owner, ResolvedVertexInfo info, ClassDescription implementation) {
        builder.add(info);
        return info;
    }

    /**
     * Registers {@link ResolvedVertexInfo} into the given {@link GraphInfoBuilder}.
     * @param builder the target builder
     * @param owner the vertex owner
     * @param info the target vertex info
     * @param implementation the vertex implementation
     * @return the registered {@link ResolvedVertexInfo}
     */
    public static ResolvedVertexInfo register(
            GraphInfoBuilder builder, VertexSpec owner, ResolvedVertexInfo info, ClassDescription implementation) {
        SubPlan origin = owner.getOrigin();
        origin.putAttribute(Implementation.class, new Implementation(implementation));
        builder.add(origin, info);
        return info;
    }

    /**
     * Generates an {@link InputAdapter} implementation about the internal output.
     * @param context the current context
     * @param vertex the target vertex
     * @param port the target port
     * @param paths the input paths
     * @return the generated adapter class
     */
    public static ClassDescription generateInternalInput(
            ClassGeneratorContext context,
            VertexSpec vertex, ExternalInput port, Collection<String> paths) {
        return generate(context, vertex, "input.internal", c -> {
            InternalInputAdapterGenerator.Spec spec =
                    new InternalInputAdapterGenerator.Spec(port.getName(), paths, port.getDataType());
            return new InternalInputAdapterGenerator().generate(context, spec, c);
        });
    }

    /**
     * Registers an internal output.
     * @param context the current context
     * @param descriptors the descriptor factory
     * @param target the target DAG builder
     * @param vertex the target vertex
     * @param port the target port
     * @param path the output path
     */
    public static void registerInternalOutput(
            ClassGeneratorContext context, DagDescriptorFactory descriptors,
            GraphInfoBuilder target,
            VertexSpec vertex, ExternalOutput port, String path) {
        ClassDescription vertexClass = generate(context, vertex, "output.internal", c -> { //$NON-NLS-1$
            List<InternalOutputPrepareGenerator.Spec> specs = Collections.singletonList(
                    new InternalOutputPrepareGenerator.Spec(port.getName(), path, port.getDataType()));
            return new InternalOutputPrepareGenerator().generate(context, specs, c);
        });
        SubPlan.Input entry = getOutputSource(vertex);
        ResolvedInputInfo input = new ResolvedInputInfo(
                InternalOutputPrepare.INPUT_NAME,
                new ResolvedEdgeInfo(
                        descriptors.newOneToOneEdge(port.getDataType()),
                        ResolvedEdgeInfo.Movement.ONE_TO_ONE,
                        port.getDataType(),
                        null));
        ResolvedVertexInfo info = new ResolvedVertexInfo(
                vertex.getId(),
                descriptors.newVertex(vertexClass),
                String.valueOf(port),
                Collections.singletonMap(entry, input),
                Collections.emptyMap());
        register(target, vertex, info, vertexClass);
    }
}
