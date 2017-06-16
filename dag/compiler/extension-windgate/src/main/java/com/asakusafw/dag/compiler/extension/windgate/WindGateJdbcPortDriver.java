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
package com.asakusafw.dag.compiler.extension.windgate;

import static com.asakusafw.dag.compiler.flow.DataFlowUtil.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.UnionRecordSerDeSupplierGenerator;
import com.asakusafw.dag.compiler.codegen.ValueSerDeGenerator;
import com.asakusafw.dag.compiler.flow.DagDescriptorFactory;
import com.asakusafw.dag.compiler.flow.ExternalPortDriver;
import com.asakusafw.dag.compiler.flow.ExternalPortDriverProvider;
import com.asakusafw.dag.compiler.jdbc.windgate.WindGateJdbcInputAdapterGenerator;
import com.asakusafw.dag.compiler.jdbc.windgate.WindGateJdbcInputModel;
import com.asakusafw.dag.compiler.jdbc.windgate.WindGateJdbcIoAnalyzer;
import com.asakusafw.dag.compiler.jdbc.windgate.WindGateJdbcModel;
import com.asakusafw.dag.compiler.jdbc.windgate.WindGateJdbcOutputModel;
import com.asakusafw.dag.compiler.jdbc.windgate.WindGateJdbcOutputProcessorGenerator;
import com.asakusafw.dag.compiler.model.build.GraphInfoBuilder;
import com.asakusafw.dag.compiler.model.build.ResolvedEdgeInfo;
import com.asakusafw.dag.compiler.model.build.ResolvedInputInfo;
import com.asakusafw.dag.compiler.model.build.ResolvedVertexInfo;
import com.asakusafw.dag.compiler.model.plan.VertexSpec;
import com.asakusafw.dag.runtime.io.UnionRecord;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcOutputProcessor;
import com.asakusafw.dag.runtime.skeleton.VoidVertexProcessor;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.common.NamePattern;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.ExternalPort;
import com.asakusafw.lang.compiler.planning.Plan;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * An implementation of {@link ExternalPortDriver} for WindGate JDBC (direct mode) ports.
 * @since 0.4.0
 */
public class WindGateJdbcPortDriver implements ExternalPortDriver {

    /**
     * The compiler property key of whether or not the JDBC I/O barrier is enabled.
     */
    public static final String KEY_BARRIER = WindGateJdbcIoAnalyzer.KEY_DIRECT + ".barrier"; //$NON-NLS-1$

    /**
     * The default value of {@link #KEY_BARRIER}.
     */
    public static final boolean DEFAULT_BARRIER = true;

    private static final String ID_BARRIER_PREFIX = "_jdbc-barrier-";

    private static final String ID_OUTPUT_PREFIX = "_jdbc-output-";

    static final Logger LOG = LoggerFactory.getLogger(WindGateJdbcPortDriver.class);

    private final Plan plan;

    private final CompilerOptions options;

    private final ClassGeneratorContext context;

    private final DagDescriptorFactory descriptors;

    private final Map<ExternalInput, WindGateJdbcInputModel> inputModels;

    private final Map<ExternalOutput, WindGateJdbcOutputModel> outputModels;

    private final Map<ExternalInput, VertexSpec> inputOwners;

    private final Map<ExternalOutput, VertexSpec> outputOwners;

    WindGateJdbcPortDriver(ExternalPortDriverProvider.Context context) {
        Arguments.requireNonNull(context);
        this.plan = context.getSourcePlan();
        this.options = context.getOptions();
        this.context = context.getGeneratorContext();
        this.descriptors = context.getDescriptorFactory();

        Predicate<WindGateJdbcModel> filter = buildWindGateJdbcFilter(options);
        ClassLoader classLoader = context.getGeneratorContext().getClassLoader();
        this.inputModels = collectInputs(classLoader, plan, filter);
        this.outputModels = collectOutputs(classLoader, plan, filter);
        this.inputOwners = collectOwners(plan, inputModels.keySet());
        this.outputOwners = collectOwners(plan, outputModels.keySet());
    }

    private static Predicate<WindGateJdbcModel> buildWindGateJdbcFilter(CompilerOptions options) {
        NamePattern pattern = WindGateJdbcIoAnalyzer.getProfileNamePattern(options);
        LOG.debug("WindGate JDBC direct profiles: {}", pattern);
        return model -> pattern.test(model.getProfileName());
    }

    private static Map<ExternalInput, WindGateJdbcInputModel> collectInputs(
            ClassLoader classLoader, Plan plan, Predicate<WindGateJdbcModel> filter) {
        return collectModels(
                plan,
                ExternalInput.class,
                filter,
                p -> WindGateJdbcIoAnalyzer.analyze(classLoader, p.getInfo()));
    }

    private static Map<ExternalOutput, WindGateJdbcOutputModel> collectOutputs(
            ClassLoader classLoader, Plan plan, Predicate<WindGateJdbcModel> filter) {
        return collectModels(
                plan,
                ExternalOutput.class,
                filter,
                p -> WindGateJdbcIoAnalyzer.analyze(classLoader, p.getInfo()));
    }

    private static <TPort extends ExternalPort, TModel> Map<TPort, TModel> collectModels(
            Plan plan,
            Class<TPort> type,
            Predicate<? super TModel> filter,
            Function<TPort, Optional<TModel>> mapper) {
        return collectOperators(plan, type).stream()
            .flatMap(p -> mapper.apply(p)
                    .map(m -> Stream.of(new Tuple<>(p, m)))
                    .orElse(Stream.empty()))
            .filter(t -> filter.test(t.right()))
            .collect(Collectors.toMap(Tuple::left, Tuple::right));
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
        WindGateJdbcInputModel model = Invariants.requireNonNull(inputModels.get(port));
        WindGateJdbcInputAdapterGenerator.Spec spec = new WindGateJdbcInputAdapterGenerator.Spec(
                port.getName(), model);
        return generate(context, vertex, "input.windgate", c -> {
            return WindGateJdbcInputAdapterGenerator.generate(context, spec, c);
        });
    }

    @Override
    public void processOutputs(GraphInfoBuilder target) {
        if (outputModels.isEmpty()) {
            return;
        }
        Map<String, List<Tuple<ExternalOutput, VertexSpec>>> profiles = outputOwners.entrySet().stream()
                .map(Tuple::of)
                .map(t -> new Tuple<>(t.left(), t.right()))
                .collect(Collectors.groupingBy(
                        t -> outputModels.get(t.left()).getProfileName(),
                        Collectors.toList()));
        profiles.forEach((k, v) -> {
            v.sort((a, b) -> a.left().getName().compareTo(b.left().getName()));
            registerOutputs(target, k, Lang.project(v, Tuple::left));
        });
    }

    private void registerOutputs(GraphInfoBuilder builder, String profileName, List<ExternalPort> ports) {
        assert ports.isEmpty() == false;
        ClassDescription serdeSupplier = generate(context, "jdbc.output", c -> {
            List<UnionRecordSerDeSupplierGenerator.Upstream> specs = ports.stream()
                    .sequential()
                    .map(outputOwners::get)
                    .filter(v -> isEmptyOutput(v) == false)
                    .map(v -> getOutputSource(v))
                    .map(downstream -> {
                        TypeDescription dataType = downstream.getOperator().getDataType();
                        ClassDescription element = ValueSerDeGenerator.get(context, dataType);
                        List<String> tags = downstream.getOpposites().stream()
                                .map(p -> getPortTag(p))
                                .collect(Collectors.toList());
                        return new UnionRecordSerDeSupplierGenerator.Upstream(tags, element);
                    })
                    .collect(Collectors.toList());
            return UnionRecordSerDeSupplierGenerator.generate(context,
                    specs, UnionRecordSerDeSupplierGenerator.Downstream.of(null),
                    c);
        });
        ClassDescription proc = generate(context, "jdbc.windgate", c -> {
            List<WindGateJdbcOutputProcessorGenerator.Spec> specs = ports.stream()
                    .map(port -> {
                        VertexSpec vertex = outputOwners.get(port);
                        WindGateJdbcOutputModel model = outputModels.get(port);
                        return new WindGateJdbcOutputProcessorGenerator.Spec(port.getName(), model)
                                .withOutput(isEmptyOutput(vertex) == false);
                    })
                    .collect(Collectors.toList());
            return WindGateJdbcOutputProcessorGenerator.generate(context, specs);
        });
        ResolvedInputInfo input = new ResolvedInputInfo(
                JdbcOutputProcessor.INPUT_NAME,
                new ResolvedEdgeInfo(
                        descriptors.newOneToOneEdge(Descriptions.classOf(UnionRecord.class), serdeSupplier),
                        ResolvedEdgeInfo.Movement.ONE_TO_ONE,
                        Descriptions.classOf(UnionRecord.class),
                        null));
        ResolvedVertexInfo info = new ResolvedVertexInfo(
                getOutputId(profileName),
                descriptors.newVertex(proc),
                String.format("JDBC(%s)", profileName),
                ports.stream()
                    .map(outputOwners::get)
                    .filter(v -> isEmptyOutput(v) == false)
                    .map(v -> getOutputSource(v))
                    .collect(Collectors.toMap(Function.identity(), p -> input)),
                Collections.emptyMap());
        register(builder, plan, info, proc);
    }

    @Override
    public void processPlan(GraphInfoBuilder target) {
        if (inputModels.isEmpty() || outputModels.isEmpty()) {
            return;
        }
        if (isBarrierEnabled() == false) {
            LOG.debug("JDBC barrier I/O barrier is disabled");
            return;
        }
        Map<String, Set<ResolvedVertexInfo>> inputs = inputOwners.entrySet().stream()
                .map(e -> new Tuple<>(e.getKey(), target.get(e.getValue().getOrigin())))
                .filter(t -> t.right() != null)
                .map(t -> new Tuple<>(inputModels.get(t.left()).getProfileName(), t.right()))
                .collect(Collectors.groupingBy(Tuple::left, Collectors.mapping(Tuple::right, Collectors.toSet())));
        Map<String, ResolvedVertexInfo> outputs = outputModels.values().stream()
                .map(WindGateJdbcOutputModel::getProfileName)
                .distinct()
                .map(s -> new Tuple<>(s, target.get(getOutputId(s))))
                .filter(t -> t.right() != null)
                .collect(Collectors.toMap(Tuple::left, Tuple::right));
        Set<String> common = new TreeSet<>();
        common.addAll(inputs.keySet());
        common.retainAll(outputs.keySet());
        common.forEach(profileName -> {
            LOG.debug("insert JDBC I/O barrier: {}", profileName);
            Set<ResolvedVertexInfo> upstreams = Invariants.requireNonNull(inputs.get(profileName));
            ResolvedVertexInfo downstream = Invariants.requireNonNull(outputs.get(profileName));
            ResolvedVertexInfo barrier = new ResolvedVertexInfo(
                    getBarrierId(profileName),
                    descriptors.newVertex(Descriptions.classOf(VoidVertexProcessor.class)),
                    null,
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    upstreams);
            downstream.addImplicitDependency(barrier);
            register(target, plan, barrier, Descriptions.classOf(VoidVertexProcessor.class));
        });
    }

    private boolean isBarrierEnabled() {
        return options.get(KEY_BARRIER, DEFAULT_BARRIER);
    }

    private String getBarrierId(String profileName) {
        return ID_BARRIER_PREFIX + profileName;
    }

    private static String getOutputId(String profileName) {
        return ID_OUTPUT_PREFIX + profileName;
    }
}
