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
package com.asakusafw.lang.compiler.analyzer;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.analyzer.OperatorAttributeAnalyzer.AttributeMap;
import com.asakusafw.lang.compiler.analyzer.model.ConstructorParameter;
import com.asakusafw.lang.compiler.analyzer.model.OperatorSource;
import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic.Level;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.AnnotationDescription;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.ImmediateDescription;
import com.asakusafw.lang.compiler.model.description.MethodDescription;
import com.asakusafw.lang.compiler.model.description.ReifiableTypeDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.FlowOperator;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;
import com.asakusafw.vocabulary.flow.Export;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.Import;
import com.asakusafw.vocabulary.flow.graph.FlowBoundary;
import com.asakusafw.vocabulary.flow.graph.FlowElement;
import com.asakusafw.vocabulary.flow.graph.FlowElementDescription;
import com.asakusafw.vocabulary.flow.graph.FlowElementInput;
import com.asakusafw.vocabulary.flow.graph.FlowElementKind;
import com.asakusafw.vocabulary.flow.graph.FlowElementOutput;
import com.asakusafw.vocabulary.flow.graph.FlowElementPortDescription;
import com.asakusafw.vocabulary.flow.graph.FlowGraph;
import com.asakusafw.vocabulary.flow.graph.FlowPartDescription;
import com.asakusafw.vocabulary.flow.graph.InputDescription;
import com.asakusafw.vocabulary.flow.graph.ObservationCount;
import com.asakusafw.vocabulary.flow.graph.OperatorDescription;
import com.asakusafw.vocabulary.flow.graph.OutputDescription;
import com.asakusafw.vocabulary.flow.graph.ShuffleKey;
import com.asakusafw.vocabulary.flow.util.PseudElementDescription;
import com.asakusafw.vocabulary.operator.Checkpoint;
import com.asakusafw.vocabulary.operator.CoGroup;
import com.asakusafw.vocabulary.operator.Extend;
import com.asakusafw.vocabulary.operator.GroupSort;
import com.asakusafw.vocabulary.operator.Project;
import com.asakusafw.vocabulary.operator.Restructure;

/**
 * Generates {@link OperatorGraph} from the {@link FlowGraph}.
 * @see FlowGraphVerifier
 * @since 0.1.0
 * @version 0.3.0
 */
public final class FlowGraphAnalyzer {

    static final Logger LOG = LoggerFactory.getLogger(FlowGraphAnalyzer.class);

    private static final Map<Class<?>, CoreOperator.CoreOperatorKind> CORE_OPERATOR_KINDS;
    static {
        Map<Class<?>, CoreOperatorKind> map = new HashMap<>();
        map.put(Checkpoint.class, CoreOperator.CoreOperatorKind.CHECKPOINT);
        map.put(Project.class, CoreOperator.CoreOperatorKind.PROJECT);
        map.put(Extend.class, CoreOperator.CoreOperatorKind.EXTEND);
        map.put(Restructure.class, CoreOperator.CoreOperatorKind.RESTRUCTURE);
        CORE_OPERATOR_KINDS = map;
    }

    private static final Map<Class<?>, Collection<Class<? extends Annotation>>> OPERATOR_ANNOTATION_ALIASES;
    static {
        Map<Class<?>, Collection<Class<? extends Annotation>>> map = new HashMap<>();
        map.put(CoGroup.class, Arrays.asList(GroupSort.class));
        OPERATOR_ANNOTATION_ALIASES = map;
    }

    private final ExternalPortAnalyzer ioAnalyzer;

    private final OperatorAttributeAnalyzer attributeAnalyzer;

    /**
     * Creates a new instance.
     * @param ioAnalyzer the external I/O analyzer
     */
    public FlowGraphAnalyzer(ExternalPortAnalyzer ioAnalyzer) {
        this(ioAnalyzer, OperatorAttributeAnalyzer.NULL);
    }

    /**
     * Creates a new instance.
     * @param ioAnalyzer the external I/O analyzer
     * @param attributeAnalyzer the attribute analyzer
     * @since 0.3.0
     */
    public FlowGraphAnalyzer(ExternalPortAnalyzer ioAnalyzer, OperatorAttributeAnalyzer attributeAnalyzer) {
        Objects.requireNonNull(ioAnalyzer);
        Objects.requireNonNull(attributeAnalyzer);
        this.ioAnalyzer = ioAnalyzer;
        this.attributeAnalyzer = attributeAnalyzer;
    }

    /**
     * Analyzes {@link FlowGraph} and returns an equivalent {@link OperatorGraph}.
     * @param graph the original graph
     * @return the equivalent operator graph
     */
    public OperatorGraph analyze(FlowGraph graph) {
        Context root = new Context(null, graph);
        OperatorGraph result = analyze0(root, graph);
        root.validate(ioAnalyzer);
        return result;
    }

    private OperatorGraph analyze0(Context parent, FlowGraph graph) {
        LOG.debug("analyzing flow graph: {}", graph.getDescription().getName()); //$NON-NLS-1$
        Context context = new Context(parent, graph);
        Graph<FlowElement> dependencies = FlowElementUtil.toDependencyGraph(graph);
        Set<Set<FlowElement>> circuits = Graphs.findCircuit(dependencies);
        if (circuits.isEmpty() == false) {
            context.error(MessageFormat.format(
                    "flow \"{0}\" must be acyclic: {1}",
                    graph.getDescription().getName(),
                    circuits));
        }
        for (FlowElement source : Graphs.sortPostOrder(dependencies)) {
            Operator target = convert(context, source.getDescription());
            if (target != null) {
                context.register(source, target);
            }
        }
        return context.done();
    }

    private Operator convert(Context context, FlowElementDescription description) {
        switch (description.getKind()) {
        case INPUT:
            return convert(context, (InputDescription) description);
        case OUTPUT:
            return convert(context, (OutputDescription) description);
        case FLOW_COMPONENT:
            return convert(context, (FlowPartDescription) description);
        case OPERATOR:
            return convert(context, (OperatorDescription) description);
        case PSEUD:
            return convert((PseudElementDescription) description);
        default:
            throw new AssertionError(description);
        }
    }

    private Operator convert(Context context, InputDescription description) {
        ImporterDescription extern = description.getImporterDescription();
        ExternalInputInfo info = null;
        OperatorSource source = null;
        if (extern != null) {
            try {
                info = ioAnalyzer.analyze(description.getName(), extern);
            } catch (DiagnosticException e) {
                context.merge(e.getDiagnostics());
                return null;
            }
            context.registerExternalInput(description.getName(), info);
            source = context.findInputSource(description.getName());
        }
        return convert(description, source, ExternalInput.builder(description.getName(), info));
    }

    private Operator convert(Context context, OutputDescription description) {
        ExporterDescription extern = description.getExporterDescription();
        ExternalOutputInfo info = null;
        OperatorSource source = null;
        if (extern != null) {
            try {
                info = ioAnalyzer.analyze(description.getName(), extern);
            } catch (DiagnosticException e) {
                context.merge(e.getDiagnostics());
                return null;
            }
            context.registerExternalOutput(description.getName(), info);
            source = context.findOutputSource(description.getName());
        }
        Set<OperatorConstraint> constraints = EnumSet.noneOf(OperatorConstraint.class);
        if (info != null && info.isGenerator()) {
            constraints.add(OperatorConstraint.GENERATOR);
        }
        return convert(description, source, ExternalOutput.builder(description.getName(), info)
                .constraint(constraints));
    }

    private Operator convert(Context context, FlowPartDescription description) {
        Class<? extends FlowDescription> flowClass = description.getFlowGraph().getDescription();
        ClassDescription declaring = Descriptions.classOf(flowClass);
        OperatorGraph inner;
        try {
            inner = analyze0(context, description.getFlowGraph());
        } catch (DiagnosticException e) {
            context.merge(e.getDiagnostics());
            return null;
        }
        OperatorSource source = new OperatorSource(Operator.OperatorKind.FLOW, flowClass);
        return convert(description, source, FlowOperator.builder(declaring, inner));
    }

    private Operator convert(Context context, OperatorDescription description) {
        OperatorDescription.Declaration declaration = description.getDeclaration();
        CoreOperator.CoreOperatorKind core = CORE_OPERATOR_KINDS.get(declaration.getAnnotationType());
        if (core != null) {
            return convert(description, null, CoreOperator.builder(core));
        }
        Method method = declaration.toMethod();
        if (method == null) {
            context.error(MessageFormat.format(
                    "failed to resolve operator method: [{0}]{1}#{2}()",
                    declaration.getAnnotationType().getSimpleName(),
                    declaration.getDeclaring().getName(),
                    declaration.getName()));
            return null;
        }
        Annotation annotation = getOperatorAnnotation(method, declaration.getAnnotationType());
        if (annotation == null) {
            context.error(MessageFormat.format(
                    "failed to resolve operator annotation: [{0}]{1}#{2}()",
                    declaration.getAnnotationType().getSimpleName(),
                    declaration.getDeclaring().getName(),
                    declaration.getName()));
            return null;
        }
        OperatorSource source = getOperatorSource(method);
        return convert(description, source, UserOperator.builder(
                AnnotationDescription.of(annotation),
                MethodDescription.of(method),
                Descriptions.classOf(declaration.getImplementing())));
    }

    private OperatorSource getOperatorSource(Method method) {
        return new OperatorSource(Operator.OperatorKind.USER, method);
    }

    private Annotation getOperatorAnnotation(Method method, Class<? extends Annotation> annotationType) {
        Annotation annotation = method.getAnnotation(annotationType);
        if (annotation != null) {
            return annotation;
        }
        Collection<Class<? extends Annotation>> aliases = OPERATOR_ANNOTATION_ALIASES.get(annotationType);
        if (aliases != null) {
            for (Class<? extends Annotation> alias : aliases) {
                Annotation origin = method.getAnnotation(alias);
                if (origin != null) {
                    return origin;
                }
            }
        }
        return null;
    }

    private Operator convert(PseudElementDescription description) {
        if (isCheckpoint(description)) {
            return convert(description, null, CoreOperator.builder(CoreOperator.CoreOperatorKind.CHECKPOINT));
        }
        // non-reifiable operators
        return null;
    }

    private Operator convert(
            FlowElementDescription description,
            OperatorSource source,
            Operator.AbstractBuilder<?, ?> builder) {
        for (FlowElementPortDescription port : description.getInputPorts()) {
            builder.input(port.getName(), typeOf(port.getDataType()), c -> c
                    .group(convert(port.getShuffleKey())));
        }
        for (FlowElementPortDescription port : description.getOutputPorts()) {
            builder.output(port.getName(), typeOf(port.getDataType()));
        }
        if (description instanceof OperatorDescription) {
            for (OperatorDescription.Parameter param : ((OperatorDescription) description).getParameters()) {
                builder.argument(param.getName(), convert(param.getType(), param.getValue()));
            }
        } else if (description instanceof FlowPartDescription) {
            for (FlowPartDescription.Parameter param : ((FlowPartDescription) description).getParameters()) {
                builder.argument(param.getName(), convert(param.getType(), param.getValue()));
            }
        }
        builder.constraint(convert(description.getAttribute(ObservationCount.class)));
        if (source != null) {
            AttributeMap map = attributeAnalyzer.analyze(source);
            map.mergeTo(builder);
        }
        return builder.build();
    }

    private static ValueDescription convert(java.lang.reflect.Type type, Object value) {
        if (value == null) {
            return ImmediateDescription.nullOf(typeOf(type));
        } else {
            return Descriptions.valueOf(value);
        }
    }

    private static Group convert(ShuffleKey shuffleKey) {
        if (shuffleKey == null) {
            return null;
        }
        List<PropertyName> grouping = new ArrayList<>();
        for (String name : shuffleKey.getGroupProperties()) {
            grouping.add(PropertyName.of(name));
        }
        List<Group.Ordering> ordering = new ArrayList<>();
        for (ShuffleKey.Order order : shuffleKey.getOrderings()) {
            PropertyName name = PropertyName.of(order.getProperty());
            Group.Direction direction;
            switch (order.getDirection()) {
            case ASC:
                direction = Group.Direction.ASCENDANT;
                break;
            case DESC:
                direction = Group.Direction.DESCENDANT;
                break;
            default:
                throw new AssertionError(order.getDirection());
            }
            ordering.add(new Group.Ordering(name, direction));
        }
        return new Group(grouping, ordering);
    }

    private static Collection<OperatorConstraint> convert(ObservationCount constraint) {
        if (constraint == null) {
            return Collections.emptySet();
        }
        switch (constraint) {
        case DONT_CARE:
            return Collections.emptySet();
        case AT_LEAST_ONCE:
            return Collections.singleton(OperatorConstraint.AT_LEAST_ONCE);
        case AT_MOST_ONCE:
            return Collections.singleton(OperatorConstraint.AT_MOST_ONCE);
        case EXACTLY_ONCE:
            return Arrays.asList(OperatorConstraint.AT_LEAST_ONCE, OperatorConstraint.AT_MOST_ONCE);
        default:
            throw new AssertionError(constraint);
        }
    }

    static boolean isCheckpoint(FlowElementDescription description) {
        return description.getAttribute(FlowBoundary.class) == FlowBoundary.STAGE;
    }

    private static ReifiableTypeDescription typeOf(java.lang.reflect.Type type) {
        if (type instanceof Class<?>) {
            return Descriptions.typeOf((Class<?>) type);
        }
        throw new DiagnosticException(Level.ERROR, MessageFormat.format(
                "failed to resolve type (must be a Class instance): {0}",
                type));
    }

    private static final class Context {

        private final OperatorGraph operators = new OperatorGraph();

        private final List<Diagnostic> errors = new ArrayList<>();

        private final Map<FlowElementOutput, OperatorOutput> upstreams = new HashMap<>();

        private final Map<String, ExternalInputInfo> inputs;

        private final Map<String, ExternalOutputInfo> outputs;

        private final Map<String, OperatorSource> inputOrigins;

        private final Map<String, OperatorSource> outputOrigins;

        Context(Context parent, FlowGraph graph) {
            if (parent == null) {
                this.inputs = new LinkedHashMap<>();
                this.outputs = new LinkedHashMap<>();
            } else {
                this.inputs = parent.inputs;
                this.outputs = parent.outputs;
            }
            this.inputOrigins = new HashMap<>();
            this.outputOrigins = new HashMap<>();
            collectOrigins(graph.getDescription(), inputOrigins, outputOrigins);
        }

        private static void collectOrigins(
                Class<? extends FlowDescription> description,
                Map<String, OperatorSource> inputs,
                Map<String, OperatorSource> outputs) {
            Constructor<?> constructor = findConstructor(description);
            if (constructor == null) {
                return;
            }
            Annotation[][] parameterAnnotations = constructor.getParameterAnnotations();
            for (int i = 0; i < parameterAnnotations.length; i++) {
                Annotation[] annotations = parameterAnnotations[i];
                for (Annotation a : annotations) {
                    Class<? extends Annotation> type = a.annotationType();
                    if (type == Import.class) {
                        String name = ((Import) a).name();
                        ConstructorParameter origin = new ConstructorParameter(constructor, i);
                        inputs.put(name, new OperatorSource(OperatorKind.INPUT, origin));
                    } else if (type == Export.class) {
                        String name = ((Export) a).name();
                        ConstructorParameter origin = new ConstructorParameter(constructor, i);
                        outputs.put(name, new OperatorSource(OperatorKind.OUTPUT, origin));
                    }
                }
            }
        }

        private static Constructor<?> findConstructor(Class<? extends FlowDescription> description) {
            for (Constructor<?> candidate : description.getConstructors()) {
                if (Modifier.isPublic(candidate.getModifiers()) == false) {
                    continue;
                }
                if (candidate.getParameterTypes().length == 0) {
                    continue;
                }
                return candidate;
            }
            return null;
        }

        OperatorSource findInputSource(String name) {
            return inputOrigins.get(name);
        }

        OperatorSource findOutputSource(String name) {
            return outputOrigins.get(name);
        }

        public void validate(ExternalPortAnalyzer analyzer) {
            analyzer.validate(inputs, outputs);
        }

        OperatorGraph done() {
            if (errors.isEmpty() == false) {
                throw new DiagnosticException(errors);
            }
            return operators;
        }

        void error(String message) {
            errors.add(new BasicDiagnostic(Diagnostic.Level.ERROR, message));
        }

        void merge(List<Diagnostic> diagnostics) {
            errors.addAll(diagnostics);
        }

        void registerExternalInput(String name, ExternalInputInfo info) {
            if (inputs.containsKey(name)) {
                error(MessageFormat.format(
                        "conflict external input: name={0}, description={1}",
                        name,
                        info.getDescriptionClass().getClassName()));
                return;
            }
            inputs.put(name, info);
        }

        void registerExternalOutput(String name, ExternalOutputInfo info) {
            if (outputs.containsKey(name)) {
                error(MessageFormat.format(
                        "conflict external output: name={0}, description={1}",
                        name,
                        info.getDescriptionClass().getClassName()));
                return;
            }
            outputs.put(name, info);
        }

        void register(FlowElement source, Operator target) {
            if (errors.isEmpty() == false) {
                return;
            }
            operators.add(target);
            restoreConnections(source, target);
            registerUpstreams(source, target);
        }

        private void restoreConnections(FlowElement source, Operator target) {
            List<FlowElementInput> sourcePorts = source.getInputPorts();
            List<OperatorInput> targetPorts = target.getInputs();
            assert sourcePorts.size() == targetPorts.size();
            for (int i = 0, n = sourcePorts.size(); i < n; i++) {
                restoreConnections(sourcePorts.get(i), targetPorts.get(i));
            }
        }

        private void restoreConnections(FlowElementInput source, OperatorInput target) {
            for (FlowElementOutput upstream : source.getOpposites()) {
                FlowElement upstreamOperator = upstream.getOwner();
                if (isPseudo(upstreamOperator)) {
                    for (FlowElementInput origin : upstreamOperator.getInputPorts()) {
                        restoreConnections(origin, target);
                    }
                } else {
                    OperatorOutput resolved = upstreams.get(upstream);
                    assert resolved != null;
                    resolved.connect(target);
                }
            }
        }

        private boolean isPseudo(FlowElement element) {
            if (element.getDescription().getKind() != FlowElementKind.PSEUD) {
                return false;
            }
            if (isCheckpoint(element.getDescription())) {
                return false;
            }
            return true;
        }

        private void registerUpstreams(FlowElement source, Operator target) {
            List<FlowElementOutput> sourcePorts = source.getOutputPorts();
            List<OperatorOutput> targetPorts = target.getOutputs();
            assert sourcePorts.size() == targetPorts.size();
            for (int i = 0, n = sourcePorts.size(); i < n; i++) {
                registerUpstreams(sourcePorts.get(i), targetPorts.get(i));
            }
        }

        private void registerUpstreams(FlowElementOutput source, OperatorOutput target) {
            assert upstreams.containsKey(source) == false : source;
            upstreams.put(source, target);
        }
    }
}
