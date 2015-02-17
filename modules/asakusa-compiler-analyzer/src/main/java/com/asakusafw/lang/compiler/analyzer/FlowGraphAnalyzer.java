package com.asakusafw.lang.compiler.analyzer;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.api.Diagnostic;
import com.asakusafw.lang.compiler.api.Diagnostic.Level;
import com.asakusafw.lang.compiler.api.DiagnosticException;
import com.asakusafw.lang.compiler.api.ExternalIoProcessor;
import com.asakusafw.lang.compiler.api.basic.BasicDiagnostic;
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
import com.asakusafw.vocabulary.operator.Extend;
import com.asakusafw.vocabulary.operator.Project;
import com.asakusafw.vocabulary.operator.Restructure;

/**
 * Generates {@link OperatorGraph} from the {@link FlowGraph}.
 * @see FlowGraphVerifier
 */
public final class FlowGraphAnalyzer {

    private static final Map<Class<?>, CoreOperator.CoreOperatorKind> CORE_OPERATOR_KINDS;
    static {
        Map<Class<?>, CoreOperatorKind> map = new HashMap<>();
        map.put(Checkpoint.class, CoreOperator.CoreOperatorKind.CHECKPOINT);
        map.put(Project.class, CoreOperator.CoreOperatorKind.PROJECT);
        map.put(Extend.class, CoreOperator.CoreOperatorKind.EXTEND);
        map.put(Restructure.class, CoreOperator.CoreOperatorKind.RESTRUCTURE);
        CORE_OPERATOR_KINDS = map;
    }

    private final ExternalIoProcessor.Context ioContext;

    private final ExternalIoProcessor ioProcessor;

    /**
     * Creates a new instance.
     * @param context the current external I/O context
     * @param processor the external I/O processor
     */
    public FlowGraphAnalyzer(ExternalIoProcessor.Context context, ExternalIoProcessor processor) {
        this.ioContext = context;
        this.ioProcessor = processor;
    }

    /**
     * Analyzes {@link FlowGraph} and returns an equivalent {@link OperatorGraph}.
     * @param graph the original graph
     * @return the equivalent operator graph
     */
    public OperatorGraph analyze(FlowGraph graph) {
        Context context = new Context();
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
        if (extern != null) {
            if (ioProcessor.isSupported(ioContext, extern.getClass()) == false) {
                context.error(MessageFormat.format(
                        "missing processor of importer description: {0} (name={1})",
                        description.getName(),
                        extern.getClass().getName()));
                return null;
            }
            try {
                info = ioProcessor.resolveInput(ioContext, description.getName(), extern);
            } catch (DiagnosticException e) {
                context.merge(e.getDiagnostics());
                return null;
            }
        }
        return convert(description, ExternalInput.builder(description.getName(), info));
    }

    private Operator convert(Context context, OutputDescription description) {
        ExporterDescription extern = description.getExporterDescription();
        ExternalOutputInfo info = null;
        if (extern != null) {
            if (ioProcessor.isSupported(ioContext, extern.getClass()) == false) {
                context.error(MessageFormat.format(
                        "missing processor of exporter description: {0} (name={1})",
                        description.getName(),
                        extern.getClass().getName()));
                return null;
            }
            try {
                info = ioProcessor.resolveOutput(ioContext, description.getName(), extern);
            } catch (DiagnosticException e) {
                context.merge(e.getDiagnostics());
                return null;
            }
        }
        return convert(description, ExternalOutput.builder(description.getName(), info));
    }

    private Operator convert(Context context, FlowPartDescription description) {
        ClassDescription declaring = Descriptions.classOf(description.getFlowGraph().getDescription());
        OperatorGraph inner;
        try {
            inner = analyze(description.getFlowGraph());
        } catch (DiagnosticException e) {
            context.merge(e.getDiagnostics());
            return null;
        }
        return convert(description, FlowOperator.builder(declaring, inner));
    }

    private Operator convert(Context context, OperatorDescription description) {
        OperatorDescription.Declaration declaration = description.getDeclaration();
        CoreOperator.CoreOperatorKind core = CORE_OPERATOR_KINDS.get(declaration.getAnnotationType());
        if (core != null) {
            return convert(description, CoreOperator.builder(core));
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
        Annotation annotation = method.getAnnotation(declaration.getAnnotationType());
        if (annotation == null) {
            context.error(MessageFormat.format(
                    "failed to resolve operator annotation: [{0}]{1}#{2}()",
                    declaration.getAnnotationType().getSimpleName(),
                    declaration.getDeclaring().getName(),
                    declaration.getName()));
            return null;
        }
        return convert(description, UserOperator.builder(
                AnnotationDescription.of(annotation),
                MethodDescription.of(method),
                Descriptions.classOf(declaration.getImplementing())));
    }

    private Operator convert(PseudElementDescription description) {
        if (isCheckpoint(description)) {
            return convert(description, CoreOperator.builder(CoreOperator.CoreOperatorKind.CHECKPOINT));
        }
        // non-reifiable operators
        return null;
    }

    private static Operator convert(FlowElementDescription description, Operator.AbstractBuilder<?, ?> builder) {
        for (FlowElementPortDescription port : description.getInputPorts()) {
            builder.input(port.getName(), typeOf(port.getDataType()), convert(port.getShuffleKey()));
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
        return builder.build();
    }

    private static ValueDescription convert(java.lang.reflect.Type type, Object value) {
        if (value == null) {
            // FIXME for nulls
            return new ImmediateDescription(typeOf(type), null);
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

        Context() {
            return;
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
