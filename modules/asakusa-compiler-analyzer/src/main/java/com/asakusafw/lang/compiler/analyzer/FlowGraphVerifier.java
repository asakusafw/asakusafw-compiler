package com.asakusafw.lang.compiler.analyzer;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.asakusafw.lang.compiler.api.Diagnostic;
import com.asakusafw.lang.compiler.api.DiagnosticException;
import com.asakusafw.lang.compiler.api.basic.BasicDiagnostic;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;
import com.asakusafw.vocabulary.flow.graph.Connectivity;
import com.asakusafw.vocabulary.flow.graph.FlowElement;
import com.asakusafw.vocabulary.flow.graph.FlowElementInput;
import com.asakusafw.vocabulary.flow.graph.FlowElementKind;
import com.asakusafw.vocabulary.flow.graph.FlowElementOutput;
import com.asakusafw.vocabulary.flow.graph.FlowGraph;
import com.asakusafw.vocabulary.flow.graph.FlowPartDescription;

/**
 * Verifies structure of {@link FlowGraph}.
 */
public final class FlowGraphVerifier {

    private final Context context;

    private FlowGraphVerifier(Context context) {
        this.context = context;
    }

    /**
     * Verifies the target flow graph.
     * @param graph the target graph
     */
    public static void verify(FlowGraph graph) {
        Context context = new Context();
        new FlowGraphVerifier(context).verifyGraph(graph);
        context.done();
    }

    private void verifyGraph(FlowGraph graph) {
        context.push(graph);
        try {
            Graph<FlowElement> dependencies = FlowElementUtil.toDependencyGraph(graph);
            Set<Set<FlowElement>> circuits = Graphs.findCircuit(dependencies);
            if (circuits.isEmpty() == false) {
                context.error(MessageFormat.format(
                        "flow must be acyclic: {0}",
                        circuits));
            }
            for (FlowElement element : dependencies.getNodeSet()) {
                verifyElement(element);
                if (element.getDescription().getKind() == FlowElementKind.FLOW_COMPONENT) {
                    FlowPartDescription part = (FlowPartDescription) element.getDescription();
                    verifyGraph(part.getFlowGraph());
                }
            }
        } finally {
            context.pop();
        }
    }

    private void verifyElement(FlowElement element) {
        Connectivity connectivity = element.getAttribute(Connectivity.class);
        if (connectivity == null) {
            connectivity = Connectivity.getDefault();
        }
        for (FlowElementInput port : element.getInputPorts()) {
            if (port.getConnected().isEmpty() == false) {
                continue;
            }
            context.error(MessageFormat.format(
                    "input port \"{1}\" of \"{0}\" is not connected from the other output port nor jobflow input",
                    element.getDescription(),
                    port.getDescription().getName()));
        }
        if (connectivity == Connectivity.MANDATORY) {
            for (FlowElementOutput port : element.getOutputPorts()) {
                if (port.getConnected().isEmpty() == false) {
                    continue;
                }
                context.error(MessageFormat.format(
                        "output port \"{1}\" of \"{0}\" is not connected to the other input port nor jobflow output",
                        element.getDescription(),
                        port.getDescription().getName()));
            }
        }
    }

    private static final class Context {

        private final Deque<FlowGraph> stack = new LinkedList<>();

        private final List<Diagnostic> errors = new ArrayList<>();

        Context() {
            return;
        }

        void done() {
            if (errors.isEmpty() == false) {
                throw new DiagnosticException(errors);
            }
        }

        void push(FlowGraph flow) {
            stack.push(flow);
        }

        FlowGraph pop() {
            return stack.pop();
        }

        FlowGraph peek() {
            return stack.peek();
        }

        void error(String message) {
            assert stack.isEmpty() == false;
            errors.add(new BasicDiagnostic(Diagnostic.Level.ERROR, MessageFormat.format(
                    "{0} ({1})",
                    message,
                    peek().getDescription().getName())));
        }
    }
}
