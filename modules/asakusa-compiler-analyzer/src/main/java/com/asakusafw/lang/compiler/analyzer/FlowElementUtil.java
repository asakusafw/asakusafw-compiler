package com.asakusafw.lang.compiler.analyzer;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;
import com.asakusafw.vocabulary.flow.graph.FlowElement;
import com.asakusafw.vocabulary.flow.graph.FlowElementInput;
import com.asakusafw.vocabulary.flow.graph.FlowElementOutput;
import com.asakusafw.vocabulary.flow.graph.FlowGraph;
import com.asakusafw.vocabulary.flow.graph.FlowIn;
import com.asakusafw.vocabulary.flow.graph.FlowOut;

/**
 * Utilities of {@link FlowElementUtil}.
 */
final class FlowElementUtil {

    private FlowElementUtil() {
        return;
    }

    static Graph<FlowElement> toDependencyGraph(FlowGraph graph) {
        Set<FlowElement> elements = collect(graph);
        Graph<FlowElement> dependencies = Graphs.newInstance();
        for (FlowElement source : elements) {
            dependencies.addNode(source);
            for (FlowElementInput port : source.getInputPorts()) {
                for (FlowElementOutput opposite : port.getOpposites()) {
                    dependencies.addEdge(source, opposite.getOwner());
                }
            }
        }
        return dependencies;
    }

    static Set<FlowElement> collect(FlowGraph graph) {
        LinkedList<FlowElement> work = new LinkedList<>();
        for (FlowIn<?> port : graph.getFlowInputs()) {
            work.add(port.getFlowElement());
        }
        for (FlowOut<?> port : graph.getFlowOutputs()) {
            work.add(port.getFlowElement());
        }
        Set<FlowElement> results = new HashSet<>();
        while (work.isEmpty() == false) {
            FlowElement element = work.removeFirst();
            if (results.contains(element) == false) {
                results.add(element);
            }
            for (FlowElementInput port : element.getInputPorts()) {
                for (FlowElementOutput opposite : port.getOpposites()) {
                    FlowElement neighbor = opposite.getOwner();
                    if (results.contains(neighbor) == false) {
                        work.add(neighbor);
                    }
                }
            }
            for (FlowElementOutput port : element.getOutputPorts()) {
                for (FlowElementInput opposite : port.getOpposites()) {
                    FlowElement neighbor = opposite.getOwner();
                    if (results.contains(neighbor) == false) {
                        work.add(neighbor);
                    }
                }
            }
        }
        return results;
    }
}
