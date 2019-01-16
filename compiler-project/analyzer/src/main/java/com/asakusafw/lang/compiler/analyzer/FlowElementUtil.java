/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
