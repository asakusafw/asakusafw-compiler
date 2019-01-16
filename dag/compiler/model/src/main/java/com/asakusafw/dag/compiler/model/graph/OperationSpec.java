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
package com.asakusafw.dag.compiler.model.graph;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * Represents information of operations.
 * @since 0.4.0
 */
public class OperationSpec {

    private static final String ID_PREFIX = "element"; //$NON-NLS-1$

    private final InputNode input;

    private final Graph<VertexElement> dependencies;

    private final Map<VertexElement, String> idMap;

    /**
     * Creates a new instance.
     * @param input the main input
     */
    public OperationSpec(InputNode input) {
        Arguments.requireNonNull(input);
        this.input = input;
        this.dependencies = toGraph(input);
        this.idMap = Collections.unmodifiableMap(collectIds(dependencies));
    }

    private static Graph<VertexElement> toGraph(VertexElement root) {
        Queue<VertexElement> work = new LinkedList<>();
        work.add(root);
        Set<VertexElement> done = new HashSet<>();
        Graph<VertexElement> results = Graphs.newInstance();
        while (work.isEmpty() == false) {
            VertexElement next = work.remove();
            if (done.contains(next)) {
                continue;
            }
            results.addEdges(next, next.getDependencies());
            work.addAll(next.getDependencies());
            done.add(next);
        }
        return results;
    }

    private static Map<VertexElement, String> collectIds(Graph<VertexElement> graph) {
        Map<VertexElement, String> results = new HashMap<>();
        List<VertexElement> sorted = Graphs.sortPostOrder(graph);
        for (int i = sorted.size() - 1; i >= 0; i--) {
            VertexElement element = sorted.get(i);
            results.put(element, ID_PREFIX + i);
        }
        return results;
    }

    /**
     * Returns the input.
     * @return the input
     */
    public InputNode getInput() {
        return input;
    }

    /**
     * Returns the sorted elements.
     * @return the sorted elements
     */
    public List<VertexElement> getSorted() {
        return Graphs.sortPostOrder(dependencies);
    }

    /**
     * Returns the ID for the target element.
     * @param element the target element
     * @return the element ID
     */
    public String getId(VertexElement element) {
        Arguments.requireNonNull(element);
        Invariants.require(idMap.containsKey(element));
        return idMap.get(element);
    }
}
