/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.planning.util;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.asakusafw.lang.compiler.common.ComplexAttribute;
import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * Represents statistics for dependency graphs.
 */
public class GraphStatistics implements ComplexAttribute {

    /**
     * Represents unknown values.
     */
    public static final int UNDEFINED = -1;

    private final int numberOfVertices;

    private final int numberOfEdges;

    private final int criticalPathLength;

    /**
     * Creates a new instance.
     * @param numberOfVertices the number of vertices
     * @param numberOfEdges the number of edges
     * @param criticalPathLength the critical path length
     */
    public GraphStatistics(int numberOfVertices, int numberOfEdges, int criticalPathLength) {
        this.numberOfVertices = numberOfVertices;
        this.numberOfEdges = numberOfEdges;
        this.criticalPathLength = criticalPathLength;
    }

    /**
     * Creates a new instance for the target graph.
     * @param graph the target graph
     * @return the created instance
     */
    public static GraphStatistics of(Graph<?> graph) {
        int numberOfVertices = getNumberOfVertices(graph);
        int numberOfEdges = getNumberOfEdges(graph);
        int criticalPathLength = getCriticalPathLength(graph);
        return new GraphStatistics(numberOfVertices, numberOfEdges, criticalPathLength);
    }

    /**
     * Returns the number of vertices.
     * @return the number of vertices
     */
    public int getNumberOfVertices() {
        return numberOfVertices;
    }

    /**
     * Returns the number of edges.
     * @return the number of edges
     */
    public int getNumberOfEdges() {
        return numberOfEdges;
    }

    /**
     * Returns the critical path length.
     * @return the critical path length, or {@link #UNDEFINED} if it is not defined
     */
    public int getCriticalPathLength() {
        return criticalPathLength;
    }

    /**
     * Returns the number of vertices in the target graph.
     * @param graph the target graph
     * @return the number of vertices
     */
    public static int getNumberOfVertices(Graph<?> graph) {
        return graph.getNodeSet().size();
    }

    /**
     * Returns the number of edges in the target graph.
     * @param graph the target graph
     * @return the number of edges
     */
    public static int getNumberOfEdges(Graph<?> graph) {
        int total = 0;
        for (Graph.Vertex<?> vertex : graph) {
            total += vertex.getConnected().size();
        }
        return total;
    }

    /**
     * Returns the critical path length of the target graph.
     * @param graph the target graph
     * @return the critical path length, or {@link #UNDEFINED} if the target graph is cyclic
     */
    public static int getCriticalPathLength(Graph<?> graph) {
        if (Graphs.findCircuit(graph).isEmpty() == false) {
            return UNDEFINED;
        }
        List<Object> elements = Graphs.sortPostOrder(graph);
        Map<Object, Integer> lengthes = new HashMap<>();
        for (Object element : elements) {
            int max = 0;
            for (Object blocker : graph.getConnected(element)) {
                Integer value = lengthes.get(blocker);
                assert value != null;
                max = Math.max(max, value);
            }
            lengthes.put(element, max + 1);
        }
        int max = 0;
        for (int value : lengthes.values()) {
            max = Math.max(max, value);
        }
        return max;
    }

    @Override
    public Map<String, ?> toMap() {
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("vertices", getNumberOfVertices()); //$NON-NLS-1$
        results.put("edges", getNumberOfEdges()); //$NON-NLS-1$
        if (criticalPathLength != UNDEFINED) {
            results.put("critical", getCriticalPathLength()); //$NON-NLS-1$
            results.put("average-width", String.format( //$NON-NLS-1$
                    "%.3f", //$NON-NLS-1$
                    criticalPathLength == 0 ? 0d : (double) numberOfVertices / criticalPathLength));
        }
        return results;
    }

    @Override
    public String toString() {
        return toMap().toString();
    }
}
