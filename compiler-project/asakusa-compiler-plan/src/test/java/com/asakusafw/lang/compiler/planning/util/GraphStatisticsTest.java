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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.utils.graph.Graph;
import com.asakusafw.utils.graph.Graphs;

/**
 * Test for {@link GraphStatistics}.
 */
public class GraphStatisticsTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        Graph<Integer> graph = Graphs.newInstance();
        path(graph, 0);

        GraphStatistics stats = GraphStatistics.of(graph);
        assertThat(stats.toString(), stats.getNumberOfVertices(), is(1));
        assertThat(stats.toString(), stats.getNumberOfEdges(), is(0));
        assertThat(stats.toString(), stats.getCriticalPathLength(), is(1));
    }

    /**
     * w/o vertices.
     */
    @Test
    public void no_vertices() {
        Graph<Integer> graph = Graphs.newInstance();
        GraphStatistics stats = GraphStatistics.of(graph);
        assertThat(stats.toString(), stats.getNumberOfVertices(), is(0));
        assertThat(stats.toString(), stats.getNumberOfEdges(), is(0));
        assertThat(stats.toString(), stats.getCriticalPathLength(), is(0));
    }

    /**
     * w/ path.
     */
    @Test
    public void path() {
        Graph<Integer> graph = Graphs.newInstance();
        path(graph, 0, 1, 2, 3);

        GraphStatistics stats = GraphStatistics.of(graph);
        assertThat(stats.toString(), stats.getNumberOfVertices(), is(4));
        assertThat(stats.toString(), stats.getNumberOfEdges(), is(3));
        assertThat(stats.toString(), stats.getCriticalPathLength(), is(4));
    }

    /**
     * w/ vertices.
     */
    @Test
    public void vertices() {
        Graph<Integer> graph = Graphs.newInstance();
        path(graph, 0);
        path(graph, 1);
        path(graph, 2);
        path(graph, 3);

        GraphStatistics stats = GraphStatistics.of(graph);
        assertThat(stats.toString(), stats.getNumberOfVertices(), is(4));
        assertThat(stats.toString(), stats.getNumberOfEdges(), is(0));
        assertThat(stats.toString(), stats.getCriticalPathLength(), is(1));
    }

    /**
     * w/ closed path.
     */
    @Test
    public void path_closed() {
        Graph<Integer> graph = Graphs.newInstance();
        path(graph, 0, 1, 2, 3, 4);
        path(graph, 3, 1);

        GraphStatistics stats = GraphStatistics.of(graph);
        assertThat(stats.toString(), stats.getNumberOfVertices(), is(5));
        assertThat(stats.toString(), stats.getNumberOfEdges(), is(5));
        assertThat(stats.toString(), stats.getCriticalPathLength(), is(GraphStatistics.UNDEFINED));
    }

    /**
     * complex graph.
<pre>{@code
0 --- 1 --+ 2 +-- 3 --- 4
      5 -/     \- 6
       \--- 7 ---/

}</pre>
     */
    @Test
    public void complex() {
        Graph<Integer> graph = Graphs.newInstance();
        path(graph, 0, 1, 2, 3, 4);
        path(graph, 5, 2, 6);
        path(graph, 5, 7, 6);

        GraphStatistics stats = GraphStatistics.of(graph);
        assertThat(stats.toString(), stats.getNumberOfVertices(), is(8));
        assertThat(stats.toString(), stats.getNumberOfEdges(), is(8));
        assertThat(stats.toString(), stats.getCriticalPathLength(), is(5));
    }

    private static void path(Graph<Integer> graph, int... vertices) {
        assert vertices.length > 0;
        int last = vertices[0];
        graph.addNode(last);
        for (int i = 1; i < vertices.length; i++) {
            int next = vertices[i];
            graph.addEdge(last, next);
            last = next;
        }
    }
}
