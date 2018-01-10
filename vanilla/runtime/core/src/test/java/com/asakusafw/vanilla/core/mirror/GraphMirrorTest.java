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
package com.asakusafw.vanilla.core.mirror;

import static com.asakusafw.vanilla.core.testing.ModelMirrors.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.api.model.PortInfo;
import com.asakusafw.dag.api.model.VertexInfo;
import com.asakusafw.dag.api.model.basic.BasicEdgeDescriptor.Movement;
import com.asakusafw.dag.runtime.skeleton.VoidVertexProcessor;
import com.asakusafw.dag.runtime.testing.IntSerDe;
import com.asakusafw.dag.runtime.testing.MockDataModelUtil;

/**
 * Test for {@link GraphMirror}.
 */
public class GraphMirrorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        GraphInfo graph = new GraphInfo();
        VertexInfo v0 = graph.addVertex("v0", vertex(VoidVertexProcessor.class));

        GraphMirror mirror = GraphMirror.of(graph);
        assertThat(mirror.getVertices(), hasSize(1));

        VertexMirror v0m = mirror.getVertex(v0.getId());
        assertThat(v0m.getInputs(), hasSize(0));
        assertThat(v0m.getOutputs(), hasSize(0));

        assertThat(v0m.newProcessor(getClass().getClassLoader()), instanceOf(VoidVertexProcessor.class));
    }

    /**
     * with edge.
     */
    @Test
    public void edge() {
        GraphInfo graph = new GraphInfo();
        VertexInfo v0 = graph.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = graph.addVertex("v1", vertex(VoidVertexProcessor.class));

        PortInfo v0o0 = v0.addOutputPort("o0");
        PortInfo v1i0 = v1.addInputPort("i0");
        graph.addEdge(v0o0.getId(), v1i0.getId(), oneToOne(IntSerDe.class));

        GraphMirror mirror = GraphMirror.of(graph);
        VertexMirror v0m = mirror.getVertex(v0.getId());
        VertexMirror v1m = mirror.getVertex(v1.getId());
        OutputPortMirror v0o0m = mirror.getOutput(v0o0.getId());
        InputPortMirror v1i0m = mirror.getInput(v1i0.getId());

        assertThat(v0o0m.getOwner(), is(v0m));
        assertThat(v0o0m.getMovement(), is(Movement.ONE_TO_ONE));
        assertThat(v0o0m.newValueSerDe(getClass().getClassLoader()), instanceOf(IntSerDe.class));
        assertThat(v0o0m.getOpposites(), contains(v1i0m));

        assertThat(v1i0m.getOwner(), is(v1m));
        assertThat(v1i0m.getMovement(), is(Movement.ONE_TO_ONE));
        assertThat(v1i0m.newValueSerDe(getClass().getClassLoader()), instanceOf(IntSerDe.class));
        assertThat(v1i0m.getOpposites(), contains(v0o0m));
    }

    /**
     * w/ scatter-gather.
     */
    @Test
    public void scatter_gather() {
        GraphInfo graph = new GraphInfo();
        VertexInfo v0 = graph.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = graph.addVertex("v1", vertex(VoidVertexProcessor.class));

        PortInfo v0o0 = v0.addOutputPort("o0");
        PortInfo v1i0 = v1.addInputPort("i0");
        graph.addEdge(v0o0.getId(), v1i0.getId(),
                scatterGather(MockDataModelUtil.KvSerDe1.class, MockDataModelUtil.KvSerDe1.class));

        GraphMirror mirror = GraphMirror.of(graph);
        VertexMirror v0m = mirror.getVertex(v0.getId());
        VertexMirror v1m = mirror.getVertex(v1.getId());
        OutputPortMirror v0o0m = mirror.getOutput(v0o0.getId());
        InputPortMirror v1i0m = mirror.getInput(v1i0.getId());

        assertThat(v0o0m.getOwner(), is(v0m));
        assertThat(v0o0m.getMovement(), is(Movement.SCATTER_GATHER));
        assertThat(v0o0m.newKeyValueSerDe(getClass().getClassLoader()), instanceOf(MockDataModelUtil.KvSerDe1.class));
        assertThat(v0o0m.newComparator(getClass().getClassLoader()), instanceOf(MockDataModelUtil.KvSerDe1.class));
        assertThat(v0o0m.getOpposites(), contains(v1i0m));

        assertThat(v1i0m.getOwner(), is(v1m));
        assertThat(v1i0m.getMovement(), is(Movement.SCATTER_GATHER));
        assertThat(v1i0m.newKeyValueSerDe(getClass().getClassLoader()), instanceOf(MockDataModelUtil.KvSerDe1.class));
        assertThat(v1i0m.newComparator(getClass().getClassLoader()), instanceOf(MockDataModelUtil.KvSerDe1.class));
        assertThat(v1i0m.getOpposites(), contains(v0o0m));
    }

    /**
     * multiple vertices.
     */
    @Test
    public void multiple() {
        GraphInfo graph = new GraphInfo();
        VertexInfo v0 = graph.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = graph.addVertex("v1", vertex(VoidVertexProcessor.class));
        VertexInfo v2 = graph.addVertex("v2", vertex(VoidVertexProcessor.class));

        PortInfo v0o0 = v0.addOutputPort("o0");
        PortInfo v1i0 = v1.addInputPort("i0");
        PortInfo v1o0 = v1.addOutputPort("o0");
        PortInfo v2i0 = v2.addInputPort("i0");
        graph.addEdge(v0o0.getId(), v1i0.getId(), oneToOne(IntSerDe.class));
        graph.addEdge(v1o0.getId(), v2i0.getId(), oneToOne(IntSerDe.class));

        GraphMirror mirror = GraphMirror.of(graph);
        assertThat(mirror.getVertices(), hasSize(3));

        VertexMirror v0m = mirror.getVertex(v0.getId());
        assertThat(v0m.getInputs(), hasSize(0));
        assertThat(v0m.getOutputs(), hasSize(1));

        VertexMirror v1m = mirror.getVertex(v1.getId());
        assertThat(v1m.getInputs(), hasSize(1));
        assertThat(v1m.getOutputs(), hasSize(1));

        VertexMirror v2m = mirror.getVertex(v2.getId());
        assertThat(v2m.getInputs(), hasSize(1));
        assertThat(v2m.getOutputs(), hasSize(0));

        OutputPortMirror v0o0m = mirror.getOutput(v0o0.getId());
        InputPortMirror v1i0m = mirror.getInput(v1i0.getId());
        assertThat(v0o0m.getOpposites(), contains(v1i0m));
        assertThat(v1i0m.getOpposites(), contains(v0o0m));

        OutputPortMirror v1o0m = mirror.getOutput(v1o0.getId());
        InputPortMirror v2i0m = mirror.getInput(v2i0.getId());
        assertThat(v1o0m.getOpposites(), contains(v2i0m));
        assertThat(v2i0m.getOpposites(), contains(v1o0m));
    }
}
