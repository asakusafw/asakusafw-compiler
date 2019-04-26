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
package com.asakusafw.vanilla.core.engine;

import static com.asakusafw.dag.runtime.testing.MockDataModelUtil.*;
import static com.asakusafw.vanilla.core.testing.ModelMirrors.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.api.common.ObjectCursor;
import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.api.model.PortId;
import com.asakusafw.dag.api.model.VertexInfo;
import com.asakusafw.dag.api.processor.GroupReader;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.ObjectWriter;
import com.asakusafw.dag.runtime.skeleton.VoidVertexProcessor;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockDataModelUtil;
import com.asakusafw.dag.runtime.testing.MockDataModelUtil.KvSerDe1;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.vanilla.core.io.BasicBufferPool;
import com.asakusafw.vanilla.core.io.BasicBufferStore;
import com.asakusafw.vanilla.core.io.BufferPool;
import com.asakusafw.vanilla.core.mirror.GraphMirror;

/**
 * Test for {@link BasicEdgeDriver}.
 */
public class BasicEdgeDriverTest {

    static final Logger LOG = LoggerFactory.getLogger(BasicEdgeDriverTest.class);

    /**
     * resource lifecycle management.
     */
    @Rule
    public final ExternalResource lifecycle = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            store = BasicBufferStore.builder()
                    .build();
            pool = new BasicBufferPool(1_000_000, store);
        }
        @Override
        protected void after() {
            if (store != null) {
                store.close();
            }
        }
    };

    BasicBufferStore store;

    BufferPool pool;

    private int partitions = 1;

    private final int bufferSize = 10_000;

    private final int bufferMargin = 1_000;

    private final int recordCount = 10_000;

    private int mergeThreshold = 0;

    private final double mergeFactor = 1.0;

    /**
     * nothing - trivial case.
     * @throws Exception if failed
     */
    @Test
    public void nothing_trivial() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId upstream = v0.addOutputPort("p").getId();
        PortId downstream = v1.addInputPort("p").getId();
        info.addEdge(upstream, downstream, nothing());

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            // just complete
            complete(driver, upstream);
            complete(driver, downstream);
        }
    }

    /**
     * one-to-one - simple case.
     * @throws Exception if failed
     */
    @Test
    public void o2o_simple() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId upstream = v0.addOutputPort("p").getId();
        PortId downstream = v1.addInputPort("p").getId();
        info.addEdge(upstream, downstream, oneToOne(MockDataModelUtil.SerDe.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(upstream)) {
                writer.putObject(object(1, 0, "Hello, world!"));
            }
            complete(driver, upstream);
            try (ObjectReader reader = (ObjectReader) driver.acquireInput(downstream, 0, 1)) {
                check(reader, object(1, 0, "Hello, world!"));
            }
            complete(driver, downstream);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * one-to-one - w/ multiple upstreams.
     * @throws Exception if failed
     */
    @Test
    public void o2o_merge() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p0").getId();
        PortId u1 = v0.addOutputPort("p1").getId();
        PortId u2 = v0.addOutputPort("p2").getId();
        PortId d0 = v1.addInputPort("p").getId();
        info.addEdge(u0, d0, oneToOne(MockDataModelUtil.SerDe.class));
        info.addEdge(u1, d0, oneToOne(MockDataModelUtil.SerDe.class));
        info.addEdge(u2, d0, oneToOne(MockDataModelUtil.SerDe.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u0)) {
                writer.putObject(object(0, 0, "Hello0"));
            }
            complete(driver, u0);
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u1)) {
                writer.putObject(object(1, 0, "Hello1"));
            }
            complete(driver, u1);
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u2)) {
                writer.putObject(object(2, 0, "Hello2"));
            }
            complete(driver, u2);

            try (ObjectReader reader = (ObjectReader) driver.acquireInput(d0, 0, 1)) {
                check(reader,
                        object(0, 0, "Hello0"),
                        object(1, 0, "Hello1"),
                        object(2, 0, "Hello2"));
            }
            complete(driver, d0);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * one-to-one - w/ multiple downstreams.
     * @throws Exception if failed
     */
    @Test
    public void o2o_duplicate() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p").getId();
        PortId d0 = v1.addInputPort("p0").getId();
        PortId d1 = v1.addInputPort("p1").getId();
        PortId d2 = v1.addInputPort("p2").getId();
        info.addEdge(u0, d0, oneToOne(MockDataModelUtil.SerDe.class));
        info.addEdge(u0, d1, oneToOne(MockDataModelUtil.SerDe.class));
        info.addEdge(u0, d2, oneToOne(MockDataModelUtil.SerDe.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u0)) {
                writer.putObject(object(1, 0, "Hello, world!"));
            }
            complete(driver, u0);
            try (ObjectReader reader = (ObjectReader) driver.acquireInput(d0, 0, 1)) {
                check(reader, object(1, 0, "Hello, world!"));
            }
            complete(driver, d0);
            try (ObjectReader reader = (ObjectReader) driver.acquireInput(d1, 0, 1)) {
                check(reader, object(1, 0, "Hello, world!"));
            }
            complete(driver, d1);
            try (ObjectReader reader = (ObjectReader) driver.acquireInput(d2, 0, 1)) {
                check(reader, object(1, 0, "Hello, world!"));
            }
            complete(driver, d2);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * one-to-one - w/ concurrent access.
     * @throws Exception if failed
     */
    @Test
    public void o2o_concurrent() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId upstream = v0.addOutputPort("p").getId();
        PortId downstream = v1.addInputPort("p").getId();
        info.addEdge(upstream, downstream, oneToOne(MockDataModelUtil.SerDe.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter w0 = (ObjectWriter) driver.acquireOutput(upstream);
                    ObjectWriter w1 = (ObjectWriter) driver.acquireOutput(upstream);
                    ObjectWriter w2 = (ObjectWriter) driver.acquireOutput(upstream)) {
                w0.putObject(object(0, 0, "Hello0"));
                w1.putObject(object(1, 0, "Hello1"));
                w2.putObject(object(2, 0, "Hello2"));
                w0.putObject(object(3, 0, "Hello0"));
                w1.putObject(object(4, 0, "Hello1"));
                w2.putObject(object(5, 0, "Hello2"));
            }
            complete(driver, upstream);
            try (ObjectReader r0 = (ObjectReader) driver.acquireInput(downstream, 0, 1);
                    ObjectReader r1 = (ObjectReader) driver.acquireInput(downstream, 0, 1);
                    ObjectReader r2 = (ObjectReader) driver.acquireInput(downstream, 0, 1)) {
                List<MockDataModel> results = new ArrayList<>();
                boolean saw;
                do {
                    saw = false;
                    for (ObjectReader r : new ObjectReader[] { r0, r1, r2 }) {
                        if (r.nextObject()) {
                            results.add(new MockDataModel((MockDataModel) r.getObject()));
                            saw = true;
                        }
                    }
                } while (saw);
                assertThat(results, containsInAnyOrder(
                        object(0, 0, "Hello0"),
                        object(1, 0, "Hello1"),
                        object(2, 0, "Hello2"),
                        object(3, 0, "Hello0"),
                        object(4, 0, "Hello1"),
                        object(5, 0, "Hello2")));
            }
            complete(driver, downstream);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * broadcast - simple case.
     * @throws Exception if failed
     */
    @Test
    public void broadcast_simple() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId upstream = v0.addOutputPort("p").getId();
        PortId downstream = v1.addInputPort("p").getId();
        info.addEdge(upstream, downstream, broadcast(MockDataModelUtil.SerDe.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(upstream)) {
                writer.putObject(object(1, 0, "Hello, world!"));
            }
            complete(driver, upstream);
            try (ObjectReader reader = (ObjectReader) driver.acquireInput(downstream, 0, 1)) {
                check(reader, object(1, 0, "Hello, world!"));
            }
            complete(driver, downstream);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * broadcast - w/ multiple upstreams.
     * @throws Exception if failed
     */
    @Test
    public void broadcast_merge() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p0").getId();
        PortId u1 = v0.addOutputPort("p1").getId();
        PortId u2 = v0.addOutputPort("p2").getId();
        PortId d0 = v1.addInputPort("p").getId();
        info.addEdge(u0, d0, broadcast(MockDataModelUtil.SerDe.class));
        info.addEdge(u1, d0, broadcast(MockDataModelUtil.SerDe.class));
        info.addEdge(u2, d0, broadcast(MockDataModelUtil.SerDe.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u0)) {
                writer.putObject(object(0, 0, "Hello0"));
            }
            complete(driver, u0);
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u1)) {
                writer.putObject(object(1, 0, "Hello1"));
            }
            complete(driver, u1);
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u2)) {
                writer.putObject(object(2, 0, "Hello2"));
            }
            complete(driver, u2);
            try (ObjectReader reader = (ObjectReader) driver.acquireInput(d0, 0, 1)) {
                check(reader,
                        object(0, 0, "Hello0"),
                        object(1, 0, "Hello1"),
                        object(2, 0, "Hello2"));
            }
            complete(driver, d0);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * broadcast - w/ multiple downstreams.
     * @throws Exception if failed
     */
    @Test
    public void broadcast_duplicate() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p").getId();
        PortId d0 = v1.addInputPort("p0").getId();
        PortId d1 = v1.addInputPort("p1").getId();
        PortId d2 = v1.addInputPort("p2").getId();
        info.addEdge(u0, d0, broadcast(MockDataModelUtil.SerDe.class));
        info.addEdge(u0, d1, broadcast(MockDataModelUtil.SerDe.class));
        info.addEdge(u0, d2, broadcast(MockDataModelUtil.SerDe.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u0)) {
                writer.putObject(object(0, 0, "Hello0"));
            }
            complete(driver, u0);
            try (ObjectReader reader = (ObjectReader) driver.acquireInput(d0, 0, 1)) {
                check(reader, object(0, 0, "Hello0"));
            }
            complete(driver, d0);
            try (ObjectReader reader = (ObjectReader) driver.acquireInput(d1, 0, 1)) {
                check(reader, object(0, 0, "Hello0"));
            }
            complete(driver, d1);
            try (ObjectReader reader = (ObjectReader) driver.acquireInput(d2, 0, 1)) {
                check(reader, object(0, 0, "Hello0"));
            }
            complete(driver, d2);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * broadcast - w/ concurrent access.
     * @throws Exception if failed
     */
    @Test
    public void broadcast_concurrent() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p").getId();
        PortId d0 = v1.addInputPort("p").getId();
        info.addEdge(u0, d0, broadcast(MockDataModelUtil.SerDe.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter w0 = (ObjectWriter) driver.acquireOutput(u0);
                    ObjectWriter w1 = (ObjectWriter) driver.acquireOutput(u0);
                    ObjectWriter w2 = (ObjectWriter) driver.acquireOutput(u0);) {
                w0.putObject(object(0, 0, "Hello0"));
                w1.putObject(object(1, 0, "Hello1"));
                w2.putObject(object(2, 0, "Hello2"));
            }
            complete(driver, u0);
            try (ObjectReader r0 = (ObjectReader) driver.acquireInput(d0, 0, 1);
                    ObjectReader r1 = (ObjectReader) driver.acquireInput(d0, 0, 1);
                    ObjectReader r2 = (ObjectReader) driver.acquireInput(d0, 0, 1)) {
                check(r0,
                        object(0, 0, "Hello0"),
                        object(1, 0, "Hello1"),
                        object(2, 0, "Hello2"));
                check(r1,
                        object(0, 0, "Hello0"),
                        object(1, 0, "Hello1"),
                        object(2, 0, "Hello2"));
                check(r2,
                        object(0, 0, "Hello0"),
                        object(1, 0, "Hello1"),
                        object(2, 0, "Hello2"));
            }
            complete(driver, d0);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * scatter-gather - simple case.
     * @throws Exception if failed
     */
    @Test
    public void scatter_simple() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p").getId();
        PortId d0 = v1.addInputPort("p").getId();
        info.addEdge(u0, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u0)) {
                writer.putObject(object(1, 0, "Hello, world!"));
            }
            complete(driver, u0);
            try (GroupReader reader = (GroupReader) driver.acquireInput(d0, 0, 1)) {
                check(reader, object(1, 0, "Hello, world!"));
            }
            complete(driver, d0);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * scatter-gather - w/o upstream data.
     * @throws Exception if failed
     */
    @Test
    public void scatter_empty() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p").getId();
        PortId d0 = v1.addInputPort("p").getId();
        info.addEdge(u0, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u0)) {
                Lang.pass(writer);
            }
            complete(driver, u0);
            try (GroupReader reader = (GroupReader) driver.acquireInput(d0, 0, 1)) {
                assertThat(reader.nextGroup(), is(false));
            }
            complete(driver, d0);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * scatter-gather - w/ multiple upstreams.
     * @throws Exception if failed
     */
    @Test
    public void scatter_merge() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p0").getId();
        PortId u1 = v0.addOutputPort("p1").getId();
        PortId u2 = v0.addOutputPort("p2").getId();
        PortId d0 = v1.addInputPort("p").getId();
        info.addEdge(u0, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));
        info.addEdge(u1, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));
        info.addEdge(u2, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u0)) {
                writer.putObject(object(0, 0, "Hello0"));
            }
            complete(driver, u0);
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u1)) {
                writer.putObject(object(1, 0, "Hello1"));
                writer.putObject(object(1, 0, "Hello2"));
            }
            complete(driver, u1);
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u2)) {
                writer.putObject(object(2, 0, "Hello3"));
                writer.putObject(object(2, 0, "Hello4"));
                writer.putObject(object(2, 0, "Hello5"));
            }
            complete(driver, u2);
            try (GroupReader reader = (GroupReader) driver.acquireInput(d0, 0, 1)) {
                check(reader,
                        object(0, 0, "Hello0"),
                        object(1, 0, "Hello1"),
                        object(1, 0, "Hello2"),
                        object(2, 0, "Hello3"),
                        object(2, 0, "Hello4"),
                        object(2, 0, "Hello5"));
            }
            complete(driver, d0);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * scatter-gather - w/ multiple upstreams.
     * @throws Exception if failed
     */
    @Test
    public void scatter_merge_multistage() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p0").getId();
        PortId u1 = v0.addOutputPort("p1").getId();
        PortId u2 = v0.addOutputPort("p2").getId();
        PortId d0 = v1.addInputPort("p").getId();
        info.addEdge(u0, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));
        info.addEdge(u1, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));
        info.addEdge(u2, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));

        mergeThreshold = 2;

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u0)) {
                writer.putObject(object(0, 0, "Hello0"));
            }
            complete(driver, u0);
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u1)) {
                writer.putObject(object(1, 0, "Hello1"));
                writer.putObject(object(1, 0, "Hello2"));
            }
            complete(driver, u1);
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u2)) {
                writer.putObject(object(2, 0, "Hello3"));
                writer.putObject(object(2, 0, "Hello4"));
                writer.putObject(object(2, 0, "Hello5"));
            }
            complete(driver, u2);
            try (GroupReader reader = (GroupReader) driver.acquireInput(d0, 0, 1)) {
                check(reader,
                        object(0, 0, "Hello0"),
                        object(1, 0, "Hello1"),
                        object(1, 0, "Hello2"),
                        object(2, 0, "Hello3"),
                        object(2, 0, "Hello4"),
                        object(2, 0, "Hello5"));
            }
            complete(driver, d0);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * scatter-gather - w/ multiple upstreams + striping.
     * @throws Exception if failed
     */
    @Test
    public void scatter_stripe() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p0").getId();
        PortId u1 = v0.addOutputPort("p1").getId();
        PortId u2 = v0.addOutputPort("p2").getId();
        PortId d0 = v1.addInputPort("p").getId();
        info.addEdge(u0, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));
        info.addEdge(u1, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));
        info.addEdge(u2, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u0)) {
                writer.putObject(object(0, 0, "Hello0"));
                writer.putObject(object(1, 0, "Hello0"));
                writer.putObject(object(2, 0, "Hello0"));
            }
            complete(driver, u0);
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u1)) {
                writer.putObject(object(1, 0, "Hello1"));
                writer.putObject(object(0, 0, "Hello1"));
                writer.putObject(object(2, 0, "Hello1"));
            }
            complete(driver, u1);
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u2)) {
                writer.putObject(object(2, 0, "Hello2"));
                writer.putObject(object(1, 0, "Hello2"));
                writer.putObject(object(0, 0, "Hello2"));
            }
            complete(driver, u2);
            try (GroupReader reader = (GroupReader) driver.acquireInput(d0, 0, 1)) {
                check(reader,
                        object(0, 0, "Hello0"),
                        object(0, 0, "Hello1"),
                        object(0, 0, "Hello2"),
                        object(1, 0, "Hello0"),
                        object(1, 0, "Hello1"),
                        object(1, 0, "Hello2"),
                        object(2, 0, "Hello0"),
                        object(2, 0, "Hello1"),
                        object(2, 0, "Hello2"));
            }
            complete(driver, d0);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * scatter-gather - w/ multiple downstreams.
     * @throws Exception if failed
     */
    @Test
    public void scatter_duplicate() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p").getId();
        PortId d0 = v1.addInputPort("p0").getId();
        PortId d1 = v1.addInputPort("p1").getId();
        PortId d2 = v1.addInputPort("p2").getId();
        info.addEdge(u0, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));
        info.addEdge(u0, d1, scatterGather(KvSerDe1.class, KvSerDe1.class));
        info.addEdge(u0, d2, scatterGather(KvSerDe1.class, KvSerDe1.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u0)) {
                writer.putObject(object(1, 0, "Hello, world!"));
            }
            complete(driver, u0);
            try (GroupReader reader = (GroupReader) driver.acquireInput(d0, 0, 1)) {
                check(reader, object(1, 0, "Hello, world!"));
            }
            complete(driver, d0);
            try (GroupReader reader = (GroupReader) driver.acquireInput(d1, 0, 1)) {
                check(reader, object(1, 0, "Hello, world!"));
            }
            complete(driver, d1);
            try (GroupReader reader = (GroupReader) driver.acquireInput(d2, 0, 1)) {
                check(reader, object(1, 0, "Hello, world!"));
            }
            complete(driver, d2);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * scatter-gather - w/ concurrent access.
     * @throws Exception if failed
     */
    @Test
    public void scatter_concurrent() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p").getId();
        PortId d0 = v1.addInputPort("p").getId();
        info.addEdge(u0, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter w0 = (ObjectWriter) driver.acquireOutput(u0);
                    ObjectWriter w1 = (ObjectWriter) driver.acquireOutput(u0);
                    ObjectWriter w2 = (ObjectWriter) driver.acquireOutput(u0)) {
                w0.putObject(object(0, 0, "Hello0"));
                w0.putObject(object(1, 0, "Hello0"));
                w0.putObject(object(2, 0, "Hello0"));
                w1.putObject(object(0, 0, "Hello1"));
                w1.putObject(object(1, 0, "Hello1"));
                w1.putObject(object(2, 0, "Hello1"));
                w2.putObject(object(0, 0, "Hello2"));
                w2.putObject(object(1, 0, "Hello2"));
                w2.putObject(object(2, 0, "Hello2"));
            }
            complete(driver, u0);
            try (GroupReader reader = (GroupReader) driver.acquireInput(d0, 0, 1)) {
                check(reader,
                        object(0, 0, "Hello0"),
                        object(0, 0, "Hello1"),
                        object(0, 0, "Hello2"),
                        object(1, 0, "Hello0"),
                        object(1, 0, "Hello1"),
                        object(1, 0, "Hello2"),
                        object(2, 0, "Hello0"),
                        object(2, 0, "Hello1"),
                        object(2, 0, "Hello2"));
            }
            complete(driver, d0);
        }
        assertThat(pool.getSize(), is(0L));
    }

    /**
     * scatter-gather - w/ multiple partitions.
     * @throws Exception if failed
     */
    @Test
    public void scatter_partitions() throws Exception {
        GraphInfo info = new GraphInfo();
        VertexInfo v0 = info.addVertex("v0", vertex(VoidVertexProcessor.class));
        VertexInfo v1 = info.addVertex("v1", vertex(VoidVertexProcessor.class));
        PortId u0 = v0.addOutputPort("p").getId();
        PortId d0 = v1.addInputPort("p").getId();
        info.addEdge(u0, d0, scatterGather(KvSerDe1.class, KvSerDe1.class));

        partitions = 3;
        List<MockDataModel> objects = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            objects.add(object(i % 153, 0, "Hello" + i));
        }

        GraphMirror graph = GraphMirror.of(info);
        try (EdgeDriver driver = driver(graph)) {
            try (ObjectWriter writer = (ObjectWriter) driver.acquireOutput(u0)) {
                for (MockDataModel object : objects) {
                    writer.putObject(object);
                }
            }
            complete(driver, u0);

            List<MockDataModel> o0;
            List<MockDataModel> o1;
            List<MockDataModel> o2;
            try (GroupReader r0 = (GroupReader) driver.acquireInput(d0, 0, partitions);
                    GroupReader r1 = (GroupReader) driver.acquireInput(d0, 1, partitions);
                    GroupReader r2 = (GroupReader) driver.acquireInput(d0, 2, partitions)) {
                o0 = collect(r0);
                o1 = collect(r1);
                o2 = collect(r2);
            }
            complete(driver, d0);

            assertThat(disjoint(keys(o0), keys(o1)), is(true));
            assertThat(disjoint(keys(o0), keys(o2)), is(true));
            assertThat(disjoint(keys(o1), keys(o2)), is(true));

            List<MockDataModel> results = new ArrayList<>();
            results.addAll(o0);
            results.addAll(o1);
            results.addAll(o2);

            assertThat(sort(results), is(sort(objects)));
        }
        assertThat(pool.getSize(), is(0L));
    }

    private static void complete(EdgeDriver edges, PortId id) throws IOException, InterruptedException {
        LOG.debug("complete {} ({})", id, edges);
        edges.complete(id);
    }

    private static List<MockDataModel> collect(GroupReader reader) throws IOException, InterruptedException {
        List<MockDataModel> results = new ArrayList<>();
        Set<Object> saw = new HashSet<>();
        while (reader.nextGroup()) {
            Object key = reader.getGroup().getValue();
            assertThat(saw.contains(key), is(false));
            saw.add(key);
            while (reader.nextObject()) {
                results.add(new MockDataModel((MockDataModel) reader.getObject()));
            }
        }
        return results;
    }

    private static void check(ObjectCursor reader, MockDataModel... expected) throws IOException, InterruptedException {
        List<MockDataModel> models = new ArrayList<>(Arrays.asList(expected));
        while (reader.nextObject()) {
            Object object = reader.getObject();
            boolean saw = models.remove(object);
            assertThat(object.toString(), saw, is(true));
        }
        assertThat(models, empty());
    }

    private static void check(GroupReader reader, MockDataModel... expected) throws IOException, InterruptedException {
        Map<Integer, List<MockDataModel>> groups = Arrays.stream(expected)
                .sequential()
                .collect(Collectors.groupingBy(
                        MockDataModel::getKey,
                        Collectors.toList()));
        while (reader.nextGroup()) {
            int key = (Integer) reader.getGroup().getValue();
            List<MockDataModel> group = groups.remove(key);
            assertThat(String.valueOf(key), group, is(notNullValue()));
            check((ObjectCursor) reader, group.toArray(new MockDataModel[group.size()]));
        }
        assertThat(groups.keySet(), empty());
    }

    private EdgeDriver driver(GraphMirror graph) {
        return new BasicEdgeDriver(
                getClass().getClassLoader(),
                graph,
                pool, store.getBlobStore(),
                partitions,
                bufferSize, bufferMargin, recordCount,
                mergeThreshold, mergeFactor);
    }

    private BitSet keys(List<MockDataModel> objects) {
        BitSet bits = new BitSet();
        objects.forEach(o -> bits.set(o.getKey()));
        return bits;
    }

    private boolean disjoint(BitSet a, BitSet b) {
        for (int i = a.nextSetBit(0); i >= 0; i = a.nextSetBit(i + 1)) {
            if (b.get(i)) {
                return false;
            }
        }
        return true;
    }
}
