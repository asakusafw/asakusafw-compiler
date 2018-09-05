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
package com.asakusafw.vanilla.compiler.core;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.dag.runtime.testing.MockKeyValueModel;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Groups;
import com.asakusafw.lang.compiler.model.graph.OperatorConstraint;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;
import com.asakusafw.lang.compiler.tester.CompilerProfile;
import com.asakusafw.lang.compiler.tester.executor.JobflowExecutor;
import com.asakusafw.lang.compiler.tester.externalio.TestInput;
import com.asakusafw.lang.compiler.tester.externalio.TestOutput;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.GroupView;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.windows.WindowsSupport;
import com.asakusafw.vanilla.compiler.tester.InProcessVanillaTaskExecutor;
import com.asakusafw.vanilla.compiler.tester.externalio.TestIoTaskExecutor;
import com.asakusafw.vocabulary.external.ImporterDescription.DataSize;
import com.asakusafw.vocabulary.flow.processor.PartialAggregation;
import com.asakusafw.vocabulary.model.Key;
import com.asakusafw.vocabulary.operator.Branch;
import com.asakusafw.vocabulary.operator.CoGroup;
import com.asakusafw.vocabulary.operator.Convert;
import com.asakusafw.vocabulary.operator.Extract;
import com.asakusafw.vocabulary.operator.Fold;
import com.asakusafw.vocabulary.operator.MasterJoinUpdate;
import com.asakusafw.vocabulary.operator.Update;

/**
 * Test for {@link VanillaJobflowProcessor}.
 */
public class VanillaJobflowProcessorTest extends VanillaCompilerTesterRoot {

    static final File WORKING = new File("target/" + VanillaJobflowProcessorTest.class.getSimpleName());

    /**
     * Support for Windows platform.
     */
    @ClassRule
    public static final WindowsSupport WINDOWS_SUPPORT = new WindowsSupport();

    /**
     * profile helper.
     */
    @Rule
    public final ExternalResource initializer = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            profile.forToolRepository()
                .useDefaults();
            profile.forCompilerOptions()
                .withRuntimeWorkingDirectory(WORKING.getAbsolutePath(), false);
        }
    };

    final CompilerProfile profile = new CompilerProfile(getClass().getClassLoader());

    final TestIoTaskExecutor testio = new TestIoTaskExecutor();

    final JobflowExecutor executor = new JobflowExecutor(Arrays.asList(new InProcessVanillaTaskExecutor(), testio))
            .withBefore(testio::check)
            .withBefore((a, c) -> ResourceUtil.delete(WORKING))
            .withAfter((a, c) -> ResourceUtil.delete(WORKING));

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello, world!"));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "Hello, world!")));
        });
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "out"));
    }

    /**
     * Orphaned generic external output w/o generator constraint.
     * @throws Exception if failed
     */
    @Test
    public void output_orphaned() throws Exception {
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, hasSize(0));
        });
        run(profile, executor, g -> g
                .output("out", TestOutput.of("t", MockDataModel.class)
                        .withGenerator(true)));
    }

    /**
     * w/ extract kind operator.
     * @throws Exception if failed
     */
    @Test
    public void extract() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello, world!"));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "Hello, world!?")));
        });
        /*
         * [In] -> [Extract] -> [Out]
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .operator("op", Ops.class, "update", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("out", typeOf(MockDataModel.class))
                        .build())
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "op")
                .connect("op", "out"));
    }

    /**
     * w/ parameterized operator.
     * @throws Exception if failed
     */
    @Test
    public void arguments() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello, world!"));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "Hello, world!$")));
        });
        /*
         * [In] -> [Extract] -> [Out]
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .operator("op", Ops.class, "parameterized", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("out", typeOf(MockDataModel.class))
                        .argument("suffix", valueOf("$"))
                        .build())
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "op")
                .connect("op", "out"));
    }

    /**
     * w/ checkpoint operator.
     * @throws Exception if failed
     */
    @Test
    public void checkpoint() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello, world!"));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "Hello, world!?")));
        });
        /*
         * [In] -> [Checkpoint] -> [Extract] -> [Out]
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .operator("cp", CoreOperatorKind.CHECKPOINT, b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("out", typeOf(MockDataModel.class))
                        .build())
                .operator("op", Ops.class, "update", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("out", typeOf(MockDataModel.class))
                        .build())
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "cp")
                .connect("cp", "op")
                .connect("op", "out"));
    }

    /**
     * w/ co-group kind operator.
     * @throws Exception if failed
     */
    @Test
    public void cogroup() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, d(1), "Hello0"));
            o.write(new MockDataModel(0, d(0), "Hello1"));
            o.write(new MockDataModel(1, d(2), "Hello2"));
            o.write(new MockDataModel(1, d(0), "Hello3"));
            o.write(new MockDataModel(1, d(1), "Hello4"));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, containsInAnyOrder(
                    new MockDataModel(0, d(0), "Hello1@0"),
                    new MockDataModel(0, d(1), "Hello0@1"),
                    new MockDataModel(1, d(0), "Hello3@0"),
                    new MockDataModel(1, d(1), "Hello4@1"),
                    new MockDataModel(1, d(2), "Hello2@2")));
        });
        /*
         * [In] -> [CoGroup] -> [Out]
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .operator("op", Ops.class, "group", b -> b
                        .input("in", typeOf(MockDataModel.class), group("key", "+sort"))
                        .output("out", typeOf(MockDataModel.class))
                        .build())
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "op")
                .connect("op", "out"));
    }

    /**
     * w/ broadcast operator.
     * @throws Exception if failed
     */
    @Test
    public void broadcast() throws Exception {
        testio.input("in0", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello0"));
            o.write(new MockDataModel(1, "Hello1"));
            o.write(new MockDataModel(2, "Hello2"));
        });
        testio.input("in1", MockKeyValueModel.class, o -> {
            o.write(new MockKeyValueModel(0, "A"));
            o.write(new MockKeyValueModel(1, "B"));
        });
        testio.output("out0", MockDataModel.class, o -> {
            assertThat(o, containsInAnyOrder(
                    new MockDataModel(0, "Hello0@A"),
                    new MockDataModel(1, "Hello1@B")));
        });
        testio.output("out1", MockDataModel.class, o -> {
            assertThat(o, containsInAnyOrder(
                    new MockDataModel(2, "Hello2")));
        });
        /*
         * [In0] -> [Join] ----> [Out0]
         *            |   \
         * [In1] -----/    \--> [Out1]
         */
        run(profile, executor, g -> g
                .input("in0", TestInput.of("in0", MockDataModel.class))
                .input("in1", TestInput.of("in1", MockKeyValueModel.class, DataSize.TINY))
                .operator("op", Ops.class, "join", b -> b
                        .input("mst", typeOf(MockKeyValueModel.class), group("key"))
                        .input("tx", typeOf(MockDataModel.class), group("key"))
                        .output("joined", typeOf(MockDataModel.class))
                        .output("missed", typeOf(MockDataModel.class))
                        .build())
                .output("out0", TestOutput.of("out0", MockDataModel.class))
                .output("out1", TestOutput.of("out1", MockDataModel.class))
                .connect("in0", "op.tx")
                .connect("in1", "op.mst")
                .connect("op.joined", "out0")
                .connect("op.missed", "out1"));
    }
    /**
     * w/ aggregative operator.
     * @throws Exception if failed
     */
    @Test
    public void aggregate() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, d(1), ""));
            o.write(new MockDataModel(1, d(2), ""));
            o.write(new MockDataModel(1, d(3), ""));
            o.write(new MockDataModel(2, d(4), ""));
            o.write(new MockDataModel(2, d(5), ""));
            o.write(new MockDataModel(2, d(6), ""));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, contains(
                    new MockDataModel(0, d(1), ""),
                    new MockDataModel(1, d(5), ""),
                    new MockDataModel(2, d(15), "")));
        });
        /*
         * [In] -> [Aggregate] -> [Out]
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .operator("op", Ops.class, "aggregate", b -> b
                        .input("in", typeOf(MockDataModel.class), group("key"))
                        .output("out", typeOf(MockDataModel.class))
                        .build())
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "op")
                .connect("op", "out"));
    }

    /**
     * w/ aggregative operator.
     * @throws Exception if failed
     */
    @Test
    public void aggregate_pre() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, d(1), ""));
            o.write(new MockDataModel(1, d(2), ""));
            o.write(new MockDataModel(1, d(3), ""));
            o.write(new MockDataModel(2, d(4), ""));
            o.write(new MockDataModel(2, d(5), ""));
            o.write(new MockDataModel(2, d(6), ""));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, contains(
                    new MockDataModel(0, d(1), ""),
                    new MockDataModel(1, d(5), ""),
                    new MockDataModel(2, d(15), "")));
        });
        /*
         * [In] -> [Aggregate] -> [Out]
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .operator("op", Ops.class, "preaggregate", b -> b
                        .input("in", typeOf(MockDataModel.class), group("key"))
                        .output("out", typeOf(MockDataModel.class))
                        .build())
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "op")
                .connect("op", "out"));
    }

    /**
     * w/ buffer operator.
     * @throws Exception if failed
     */
    @Test
    public void buffer() throws Exception {
        testio.input("in", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello"));
        });
        testio.output("out0", MockDataModel.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "HelloA")));
        });
        testio.output("out1", MockDataModel.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "HelloB")));
        });
        testio.output("out2", MockDataModel.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "HelloC")));
        });
        /*
         *          +-> [Extract0] -> [Out0]
         *         /
         * [In] --+---> [Extract1] -> [Out1]
         *         \
         *          +-> [Extract2] -> [Out2]
         *
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("in", MockDataModel.class))
                .operator("op0", Ops.class, "parameterized", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("out", typeOf(MockDataModel.class))
                        .argument("suffix", valueOf("A"))
                        .build())
                .operator("op1", Ops.class, "parameterized", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("out", typeOf(MockDataModel.class))
                        .argument("suffix", valueOf("B"))
                        .build())
                .operator("op2", Ops.class, "parameterized", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("out", typeOf(MockDataModel.class))
                        .argument("suffix", valueOf("C"))
                        .build())
                .output("out0", TestOutput.of("out0", MockDataModel.class))
                .output("out1", TestOutput.of("out1", MockDataModel.class))
                .output("out2", TestOutput.of("out2", MockDataModel.class))
                .connect("in", "op0", "op1", "op2")
                .connect("op0", "out0")
                .connect("op1", "out1")
                .connect("op2", "out2"));
    }

    /**
     * w/ sticky operator.
     * @throws Exception if failed
     */
    @Test
    public void sticky() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello, world!"));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "Hello, world!?")));
        });
        /*
         * [In] --+--> [Extract] -> [Out]
         *         \
         *          -> [Sticky] -|
         */
        Ops.STICKY.set(false);
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .operator("op", Ops.class, "update", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("out", typeOf(MockDataModel.class))
                        .build())
                .operator("sticky", Ops.class, "sticky", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("out", typeOf(MockDataModel.class))
                        .constraint(OperatorConstraint.AT_LEAST_ONCE)
                        .build())
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "op", "sticky")
                .connect("op", "out"));
        assertThat(Ops.STICKY.get(), is(true));
    }

    /**
     * w/ sticky operator.
     * @throws Exception if failed
     */
    @Test
    public void sticky_without_other_edge_output() throws Exception {
        testio.input("in0", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello, world!"));
        });
        testio.input("in1", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello, world!"));
        });
        testio.output("out", MockDataModel.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "Hello, world!?")));
        });
        /*
         * [In0] -> [Extract] -> [Out]
         * [In1] -> [Sticky] -|
         */
        Ops.STICKY.set(false);
        run(profile, executor, g -> g
                .input("in0", TestInput.of("in0", MockDataModel.class))
                .input("in1", TestInput.of("in1", MockDataModel.class))
                .operator("op", Ops.class, "update", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("out", typeOf(MockDataModel.class))
                        .build())
                .operator("sticky", Ops.class, "sticky", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("out", typeOf(MockDataModel.class))
                        .constraint(OperatorConstraint.AT_LEAST_ONCE)
                        .build())
                .output("out", TestOutput.of("out", MockDataModel.class))
                .connect("in0", "op")
                .connect("in1", "sticky")
                .connect("op", "out"));
        assertThat(Ops.STICKY.get(), is(true));
    }

    /**
     * operator w/ disconnected port.
     * @throws Exception if failed
     */
    @Test
    public void output_discard() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello, world!"));
        });
        testio.output("t", MockKeyValueModel.class, o -> {
            assertThat(o, contains(new MockKeyValueModel(0, "Hello, world!")));
        });
        /*
         * [In] -> [Extract] --> [Out]
         *                  \
         *                   +-|
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .operator("op", Ops.class, "convert", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("original", typeOf(MockDataModel.class))
                        .output("converted", typeOf(MockKeyValueModel.class))
                        .build())
                .output("out", TestOutput.of("t", MockKeyValueModel.class))
                .connect("in", "op")
                .connect("op.converted", "out"));
    }

    /**
     * self join w/ scatter-gather.
     * @throws Exception if failed
     */
    @Test
    public void self_join_scatter_gather() throws Exception {
        testio.input("in", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello0"));
            o.write(new MockDataModel(1, "Hello1"));
            o.write(new MockDataModel(2, "Hello2"));
        });
        testio.output("out0", MockDataModel.class, o -> {
            assertThat(o, containsInAnyOrder(
                    new MockDataModel(0, "Hello0@Hello0"),
                    new MockDataModel(1, "Hello1@Hello1"),
                    new MockDataModel(2, "Hello2@Hello2")));
        });
        testio.output("out1", MockDataModel.class, o -> {
            assertThat(o, hasSize(0));
        });
        /*
         * [In] -+--> [Join] -> [Out0]
         *        \   |   \
         *         \--/    \--> [Out1]
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("in", MockDataModel.class, DataSize.LARGE))
                .operator("op", Ops.class, "join_self", b -> b
                        .input("mst", typeOf(MockDataModel.class), group("key"))
                        .input("tx", typeOf(MockDataModel.class), group("key"))
                        .output("joined", typeOf(MockDataModel.class))
                        .output("missed", typeOf(MockDataModel.class))
                        .build())
                .output("out0", TestOutput.of("out0", MockDataModel.class))
                .output("out1", TestOutput.of("out1", MockDataModel.class))
                .connect("in", "op.tx", "op.mst")
                .connect("op.joined", "out0")
                .connect("op.missed", "out1"));
    }

    /**
     * self join w/ broadcast.
     * @throws Exception if failed
     */
    @Test
    public void self_join_broadcast() throws Exception {
        testio.input("in", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello0"));
            o.write(new MockDataModel(1, "Hello1"));
            o.write(new MockDataModel(2, "Hello2"));
        });
        testio.output("out0", MockDataModel.class, o -> {
            assertThat(o, containsInAnyOrder(
                    new MockDataModel(0, "Hello0@Hello0"),
                    new MockDataModel(1, "Hello1@Hello1"),
                    new MockDataModel(2, "Hello2@Hello2")));
        });
        testio.output("out1", MockDataModel.class, o -> {
            assertThat(o, hasSize(0));
        });
        /*
         * [In] -+--> [Join] -> [Out0]
         *        \   |   \
         *         \--/    \--> [Out1]
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("in", MockDataModel.class, DataSize.TINY))
                .operator("op", Ops.class, "join_self", b -> b
                        .input("mst", typeOf(MockDataModel.class), group("key"))
                        .input("tx", typeOf(MockDataModel.class), group("key"))
                        .output("joined", typeOf(MockDataModel.class))
                        .output("missed", typeOf(MockDataModel.class))
                        .build())
                .output("out0", TestOutput.of("out0", MockDataModel.class))
                .output("out1", TestOutput.of("out1", MockDataModel.class))
                .connect("in", "op.tx", "op.mst")
                .connect("op.joined", "out0")
                .connect("op.missed", "out1"));
    }

    /**
     * join w/ orphaned master port.
     * @throws Exception if failed
     */
    @Test
    public void orphaned_master_join() throws Exception {
        testio.input("in", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello0"));
            o.write(new MockDataModel(1, "Hello1"));
            o.write(new MockDataModel(2, "Hello2"));
        });
        testio.output("out0", MockDataModel.class, o -> {
            assertThat(o, hasSize(0));
        });
        testio.output("out1", MockDataModel.class, o -> {
            assertThat(o, containsInAnyOrder(
                    new MockDataModel(0, "Hello0"),
                    new MockDataModel(1, "Hello1"),
                    new MockDataModel(2, "Hello2")));
        });
        /*
         * [In] ---> [Join] -> [Out0]
         *            |   \
         *        X|--/    \--> [Out1]
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("in", MockDataModel.class))
                .operator("op", Ops.class, "join_self", b -> b
                        .input("mst", typeOf(MockDataModel.class), group("key"))
                        .input("tx", typeOf(MockDataModel.class), group("key"))
                        .output("joined", typeOf(MockDataModel.class))
                        .output("missed", typeOf(MockDataModel.class))
                        .build())
                .output("out0", TestOutput.of("out0", MockDataModel.class))
                .output("out1", TestOutput.of("out1", MockDataModel.class))
                .connect("in", "op.tx")
                .connect("op.joined", "out0")
                .connect("op.missed", "out1"));
    }

    /**
     * join each other.
     * @throws Exception if failed
     */
    @Test
    public void join_cross() throws Exception {
        testio.input("in0", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello0"));
        });
        testio.input("in1", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello1"));
        });
        testio.output("out0", MockDataModel.class, o -> {
            assertThat(o, hasSize(2));
        });
        testio.output("out1", MockDataModel.class, o -> {
            assertThat(o, hasSize(0));
        });
        /*
         * [In0] --\-/--> [Join0] --\
         *          X                +--> [Out0,1]
         * [In1] --/-\--> [Join1] --/
         *
         */
        run(profile, executor, g -> g
                .input("in0", TestInput.of("in0", MockDataModel.class, DataSize.TINY))
                .input("in1", TestInput.of("in1", MockDataModel.class, DataSize.TINY))
                .operator("op0", Ops.class, "join_self", b -> b
                        .input("mst", typeOf(MockDataModel.class), group("key"))
                        .input("tx", typeOf(MockDataModel.class), group("key"))
                        .output("joined", typeOf(MockDataModel.class))
                        .output("missed", typeOf(MockDataModel.class))
                        .build())
                .operator("op1", Ops.class, "join_self", b -> b
                        .input("mst", typeOf(MockDataModel.class), group("key"))
                        .input("tx", typeOf(MockDataModel.class), group("key"))
                        .output("joined", typeOf(MockDataModel.class))
                        .output("missed", typeOf(MockDataModel.class))
                        .build())
                .output("out0", TestOutput.of("out0", MockDataModel.class))
                .output("out1", TestOutput.of("out1", MockDataModel.class))
                .connect("in0", "op0.tx")
                .connect("in0", "op1.mst")
                .connect("in1", "op1.tx")
                .connect("in1", "op0.mst")
                .connect("op0.joined", "out0")
                .connect("op0.missed", "out1")
                .connect("op1.joined", "out0")
                .connect("op1.missed", "out1"));
    }

    /**
     * join w/ orphaned tx port.
     * @throws Exception if failed
     */
    @Test
    public void orphaned_tx_join() throws Exception {
        testio.input("in", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello0"));
        });
        testio.output("out0", MockDataModel.class, o -> {
            assertThat(o, hasSize(0));
        });
        testio.output("out1", MockDataModel.class, o -> {
            assertThat(o, hasSize(0));
        });
        /*
         *    X|-> [Join] -> [Out0]
         *         |   \
         * [In] ---/    \--> [Out1]
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("in", MockDataModel.class))
                .operator("op", Ops.class, "join_self", b -> b
                        .input("mst", typeOf(MockDataModel.class), group("key"))
                        .input("tx", typeOf(MockDataModel.class), group("key"))
                        .output("joined", typeOf(MockDataModel.class))
                        .output("missed", typeOf(MockDataModel.class))
                        .build())
                .output("out0", TestOutput.of("out0", MockDataModel.class))
                .output("out1", TestOutput.of("out1", MockDataModel.class))
                .connect("in", "op.mst")
                .connect("op.joined", "out0")
                .connect("op.missed", "out1"));
    }

    /**
     * cache conflict (branch).
     * @throws Exception if failed
     */
    @Test
    public void cache_branch() throws Exception {
        testio.input("i0", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "A"));
            o.write(new MockDataModel(1, "B"));
            o.write(new MockDataModel(2, "C"));
        });
        testio.input("i1", MockDataModel.class, o -> {
            o.write(new MockDataModel(3, "A"));
            o.write(new MockDataModel(4, "B"));
            o.write(new MockDataModel(5, "C"));
        });
        testio.output("o0", MockDataModel.class, o -> {
            assertThat(o, hasItem(new MockDataModel(0, "A")));
            assertThat(o, hasItem(new MockDataModel(1, "B")));
            assertThat(o, hasItem(new MockDataModel(3, "A")));
        });
        testio.output("o1", MockDataModel.class, o -> {
            assertThat(o, hasItem(new MockDataModel(2, "C")));
            assertThat(o, hasItem(new MockDataModel(4, "B")));
            assertThat(o, hasItem(new MockDataModel(5, "C")));
        });
        run(profile, executor, g -> g
                .input("i0", TestInput.of("i0", MockDataModel.class))
                .input("i1", TestInput.of("i1", MockDataModel.class))
                .operator("x0", Ops.class, "branch3", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("a", typeOf(MockDataModel.class))
                        .output("b", typeOf(MockDataModel.class))
                        .output("c", typeOf(MockDataModel.class))
                        .build())
                .operator("x1", Ops.class, "branch3", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("a", typeOf(MockDataModel.class))
                        .output("b", typeOf(MockDataModel.class))
                        .output("c", typeOf(MockDataModel.class))
                        .build())
                .output("o0", TestOutput.of("o0", MockDataModel.class))
                .output("o1", TestOutput.of("o1", MockDataModel.class))
                .connect("i0", "x0")
                .connect("i1", "x1")
                .connect("x0.a", "o0")
                .connect("x0.b", "o0")
                .connect("x0.c", "o1")
                .connect("x1.a", "o0")
                .connect("x1.b", "o1")
                .connect("x1.c", "o1"));
    }

    /**
     * cache conflict (extract).
     * @throws Exception if failed
     */
    @Test
    public void cache_extract() throws Exception {
        testio.input("in", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "OK"));
        });
        testio.output("o0", MockDataModel.class, o -> {
            assertThat(o, hasSize(3));
        });
        testio.output("o1", MockDataModel.class, o -> {
            assertThat(o, hasSize(3));
        });
        run(profile, executor, g -> g
                .input("in", TestInput.of("in", MockDataModel.class))
                .operator("x0", Ops.class, "extract3", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("a", typeOf(MockDataModel.class))
                        .output("b", typeOf(MockDataModel.class))
                        .output("c", typeOf(MockDataModel.class))
                        .build())
                .operator("x1", Ops.class, "extract3", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .output("a", typeOf(MockDataModel.class))
                        .output("b", typeOf(MockDataModel.class))
                        .output("c", typeOf(MockDataModel.class))
                        .build())
                .output("o0", TestOutput.of("o0", MockDataModel.class))
                .output("o1", TestOutput.of("o1", MockDataModel.class))
                .connect("in", "x0")
                .connect("in", "x1")
                .connect("x0.a", "o0")
                .connect("x0.b", "o0")
                .connect("x0.c", "o1")
                .connect("x1.a", "o0")
                .connect("x1.b", "o1")
                .connect("x1.c", "o1"));
    }

    /**
     * w/ view.
     * @throws Exception if failed
     */
    @Test
    public void view() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello, world!"));
        });
        testio.input("v", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, BigDecimal.valueOf(2), "x"));
            o.write(new MockDataModel(0, BigDecimal.valueOf(1), "o"));
            o.write(new MockDataModel(0, BigDecimal.valueOf(3), "x"));
            o.write(new MockDataModel(1, BigDecimal.valueOf(0), "x"));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "Hello, world!o")));
        });
        /*
         * [In(t)] -> [Extract] -> [Out]
         *            |
         * [In(v)] ---/
         */
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .input("side", TestInput.of("v", MockDataModel.class))
                .operator("op", Ops.class, "view", b -> b
                        .input("in", typeOf(MockDataModel.class))
                        .input("view", typeOf(MockDataModel.class), c -> c
                                .group(group("=key", "+sort"))
                                .unit(InputUnit.WHOLE))
                        .output("out", typeOf(MockDataModel.class))
                        .build())
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "op.in")
                .connect("side", "op.view")
                .connect("op", "out"));
    }

    /**
     * group w/ view.
     * @throws Exception if failed
     */
    @Test
    public void group_view() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "A"));
            o.write(new MockDataModel(1, "B"));
            o.write(new MockDataModel(2, "C"));
        });
        testio.input("u", MockDataModel.class, o -> {
            Lang.pass();
        });
        testio.input("v", MockDataModel.class, o -> {
            o.write(new MockDataModel(3, "B"));
            o.write(new MockDataModel(4, "C"));
            o.write(new MockDataModel(5, "C"));
            o.write(new MockDataModel(6, "D"));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, containsInAnyOrder(
                    new MockDataModel(0, "A@0"),
                    new MockDataModel(1, "B@1"),
                    new MockDataModel(2, "C@2")));
        });
        /*
         * [In(t)] -+
         *           \
         * [In(t)] ---+-> [CoGroup] -> [Out]
         *                |
         * [In(v)] -------/
         */
        run(profile, executor, g -> g
                .input("in0", TestInput.of("t", MockDataModel.class))
                .input("in1", TestInput.of("u", MockDataModel.class))
                .input("side", TestInput.of("v", MockDataModel.class))
                .operator("op", Ops.class, "group_view", b -> b
                        .input("in0", typeOf(MockDataModel.class), c -> c
                                .group(group("=key", "+sort")))
                        .input("in1", typeOf(MockDataModel.class), c -> c
                                .group(group("=key", "+sort")))
                        .input("view", typeOf(MockDataModel.class), c -> c
                                .group(group("=value"))
                                .unit(InputUnit.WHOLE))
                        .output("out", typeOf(MockDataModel.class))
                        .build())
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in0", "op.in0")
                .connect("in1", "op.in1")
                .connect("side", "op.view")
                .connect("op", "out"));
    }

    private static BigDecimal d(long value) {
        return new BigDecimal(value);
    }

    private static Group group(String... terms) {
        return Groups.parse(terms);
    }

    @SuppressWarnings("javadoc")
    public static class Ops {

        static final AtomicBoolean STICKY = new AtomicBoolean(false);

        @Update
        public void update(MockDataModel model) {
            model.setValue(model.getValue() + "?");
        }

        @Update
        public void parameterized(MockDataModel model, String suffix) {
            model.setValue(model.getValue() + suffix);
        }

        @CoGroup
        public void group(
                @Key(group = "key", order="sort") List<MockDataModel> in,
                Result<MockDataModel> out) {
            int index = 0;
            for (MockDataModel model : in) {
                model.setValue(model.getValue() + "@" + index++);
                out.add(model);
            }
        }

        @MasterJoinUpdate
        public void join(
                @Key(group = "key") MockKeyValueModel mst,
                @Key(group = "key") MockDataModel tx) {
            tx.setValue(tx.getValue() + "@" + mst.getValue());
        }

        @MasterJoinUpdate
        public void join_self(
                @Key(group = "key") MockDataModel mst,
                @Key(group = "key") MockDataModel tx) {
            tx.setValue(tx.getValue() + "@" + mst.getValue());
        }

        @Fold(partialAggregation = PartialAggregation.TOTAL)
        public void aggregate(@Key(group = "key") MockDataModel a, MockDataModel b) {
            a.setSort(a.getSort().add(b.getSort()));
        }

        @Fold(partialAggregation = PartialAggregation.PARTIAL)
        public void preaggregate(@Key(group = "key") MockDataModel a, MockDataModel b) {
            a.setSort(a.getSort().add(b.getSort()));
        }

        //@Sticky
        @Update
        public void sticky(MockDataModel model) {
            Lang.pass(model);
            STICKY.set(true);
        }

        private final MockKeyValueModel kv = new MockKeyValueModel();

        @Convert
        public MockKeyValueModel convert(MockDataModel model) {
            kv.setKey(model.getKey());
            kv.setValue(model.getValue());
            return kv;
        }

        @Extract
        public void extract3(MockDataModel model,
                Result<MockDataModel> a, Result<MockDataModel> b, Result<MockDataModel> c) {
            a.add(model);
            b.add(model);
            c.add(model);
        }

        @Branch
        public Trinary branch3(MockDataModel model) {
            return Trinary.valueOf(model.getValue());
        }

        @Update
        public void view(MockDataModel model,
                @Key(group = "key", order="sort") GroupView<MockDataModel> table) {
            model.setValue(model.getValue() + table.find(model.getKeyOption()).get(0).getValue());
        }

        @CoGroup
        public void group_view(
                @Key(group = "key", order="sort") List<MockDataModel> in0,
                @Key(group = "key", order="sort") List<MockDataModel> in1,
                @Key(group = "value") GroupView<MockDataModel> table,
                Result<MockDataModel> out) {
            Lang.pass(in1);
            for (MockDataModel model : in0) {
                model.setValue(model.getValue() + "@" + table.find(model.getValueOption()).size());
                out.add(model);
            }
        }
    }

    @SuppressWarnings("javadoc")
    public enum Trinary {
        A, B, C,
    }
}
