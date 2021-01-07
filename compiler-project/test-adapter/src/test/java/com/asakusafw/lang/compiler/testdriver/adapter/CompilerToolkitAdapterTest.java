/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.testdriver.adapter;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessor;
import com.asakusafw.lang.compiler.internalio.InternalExporterDescription;
import com.asakusafw.lang.compiler.internalio.InternalImporterDescription;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.MockData;
import com.asakusafw.lang.compiler.testdriver.adapter.mock.SimpleBatch;
import com.asakusafw.lang.compiler.testdriver.adapter.mock.SimpleInputDescription;
import com.asakusafw.lang.compiler.testdriver.adapter.mock.SimpleJobflow;
import com.asakusafw.lang.compiler.testdriver.adapter.mock.SimpleOutputDescription;
import com.asakusafw.testdriver.compiler.ArtifactMirror;
import com.asakusafw.testdriver.compiler.BatchMirror;
import com.asakusafw.testdriver.compiler.CompilerConfiguration;
import com.asakusafw.testdriver.compiler.CompilerConstants;
import com.asakusafw.testdriver.compiler.CompilerSession;
import com.asakusafw.testdriver.compiler.FlowPortMap;
import com.asakusafw.testdriver.compiler.JobflowMirror;
import com.asakusafw.testdriver.compiler.PortMirror;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.Out;
import com.asakusafw.vocabulary.flow.testing.MockIn;
import com.asakusafw.vocabulary.flow.testing.MockOut;

/**
 * Test for {@link CompilerToolkitAdapter}.
 */
public class CompilerToolkitAdapterTest {

    /**
     * working directories.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * flow-part.
     * @throws Exception if failed
     */
    @Test
    public void flowpart() throws Exception {
        CompilerToolkitAdapter tk = new CompilerToolkitAdapter();

        FlowPortMap ports = tk.newFlowPortMap();
        In<MockData> in = ports.addInput("in", MockData.class);
        Out<MockData> out = ports.addOutput("out", MockData.class);
        SimpleJobflow flow = new SimpleJobflow(in, out);

        CompilerConfiguration conf = getConf(tk);
        try (CompilerSession session = tk.newSession(conf)) {
            ArtifactMirror artifact = session.compileFlow(flow, ports);
            check(artifact);
            JobflowMirror single = single(artifact.getBatch());

            assertThat(single.getInputs(), hasSize(1));
            PortMirror<? extends ImporterDescription> i0 = single.findInput("in");
            assertThat(i0, is(notNullValue()));
            assertThat(i0.getDescription(), is(instanceOf(InternalImporterDescription.class)));

            assertThat(single.getOutputs(), hasSize(1));
            PortMirror<? extends ExporterDescription> o0 = single.findOutput("out");
            assertThat(o0, is(notNullValue()));
            assertThat(o0.getDescription(), is(instanceOf(InternalExporterDescription.class)));
        }
    }

    /**
     * flow-part w/ invalid inputs.
     * @throws Exception if failed
     */
    @Test(expected = UnsupportedOperationException.class)
    public void flowpart_invalid_input() throws Exception {
        CompilerToolkitAdapter tk = new CompilerToolkitAdapter();

        FlowPortMap ports = tk.newFlowPortMap();
        In<MockData> in = new MockIn<>(MockData.class, "in");
        Out<MockData> out = ports.addOutput("out", MockData.class);
        SimpleJobflow flow = new SimpleJobflow(in, out);

        try (CompilerSession session = tk.newSession(getConf(tk))) {
            session.compileFlow(flow, ports);
        }
    }

    /**
     * flow-part w/ invalid outputs.
     * @throws Exception if failed
     */
    @Test(expected = UnsupportedOperationException.class)
    public void flowpart_invalid_output() throws Exception {
        CompilerToolkitAdapter tk = new CompilerToolkitAdapter();

        FlowPortMap ports = tk.newFlowPortMap();
        In<MockData> in = ports.addInput("in", MockData.class);
        Out<MockData> out = new MockOut<>(MockData.class, "out");
        SimpleJobflow flow = new SimpleJobflow(in, out);

        try (CompilerSession session = tk.newSession(getConf(tk))) {
            session.compileFlow(flow, ports);
        }
    }

    /**
     * jobflow.
     * @throws Exception if failed
     */
    @Test
    public void jobflow() throws Exception {
        CompilerToolkitAdapter tk = new CompilerToolkitAdapter();
        CompilerConfiguration conf = getConf(tk);
        try (CompilerSession session = tk.newSession(conf)) {
            ArtifactMirror artifact = session.compileJobflow(SimpleJobflow.class);
            check(artifact);
            JobflowMirror single = single(artifact.getBatch());
            checkSimpleJobflow(single);
        }
    }

    /**
     * batch.
     * @throws Exception if failed
     */
    @Test
    public void batch() throws Exception {
        CompilerToolkitAdapter tk = new CompilerToolkitAdapter();
        CompilerConfiguration conf = getConf(tk);
        try (CompilerSession session = tk.newSession(conf)) {
            ArtifactMirror artifact = session.compileBatch(SimpleBatch.class);
            check(artifact);
            assertThat(artifact.getBatch().getId(), is("batch.simple"));

            JobflowMirror single = single(artifact.getBatch());
            checkSimpleJobflow(single);
        }
    }

    private void checkSimpleJobflow(JobflowMirror single) {
        assertThat(single.getId(), is("simple"));

        assertThat(single.getInputs(), hasSize(1));
        PortMirror<? extends ImporterDescription> i0 = single.findInput("IN");
        assertThat(i0, is(notNullValue()));
        assertThat(i0.getDescription(), is(instanceOf(SimpleInputDescription.class)));

        assertThat(single.getOutputs(), hasSize(1));
        PortMirror<? extends ExporterDescription> o0 = single.findOutput("OUT");
        assertThat(o0, is(notNullValue()));
        assertThat(o0.getDescription(), is(instanceOf(SimpleOutputDescription.class)));
    }

    private CompilerConfiguration getConf(CompilerToolkitAdapter tk) throws IOException {
        CompilerConfiguration conf = tk.newConfiguration();
        conf.withClassLoader(getClass().getClassLoader());
        conf.withWorkingDirectory(temporary.newFolder());
        ((CompilerConfigurationAdapter) conf).withEdit(profile -> profile
                .forToolRepository()
                .use(new MockJobflowProcessor()));
        return conf;
    }

    private static JobflowMirror single(BatchMirror batch) {
        assertThat(batch.getElements(), hasSize(1));
        return batch.getElements().iterator().next();
    }

    private void check(ArtifactMirror artifact) {
        File batchDir = artifact.getContents();
        assertThat(batchDir.isDirectory(), is(true));
        for (JobflowMirror jobflow : artifact.getBatch().getElements()) {
            File jobflowLib = CompilerConstants.getJobflowLibraryPath(batchDir, jobflow.getId());
            assertThat(jobflowLib.isFile(), is(true));
        }
    }
}
