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
package com.asakusafw.lang.compiler.tester;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReferenceMap;
import com.asakusafw.lang.compiler.api.testing.MockBatchProcessor;
import com.asakusafw.lang.compiler.api.testing.MockDataModelProcessor;
import com.asakusafw.lang.compiler.api.testing.MockExternalPortProcessor;
import com.asakusafw.lang.compiler.api.testing.MockImporterDescription;
import com.asakusafw.lang.compiler.api.testing.MockJobflowProcessor;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.testing.FileEditor;
import com.asakusafw.lang.compiler.core.CompilerParticipant;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.packaging.ByteArrayItem;
import com.asakusafw.lang.compiler.packaging.ResourceItem;
import com.asakusafw.lang.compiler.tester.executor.DummyTaskExecutor;
import com.asakusafw.lang.compiler.tester.executor.JobflowExecutor;

/**
 * Test for {@link CompilerProfile} and {@link CompilerTester}.
 */
public class CompilerProfileTest {

    private static final Charset ENCODING = StandardCharsets.UTF_8;
    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        DummyTaskExecutor tracker = new DummyTaskExecutor();
        JobflowExecutor executor = new JobflowExecutor(Collections.singleton(tracker));

        CompilerProfile profile = newProfile();
        profile.forToolRepository()
            .use((JobflowProcessor) (context, source) -> context.addTask(
                    "testing", "testing",
                    Location.of("testing"), Collections.emptyList()));

        File home;
        File batchapps;
        try (CompilerTester compiler = profile.build()) {
            home = compiler.getTesterContext().getFrameworkHome();
            batchapps = compiler.getTesterContext().getBatchApplicationHome();
            assertThat(home.isDirectory(), is(true));
            assertThat(batchapps.isDirectory(), is(true));

            JobflowArtifact artifact = compiler.compile(jobflow("testing"));
            assertThat(artifact.getReference().getFlowId(), is("testing"));
            assertThat(flatten(artifact.getReference()), hasSize(1));
            assertThat(artifact.getExternalPorts().getInputs(), hasSize(1));
            assertThat(artifact.getExternalPorts().getOutputs(), hasSize(1));

            executor.execute(compiler.getTesterContext(), artifact);
            assertThat(tracker.getTasks(), hasSize(1));
        }

        // disposed
        assertThat(home.isDirectory(), is(false));
        assertThat(batchapps.isDirectory(), is(false));
    }

    /**
     * w/ environment variables.
     * @throws Exception if failed
     */
    @Test
    public void environment_variables() throws Exception {
        CompilerProfile profile = newProfile();
        profile.withEnvironmentVariables(Collections.singletonMap("a", "A"));
        profile.withEnvironmentVariables(Collections.singletonMap("b", "B"));
        profile.withEnvironmentVariables(Collections.singletonMap("c", "C"));
        try (CompilerTester compiler = profile.build()) {
            Map<String, String> env = compiler.getTesterContext().getEnvironmentVariables();
            assertThat(env, hasKey("ASAKUSA_HOME"));
            assertThat(env, hasKey("ASAKUSA_BATCHAPPS_HOME"));
            assertThat(env, hasEntry("a", "A"));
            assertThat(env, hasEntry("b", "B"));
            assertThat(env, hasEntry("c", "C"));
        }
    }

    /**
     * w/ volatile framework installation.
     * @throws Exception if failed
     */
    @Test
    public void volatile_framework() throws Exception {
        CompilerProfile profile = newProfile();
        profile.forFrameworkInstallation()
            .add(item("TESTING", "Hello, world!"));

        try (CompilerTester compiler = profile.build()) {
            File home = compiler.getTesterContext().getFrameworkHome();
            assertThat(FileEditor.get(new File(home, "TESTING")), contains("Hello, world!"));
        }
    }

    /**
     * w/ explicit framework installation.
     * @throws Exception if failed
     */
    @Test
    public void explicit_framework() throws Exception {
        File file = new File(folder.getRoot(), "TESTING");
        FileEditor.put(file, "Hello, world!");

        CompilerProfile profile = newProfile();
        profile.withFrameworkInstallation(folder.getRoot());
        profile.forFrameworkInstallation()
            .add(item("EXTRA", "Hello, world!"));

        try (CompilerTester compiler = profile.build()) {
            File home = compiler.getTesterContext().getFrameworkHome();
            assertThat(FileEditor.get(new File(home, "TESTING")), contains("Hello, world!"));
            assertThat(FileEditor.get(new File(home, "EXTRA")), contains("Hello, world!"));
        }

        assertThat("still exists", file.isFile(), is(true));
        assertThat("still exists", new File(folder.getRoot(), "EXTRA").isFile(), is(true));
    }

    /**
     * w/o framework installation.
     * @throws Exception if failed
     */
    @Test
    public void disable_framework() throws Exception {
        CompilerProfile profile = newProfile();
        profile.withFrameworkInstallation(null);
        profile.forFrameworkInstallation()
            .add(item("TESTING", "Hello, world!"));

        try (CompilerTester compiler = profile.build()) {
            File home = compiler.getTesterContext().getFrameworkHome();
            assertThat(new File(home, "TESTING").exists(), is(false));
        }
    }

    /**
     * w/ edit.
     * @throws Exception if failed
     */
    @Test
    public void edit() throws Exception {
        CompilerProfile profile = newProfile();
        profile.apply(p -> p.withEnvironmentVariables(Collections.singletonMap("a", "A")));
        try (CompilerTester compiler = profile.build()) {
            Map<String, String> env = compiler.getTesterContext().getEnvironmentVariables();
            assertThat(env, hasEntry("a", "A"));
        }
    }

    /**
     * copy artifacts.
     * @throws Exception if failed
     */
    @Test
    public void copy() throws Exception {
        CompilerProfile profile = newProfile();
        profile.forToolRepository()
            .use((JobflowProcessor) (context, source) -> {
                return;
            });

        File copy = new File(folder.getRoot(), "copy");
        File home;
        File batchapps;
        try (CompilerTester compiler = profile.build()) {
            home = compiler.getTesterContext().getFrameworkHome();
            batchapps = compiler.getTesterContext().getBatchApplicationHome();
            assertThat(home.isDirectory(), is(true));
            assertThat(batchapps.isDirectory(), is(true));

            JobflowArtifact artifact = compiler.compile("B", jobflow("testing"));
            compiler.copyArtifact(artifact, copy);
        }

        // disposed
        assertThat(home.isDirectory(), is(false));
        assertThat(batchapps.isDirectory(), is(false));
        assertThat(new File(copy, "B").isDirectory(), is(true));
    }

    private CompilerProfile newProfile(CompilerParticipant... participants) {
        CompilerProfile profile = new CompilerProfile(getClass().getClassLoader());
        profile.forToolRepository()
            .use(new MockDataModelProcessor())
            .use(new MockExternalPortProcessor())
            .use(new MockBatchProcessor())
            .use(new MockJobflowProcessor());
        for (CompilerParticipant participant : participants) {
            profile.forToolRepository().use(participant);
        }
        return profile;
    }

    private Jobflow jobflow(String id) {
        ExternalInput in = ExternalInput.newInstance("in", new ExternalInputInfo.Basic(
                Descriptions.classOf(MockImporterDescription.class),
                "simple",
                Descriptions.classOf(String.class),
                ExternalInputInfo.DataSize.UNKNOWN));
        ExternalOutput out = ExternalOutput.newInstance("out", new ExternalOutputInfo.Basic(
                Descriptions.classOf(MockImporterDescription.class),
                "simple",
                Descriptions.classOf(String.class)));
        in.getOperatorPort().connect(out.getOperatorPort());
        return new Jobflow(id, new ClassDescription(id), new OperatorGraph().add(in).add(out));
    }

    private List<TaskReference> flatten(TaskReferenceMap tasks) {
        List<TaskReference> results = new ArrayList<>();
        for (TaskReference.Phase phase : TaskReference.Phase.values()) {
            results.addAll(tasks.getTasks(phase));
        }
        return results;
    }

    private ResourceItem item(String path, String contents) {
        return new ByteArrayItem(Location.of(path), contents.getBytes(ENCODING));
    }
}
