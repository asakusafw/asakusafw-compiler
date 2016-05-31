/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.hive.testing;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.asakusafw.lang.compiler.analyzer.ExternalPortAnalyzer;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.adapter.ExternalPortAnalyzerAdapter;
import com.asakusafw.lang.compiler.core.basic.JobflowPackager;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.OperatorGraph;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.packaging.FileContainer;
import com.asakusafw.lang.compiler.tester.CompilerProfile;
import com.asakusafw.lang.compiler.tester.CompilerTester;
import com.asakusafw.lang.compiler.tester.executor.util.DummyBatchClass;
import com.asakusafw.lang.compiler.tester.executor.util.DummyJobflowClass;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * Test root for this project.
 */
public class HiveSchemaProcessorTester implements TestRule {

    private final AtomicInteger counter = new AtomicInteger();

    private final OperatorGraph graph = new OperatorGraph();

    CompilerTester tester;

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                CompilerProfile profile = new CompilerProfile(description.getTestClass().getClassLoader());
                profile.forToolRepository().useDefaults();
                profile.withFrameworkInstallation(null);
                try (CompilerTester t = profile.build()) {
                    tester = t;
                    base.evaluate();
                } finally {
                    tester = null;
                }
            }
        };
    }

    /**
     * Adds a pair of I/O.
     * @param input input port
     * @param output output port
     * @return this
     */
    public HiveSchemaProcessorTester add(ImporterDescription input, ExporterDescription output) {
        int count = counter.incrementAndGet();
        String inputId = "i" + count;
        String outputId = "o" + count;

        ExternalPortAnalyzer analyzer = new ExternalPortAnalyzerAdapter(tester.getCompilerContext());
        ExternalInput inputOperator = ExternalInput.newInstance(inputId, analyzer.analyze(inputId, input));
        ExternalOutput outputOperator = ExternalOutput.newInstance(outputId, analyzer.analyze(outputId, output));
        inputOperator.getOperatorPort().connect(outputOperator.getOperatorPort());
        graph.add(inputOperator);
        graph.add(outputOperator);
        return this;
    }

    /**
     * Compiles the current graph.
     * @return this
     * @throws IOException if failed
     */
    public HiveSchemaProcessorTester compile() throws IOException {
        Batch batch = new Batch(new BatchInfo.Basic(
                DummyBatchClass.ID,
                Descriptions.classOf(DummyBatchClass.class)));
        batch.addElement(new Jobflow(
                DummyJobflowClass.ID,
                Descriptions.classOf(DummyJobflowClass.class),
                graph));
        tester.compile(batch);
        return this;
    }

    private FileContainer root() {
        File home = tester.getTesterContext().getBatchApplicationHome();
        FileContainer batch = new FileContainer(new File(home, DummyBatchClass.ID));
        return batch;
    }

    /**
     * Returns a batch resource.
     * @param location resource location
     * @return the batch resource
     */
    public File get(Location location) {
        File resource = root().toFile(location);
        assertThat(resource.getPath(), resource.isFile(), is(true));
        return resource;
    }

    /**
     * Returns the jobflow package.
     * @return the jobflow package
     */
    public File getJobflow() {
        return get(JobflowPackager.getLibraryLocation(DummyJobflowClass.ID));
    }
}
