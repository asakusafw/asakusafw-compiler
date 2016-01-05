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
package com.asakusafw.lang.compiler.core.participant;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.lang.compiler.common.testing.FileDeployer;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.basic.BasicBatchCompiler;
import com.asakusafw.lang.compiler.core.dummy.SimpleBatchProcessor;
import com.asakusafw.lang.compiler.core.dummy.SimpleJobflowProcessor;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.packaging.FileContainer;

/**
 * Test for {@link AttachedLibrariesParticipant}.
 */
public class AttachedLibrariesParticipantTest extends CompilerTestRoot {

    /**
     * deployer.
     */
    @Rule
    public final FileDeployer deployer = new FileDeployer();

    /**
     * simple case.
     */
    @Test
    public void simple() {
        batchProcessors.add(new SimpleBatchProcessor());
        jobflowProcessors.add(new SimpleJobflowProcessor());
        compilerParticipants.add(new AttachedLibrariesParticipant());

        Batch batch = new Batch(batchInfo("testing"));
        batch.addElement(jobflow("j0"));

        FileContainer output = container();
        BatchCompiler.Context context = new BatchCompiler.Context(context(true), output);
        new BasicBatchCompiler().compile(context, batch);

        assertThat(collect(context), hasSize(0));
    }

    /**
     * w/ attached.
     */
    @Test
    public void file() {
        batchProcessors.add(new SimpleBatchProcessor());
        jobflowProcessors.add(new SimpleJobflowProcessor());
        compilerParticipants.add(new AttachedLibrariesParticipant());
        attached.add(deployer.copy("example.jar", "a.jar"));

        Batch batch = new Batch(batchInfo("testing"));
        batch.addElement(jobflow("j0"));

        FileContainer output = container();
        BatchCompiler.Context context = new BatchCompiler.Context(context(true), output);
        new BasicBatchCompiler().compile(context, batch);

        assertThat(collect(context), hasSize(1));
    }

    /**
     * w/ multiple files.
     */
    @Test
    public void multiple_file() {
        batchProcessors.add(new SimpleBatchProcessor());
        jobflowProcessors.add(new SimpleJobflowProcessor());
        compilerParticipants.add(new AttachedLibrariesParticipant());
        attached.add(deployer.copy("example.jar", "a.jar"));
        attached.add(deployer.copy("example.jar", "b.jar"));
        attached.add(deployer.copy("example.jar", "c.jar"));

        Batch batch = new Batch(batchInfo("testing"));
        batch.addElement(jobflow("j0"));

        FileContainer output = container();
        BatchCompiler.Context context = new BatchCompiler.Context(context(true), output);
        new BasicBatchCompiler().compile(context, batch);

        assertThat(collect(context), hasSize(3));
    }

    /**
     * w/ multiple files.
     */
    @Test
    public void conflict() {
        batchProcessors.add(new SimpleBatchProcessor());
        jobflowProcessors.add(new SimpleJobflowProcessor());
        compilerParticipants.add(new AttachedLibrariesParticipant());
        attached.add(deployer.copy("example.jar", "1/a.jar"));
        attached.add(deployer.copy("example.jar", "2/a.jar"));
        attached.add(deployer.copy("example.jar", "3/a.jar"));

        Batch batch = new Batch(batchInfo("testing"));
        batch.addElement(jobflow("j0"));

        FileContainer output = container();
        BatchCompiler.Context context = new BatchCompiler.Context(context(true), output);
        new BasicBatchCompiler().compile(context, batch);

        assertThat(collect(context), hasSize(3));
    }

    private Set<File> collect(BatchCompiler.Context context) {
        File dir = context.getOutput().toFile(AttachedLibrariesParticipant.LOCATION);
        if (dir.exists() == false) {
            return Collections.emptySet();
        }
        Set<File> results = new LinkedHashSet<>();
        assertThat(dir.isDirectory(), is(true));
        for (File file : dir.listFiles()) {
            if (file.getName().startsWith(".") == false) {
                results.add(file);
            }
        }
        return results;
    }
}
