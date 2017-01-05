/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.core.basic;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.dummy.SimpleBatchProcessor;
import com.asakusafw.lang.compiler.core.dummy.SimpleCompilerParticipant;
import com.asakusafw.lang.compiler.core.dummy.SimpleJobflowProcessor;
import com.asakusafw.lang.compiler.core.util.BatchReferenceCollector;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.graph.BatchElement;
import com.asakusafw.lang.compiler.packaging.FileContainer;

/**
 * Test for {@link BasicBatchCompiler}.
 */
public class BasicBatchCompilerTest extends CompilerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        batchProcessors.add(new SimpleBatchProcessor());
        jobflowProcessors.add(new SimpleJobflowProcessor());
        compilerParticipants.add(new BatchReferenceCollector());

        Batch batch = new Batch(batchInfo("testing"));
        batch.addElement(jobflow("j0"));

        FileContainer output = container();
        BatchCompiler.Context context = new BatchCompiler.Context(context(true), output);
        new BasicBatchCompiler().compile(context, batch);

        assertThat(SimpleBatchProcessor.contains(context), is(true));
        assertThat(SimpleJobflowProcessor.contains(context, "j0"), is(true));
        assertThat(SimpleCompilerParticipant.contains(context), is(false));

        BatchReference result = BatchReferenceCollector.get(context);
        assertThat(result.find("j0"), is(notNullValue()));
    }

    /**
     * diamond.
     */
    @Test
    public void diamond() {
        batchProcessors.add(new SimpleBatchProcessor());
        jobflowProcessors.add(new SimpleJobflowProcessor());
        compilerParticipants.add(new BatchReferenceCollector());

        Batch batch = new Batch(batchInfo("testing"));
        BatchElement bj0 = batch.addElement(jobflow("j0"));
        BatchElement bj1 = batch.addElement(jobflow("j1"));
        BatchElement bj2 = batch.addElement(jobflow("j2"));
        BatchElement bj3 = batch.addElement(jobflow("j3"));
        bj1.addBlockerElement(bj0);
        bj2.addBlockerElement(bj0);
        bj3.addBlockerElement(bj1);
        bj3.addBlockerElement(bj2);

        FileContainer output = container();
        BatchCompiler.Context context = new BatchCompiler.Context(context(true), output);
        new BasicBatchCompiler().compile(context, batch);

        assertThat(SimpleBatchProcessor.contains(context), is(true));
        assertThat(SimpleJobflowProcessor.contains(context, "j0"), is(true));
        assertThat(SimpleJobflowProcessor.contains(context, "j1"), is(true));
        assertThat(SimpleJobflowProcessor.contains(context, "j2"), is(true));
        assertThat(SimpleJobflowProcessor.contains(context, "j3"), is(true));
        assertThat(SimpleCompilerParticipant.contains(context), is(false));

        BatchReference result = BatchReferenceCollector.get(context);
        JobflowReference rj0 = result.find("j0");
        JobflowReference rj1 = result.find("j1");
        JobflowReference rj2 = result.find("j2");
        JobflowReference rj3 = result.find("j3");
        assertThat(rj0.getBlockers(), is(empty()));
        assertThat(rj1.getBlockers(), containsInAnyOrder(rj0));
        assertThat(rj2.getBlockers(), containsInAnyOrder(rj0));
        assertThat(rj3.getBlockers(), containsInAnyOrder(rj1, rj2));
    }

    /**
     * w/ compiler participants.
     */
    @Test
    public void participants() {
        batchProcessors.add(new SimpleBatchProcessor());
        jobflowProcessors.add(new SimpleJobflowProcessor());
        compilerParticipants.add(new SimpleCompilerParticipant());
        compilerParticipants.add(new BatchReferenceCollector());

        Batch batch = new Batch(batchInfo("testing"));
        batch.addElement(jobflow("j0"));

        FileContainer output = container();
        BatchCompiler.Context context = new BatchCompiler.Context(context(true), output);
        new BasicBatchCompiler().compile(context, batch);

        assertThat(SimpleBatchProcessor.contains(context), is(true));
        assertThat(SimpleJobflowProcessor.contains(context, "j0"), is(true));
        assertThat(SimpleCompilerParticipant.contains(context), is(true));
        assertThat(context.getExtension(BatchReference.class), is(notNullValue()));
    }
}
