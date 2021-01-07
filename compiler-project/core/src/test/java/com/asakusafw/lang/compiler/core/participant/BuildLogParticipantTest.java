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
package com.asakusafw.lang.compiler.core.participant;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;

import org.junit.Test;

import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.basic.BasicBatchCompiler;
import com.asakusafw.lang.compiler.core.dummy.SimpleExternalPortProcessor;
import com.asakusafw.lang.compiler.core.dummy.SimpleJobflowProcessor;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.packaging.FileContainer;

/**
 * Test for {@link BuildLogParticipant}.
 */
public class BuildLogParticipantTest extends CompilerTestRoot {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        jobflowProcessors.add(new SimpleJobflowProcessor());
        externalPortProcessors.add(new SimpleExternalPortProcessor());
        compilerParticipants.add(new BuildLogParticipant());

        Batch batch = new Batch(batchInfo("testing"));
        batch.addElement(jobflow("j0"));

        FileContainer output = container();
        BatchCompiler.Context context = new BatchCompiler.Context(context(true), output);
        new BasicBatchCompiler().compile(context, batch);

        File file = context.getOutput().toFile(BuildLogParticipant.LOCATION);
        assertThat(file.exists(), is(true));
    }
}