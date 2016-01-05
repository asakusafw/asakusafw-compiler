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
package com.asakusafw.lang.compiler.core.basic;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.dummy.SimpleCompilerParticipant;
import com.asakusafw.lang.compiler.core.dummy.SimpleExternalPortProcessor;
import com.asakusafw.lang.compiler.core.dummy.SimpleJobflowProcessor;
import com.asakusafw.lang.compiler.packaging.FileContainer;

/**
 * Test for {@link BasicJobflowCompiler}.
 */
public class BasicJobflowCompilerTest extends CompilerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        jobflowProcessors.add(new SimpleJobflowProcessor());
        externalPortProcessors.add(new SimpleExternalPortProcessor());

        FileContainer output = container();
        JobflowCompiler.Context context = new JobflowCompiler.Context(context(true), output);
        new BasicJobflowCompiler().compile(
                context,
                batchInfo("b"),
                jobflow("testing"));

        assertThat(SimpleJobflowProcessor.contains(context), is(true));
        assertThat(SimpleExternalPortProcessor.contains(context), is(false));
        assertThat(SimpleCompilerParticipant.contains(context), is(false));
    }

    /**
     * w/ external ports.
     */
    @Test
    public void external_ports() {
        jobflowProcessors.add(new SimpleJobflowProcessor().withUseExternalPort(true));
        externalPortProcessors.add(new SimpleExternalPortProcessor());

        FileContainer output = container();
        JobflowCompiler.Context context = new JobflowCompiler.Context(context(true), output);
        new BasicJobflowCompiler().compile(
                context,
                batchInfo("b"),
                jobflow("testing"));

        assertThat(SimpleJobflowProcessor.contains(context), is(true));
        assertThat(SimpleExternalPortProcessor.contains(context), is(true));
        assertThat(SimpleCompilerParticipant.contains(context), is(false));
    }

    /**
     * w/ compiler participants.
     */
    @Test
    public void participants() {
        jobflowProcessors.add(new SimpleJobflowProcessor());
        externalPortProcessors.add(new SimpleExternalPortProcessor());
        compilerParticipants.add(new SimpleCompilerParticipant());

        FileContainer output = container();
        JobflowCompiler.Context context = new JobflowCompiler.Context(context(true), output);
        new BasicJobflowCompiler().compile(
                context,
                batchInfo("b"),
                jobflow("testing"));

        assertThat(SimpleJobflowProcessor.contains(context), is(true));
        assertThat(SimpleExternalPortProcessor.contains(context), is(false));
        assertThat(SimpleCompilerParticipant.contains(context), is(true));
    }
}
