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

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.basic.BasicJobflowCompiler;
import com.asakusafw.lang.compiler.core.dummy.SimpleExternalPortProcessor;
import com.asakusafw.lang.compiler.hadoop.HadoopFormatExtension;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.packaging.FileContainer;

/**
 * Test for {@link HadoopFormatExtensionParticipant}.
 */
public class HadoopFormatExtensionParticipantTest extends CompilerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        AtomicBoolean executed = new AtomicBoolean();
        externalPortProcessors.add(new SimpleExternalPortProcessor());
        compilerParticipants.add(new HadoopFormatExtensionParticipant());
        jobflowProcessors.add((context, source) -> {
            HadoopFormatExtension extension = context.getExtension(HadoopFormatExtension.class);
            assertThat(extension, is(notNullValue()));

            assertThat(extension.getInputFormat(), is(HadoopFormatExtension.DEFAULT_INPUT_FORMAT));
            assertThat(extension.getOutputFormat(), is(HadoopFormatExtension.DEFAULT_OUTPUT_FORMAT));

            executed.set(true);
        });

        FileContainer output = container();
        JobflowCompiler.Context context = new JobflowCompiler.Context(context(true), output);
        new BasicJobflowCompiler().compile(
                context,
                batchInfo("b"),
                jobflow("testing"));

        assertThat(executed.get(), is(true));
    }

    /**
     * w/ custom formats.
     */
    @Test
    public void custom() {
        options
            .withProperty(HadoopFormatExtensionParticipant.KEY_INPUT_FORMAT, "TestingInput")
            .withProperty(HadoopFormatExtensionParticipant.KEY_OUTPUT_FORMAT, "TestingOutput");

        AtomicBoolean executed = new AtomicBoolean();
        externalPortProcessors.add(new SimpleExternalPortProcessor());
        compilerParticipants.add(new HadoopFormatExtensionParticipant());
        jobflowProcessors.add((context, source) -> {
            HadoopFormatExtension extension = context.getExtension(HadoopFormatExtension.class);
            assertThat(extension, is(notNullValue()));

            assertThat(extension.getInputFormat(), is(new ClassDescription("TestingInput")));
            assertThat(extension.getOutputFormat(), is(new ClassDescription("TestingOutput")));

            executed.set(true);
        });

        FileContainer output = container();
        JobflowCompiler.Context context = new JobflowCompiler.Context(context(true), output);
        new BasicJobflowCompiler().compile(
                context,
                batchInfo("b"),
                jobflow("testing"));

        assertThat(executed.get(), is(true));
    }
}
