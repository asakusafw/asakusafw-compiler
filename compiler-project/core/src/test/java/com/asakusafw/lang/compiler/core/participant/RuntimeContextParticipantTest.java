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
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.junit.Test;

import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.basic.BasicJobflowCompiler;
import com.asakusafw.lang.compiler.core.dummy.SimpleExternalPortProcessor;
import com.asakusafw.lang.compiler.core.dummy.SimpleJobflowProcessor;
import com.asakusafw.lang.compiler.packaging.FileContainer;
import com.asakusafw.runtime.core.context.RuntimeContext;

/**
 * Test for {@link RuntimeContextParticipant}.
 */
public class RuntimeContextParticipantTest extends CompilerTestRoot {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        jobflowProcessors.add(new SimpleJobflowProcessor());
        externalPortProcessors.add(new SimpleExternalPortProcessor());
        compilerParticipants.add(new RuntimeContextParticipant());

        FileContainer output = container();
        JobflowCompiler.Context context = new JobflowCompiler.Context(context(true), output);
        new BasicJobflowCompiler().compile(
                context,
                batchInfo("b"),
                jobflow("f"));

        File file = context.getOutput().toFile(RuntimeContextParticipant.LOCATION);
        assertThat(file.exists(), is(true));
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            properties.load(in);
        }

        assertThat(properties.getProperty(RuntimeContext.KEY_BATCH_ID), is("b"));
        assertThat(properties.getProperty(RuntimeContext.KEY_FLOW_ID), is("f"));
        assertThat(properties.getProperty(RuntimeContext.KEY_BUILD_ID), is(notNullValue()));
        assertThat(properties.getProperty(RuntimeContext.KEY_BUILD_DATE), is(notNullValue()));
        assertThat(properties.getProperty(RuntimeContext.KEY_RUNTIME_VERSION), is(notNullValue()));
    }
}
