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

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.basic.BasicJobflowCompiler;
import com.asakusafw.lang.compiler.core.dummy.DummyImporterDescription;
import com.asakusafw.lang.compiler.core.dummy.SimpleExternalPortProcessor;
import com.asakusafw.lang.compiler.hadoop.InputFormatInfo;
import com.asakusafw.lang.compiler.hadoop.InputFormatInfoExtension;
import com.asakusafw.lang.compiler.hadoop.InputFormatInfoSupport;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.packaging.FileContainer;

/**
 * Test for {@link InputFormatInfoExtensionParticipant}.
 */
public class InputFormatInfoExtensionParticipantTest extends CompilerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        AtomicBoolean saw = new AtomicBoolean();

        List<?> adapters = Arrays.asList((InputFormatInfoSupport) (context, name, info) -> {
            Map<String, String> conf = new LinkedHashMap<>();
            conf.put("testing", "ok");
            return new InputFormatInfo(
                    classOf(String.class),
                    classOf(NullWritable.class),
                    classOf(Text.class), conf);
        });
        externalPortProcessors.add(new SimpleExternalPortProcessor(adapters));
        compilerParticipants.add(new InputFormatInfoExtensionParticipant());
        jobflowProcessors.add((context, source) -> {
            ExternalInputInfo info = SimpleExternalPortProcessor.createInput(classOf(DummyImporterDescription.class));
            InputFormatInfo resolved = InputFormatInfoExtension.resolve(context, "testing", info);
            assertThat(resolved, is(notNullValue()));
            assertThat(resolved.getExtraConfiguration(), hasEntry("testing", "ok"));
            saw.set(true);
        });

        FileContainer output = container();
        JobflowCompiler.Context context = new JobflowCompiler.Context(context(true), output);
        new BasicJobflowCompiler().compile(
                context,
                batchInfo("b"),
                jobflow("testing"));

        assertThat(saw.get(), is(true));
    }
}
