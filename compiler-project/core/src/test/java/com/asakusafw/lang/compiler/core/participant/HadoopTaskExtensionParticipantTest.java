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

import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.basic.BasicJobflowCompiler;
import com.asakusafw.lang.compiler.core.dummy.SimpleExternalPortProcessor;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskExtension;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.packaging.FileContainer;

/**
 * Test for {@link HadoopTaskExtensionParticipant}.
 */
public class HadoopTaskExtensionParticipantTest extends CompilerTestRoot {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        externalPortProcessors.add(new SimpleExternalPortProcessor());
        compilerParticipants.add(new HadoopTaskExtensionParticipant());
        jobflowProcessors.add((context, source) -> {
            HadoopTaskExtension extension = context.getExtension(HadoopTaskExtension.class);
            assertThat(extension, is(notNullValue()));
            extension.addTask(Phase.MAIN, new ClassDescription("testing"));
        });

        FileContainer output = container();
        JobflowCompiler.Context context = new JobflowCompiler.Context(context(true), output);
        new BasicJobflowCompiler().compile(
                context,
                batchInfo("b"),
                jobflow("testing"));

        List<TaskReference> tasks = context.getTaskContainerMap().getMainTaskContainer().getElements();
        assertThat(tasks, contains(instanceOf(HadoopTaskReference.class)));
    }
}
