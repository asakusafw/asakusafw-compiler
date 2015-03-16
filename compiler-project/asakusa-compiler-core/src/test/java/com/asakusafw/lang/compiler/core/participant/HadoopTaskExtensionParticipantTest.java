package com.asakusafw.lang.compiler.core.participant;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference.Phase;
import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.basic.BasicJobflowCompiler;
import com.asakusafw.lang.compiler.core.dummy.SimpleExternalPortProcessor;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskExtension;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
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
        jobflowProcessors.add(new JobflowProcessor() {
            @Override
            public void process(Context context, Jobflow source) throws IOException {
                HadoopTaskExtension extension = context.getExtension(HadoopTaskExtension.class);
                assertThat(extension, is(notNullValue()));
                extension.addTask(Phase.MAIN, new ClassDescription("testing"));
            }
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
