package com.asakusafw.lang.compiler.core.participant;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.basic.BasicJobflowCompiler;
import com.asakusafw.lang.compiler.core.dummy.SimpleExternalPortProcessor;
import com.asakusafw.lang.compiler.hadoop.HadoopFormatExtension;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
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
        final AtomicBoolean executed = new AtomicBoolean();
        externalPortProcessors.add(new SimpleExternalPortProcessor());
        compilerParticipants.add(new HadoopFormatExtensionParticipant());
        jobflowProcessors.add(new JobflowProcessor() {
            @Override
            public void process(Context context, Jobflow source) throws IOException {
                HadoopFormatExtension extension = context.getExtension(HadoopFormatExtension.class);
                assertThat(extension, is(notNullValue()));

                assertThat(extension.getInputFormat(), is(HadoopFormatExtension.DEFAULT_INPUT_FORMAT));
                assertThat(extension.getOutputFormat(), is(HadoopFormatExtension.DEFAULT_OUTPUT_FORMAT));

                executed.set(true);
            }
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
        options = CompilerOptions.builder()
            .withProperty(HadoopFormatExtensionParticipant.KEY_INPUT_FORMAT, "TestingInput")
            .withProperty(HadoopFormatExtensionParticipant.KEY_OUTPUT_FORMAT, "TestingOutput")
            .build();

        final AtomicBoolean executed = new AtomicBoolean();
        externalPortProcessors.add(new SimpleExternalPortProcessor());
        compilerParticipants.add(new HadoopFormatExtensionParticipant());
        jobflowProcessors.add(new JobflowProcessor() {
            @Override
            public void process(Context context, Jobflow source) throws IOException {
                HadoopFormatExtension extension = context.getExtension(HadoopFormatExtension.class);
                assertThat(extension, is(notNullValue()));

                assertThat(extension.getInputFormat(), is(new ClassDescription("TestingInput")));
                assertThat(extension.getOutputFormat(), is(new ClassDescription("TestingOutput")));

                executed.set(true);
            }
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
