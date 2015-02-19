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
