package com.asakusafw.lang.compiler.core.participant;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.core.JobflowCompiler.Context;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.javac.BasicJavaCompilerSupport;
import com.asakusafw.lang.compiler.javac.JavaCompilerUtil;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;

/**
 * A compiler participant for enabling {@link JavaSourceExtension}.
 */
public class JavaSourceExtensionParticipant extends AbstractCompilerParticipant {

    static final Logger LOG = LoggerFactory.getLogger(JavaSourceExtensionParticipant.class);

    @Override
    public void beforeJobflow(Context context, BatchInfo batch, Jobflow jobflow) {
        LOG.debug("enabling {}", JavaSourceExtension.class.getName());
        List<File> classPath = JavaCompilerUtil.getLibraries(context.getProject().getClassLoader());
        File sourcePath = createTemporaryOutput(context, jobflow);
        BasicJavaCompilerSupport extension = new BasicJavaCompilerSupport(
                sourcePath,
                classPath,
                context.getOutput().getBasePath());
        context.registerExtension(JavaSourceExtension.class, extension);
    }

    @Override
    public void afterJobflow(Context context, BatchInfo batch, Jobflow jobflow) {
        JavaSourceExtension extension = context.getExtension(JavaSourceExtension.class);
        if ((extension instanceof BasicJavaCompilerSupport) == false) {
            return;
        }
        context.registerExtension(JavaSourceExtension.class, null);

        BasicJavaCompilerSupport javac = (BasicJavaCompilerSupport) extension;
        javac.process();
        ResourceUtil.delete(javac.getSourcePath());
    }

    private File createTemporaryOutput(Context context, JobflowInfo jobflow) {
        String prefix = String.format("javac-%s", jobflow.getFlowId()); //$NON-NLS-1$
        try {
            return context.getTemporaryOutputs().newContainer(prefix).getBasePath();
        } catch (IOException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to create temporary output: {0}",
                    context.getTemporaryOutputs().getRoot()));
        }
    }
}
