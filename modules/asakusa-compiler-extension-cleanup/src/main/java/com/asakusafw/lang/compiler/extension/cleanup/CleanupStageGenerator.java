package com.asakusafw.lang.compiler.extension.cleanup;

import java.io.IOException;

import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.mapreduce.CleanupStageEmitter;
import com.asakusafw.lang.compiler.mapreduce.CleanupStageInfo;
import com.asakusafw.lang.compiler.mapreduce.StageInfo;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.runtime.stage.AbstractCleanupStageClient;

/**
 * Adds a default jobflow cleanup stage into the target jobflow.
 */
public class CleanupStageGenerator implements JobflowProcessor {

    @Override
    public void process(Context context, Jobflow source) throws IOException {
        JavaSourceExtension javac = getJavaCompiler(context);
        ClassDescription client = new ClassDescription(AbstractCleanupStageClient.IMPLEMENTATION);
        CleanupStageInfo info = new CleanupStageInfo(
                new StageInfo(context.getBatchId(), source.getFlowId(), CleanupStageInfo.DEFAULT_STAGE_ID),
                context.getOptions().getRuntimeWorkingDirectory());
        CleanupStageEmitter.emit(client, info, javac);
    }

    private JavaSourceExtension getJavaCompiler(Context context) {
        JavaSourceExtension extension = context.getExtension(JavaSourceExtension.class);
        if (extension == null) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, "Java compiler must be supported");
        }
        return extension;
    }
}
