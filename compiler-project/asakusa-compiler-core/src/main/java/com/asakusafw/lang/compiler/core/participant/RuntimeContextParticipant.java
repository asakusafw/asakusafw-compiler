package com.asakusafw.lang.compiler.core.participant;

import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.JobflowCompiler.Context;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.runtime.core.context.RuntimeContext;

/**
 * A compiler participant for enabling {@link RuntimeContext}.
 */
public class RuntimeContextParticipant extends AbstractCompilerParticipant {

    static final Logger LOG = LoggerFactory.getLogger(RuntimeContextParticipant.class);

    static final Location LOCATION = Location.of(RuntimeContext.PATH_APPLICATION_INFO);

    @Override
    public void afterJobflow(Context context, BatchInfo batch, Jobflow jobflow) {
        LOG.debug("injecting application info: {}/{}", batch.getBatchId(), jobflow.getFlowId()); //$NON-NLS-1$
        Properties properties = new Properties();
        properties.setProperty(RuntimeContext.KEY_BATCH_ID, batch.getBatchId());
        properties.setProperty(RuntimeContext.KEY_FLOW_ID, jobflow.getFlowId());
        properties.setProperty(RuntimeContext.KEY_BUILD_ID, context.getOptions().getBuildId());
        properties.setProperty(RuntimeContext.KEY_BUILD_DATE, getDate());
        properties.setProperty(RuntimeContext.KEY_RUNTIME_VERSION, RuntimeContext.getRuntimeVersion());

        try (OutputStream output = context.getOutput().addResource(LOCATION)) {
            properties.store(output, null);
        } catch (IOException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to create application info file: batch={0}, jobflow={1}, path={2}",
                    batch.getBatchId(),
                    jobflow.getFlowId(),
                    LOCATION), e);
        }
    }

    private static String getDate() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()); //$NON-NLS-1$
    }
}
