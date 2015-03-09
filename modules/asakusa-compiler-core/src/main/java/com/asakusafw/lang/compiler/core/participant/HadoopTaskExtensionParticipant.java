package com.asakusafw.lang.compiler.core.participant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.core.JobflowCompiler.Context;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.hadoop.BasicHadoopTaskExtension;
import com.asakusafw.lang.compiler.hadoop.HadoopTaskExtension;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * A compiler participant for enabling {@link HadoopTaskExtension}.
 */
public class HadoopTaskExtensionParticipant extends AbstractCompilerParticipant {

    static final Logger LOG = LoggerFactory.getLogger(HadoopTaskExtensionParticipant.class);

    @Override
    public void beforeJobflow(Context context, BatchInfo batch, Jobflow jobflow) {
        LOG.debug("enabling {}", HadoopTaskExtension.class.getName());
        BasicHadoopTaskExtension extension = new BasicHadoopTaskExtension(context.getTaskContainerMap());
        context.registerExtension(HadoopTaskExtension.class, extension);
    }
}
