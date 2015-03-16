package com.asakusafw.lang.compiler.core.participant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.core.JobflowCompiler.Context;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.hadoop.HadoopFormatExtension;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * A compiler participant for enabling {@link HadoopFormatExtension}.
 * <p>
 * This participant recognize the following compiler properties:
 * </p>
 * <dl>
 * <dt> {@code hadoop.format.input} ({@link #KEY_INPUT_FORMAT}) </dt>
 *   <dd> Hadoop input format class name </dd>
 *   <dd> default: {@code "com.asakusafw.runtime.stage.input.TemporaryInputFormat"} </dd>
 *
 * <dt> {@code hadoop.format.output} ({@link #KEY_OUTPUT_FORMAT}) </dt>
 *   <dd> Hadoop output format class name </dd>
 *   <dd> default: {@code "com.asakusafw.runtime.stage.input.TemporaryOutputFormat"} </dd>
 * </dl>
 */
public class HadoopFormatExtensionParticipant extends AbstractCompilerParticipant {

    static final Logger LOG = LoggerFactory.getLogger(HadoopFormatExtensionParticipant.class);

    private static final String KEY_PREFIX = "hadoop.format."; //$NON-NLS-1$

    /**
     * The property key for the Hadoop input format class.
     */
    public static final String KEY_INPUT_FORMAT = KEY_PREFIX + "input"; //$NON-NLS-1$

    /**
     * The property key for the Hadoop output format class.
     */
    public static final String KEY_OUTPUT_FORMAT = KEY_PREFIX + "output"; //$NON-NLS-1$

    @Override
    public void beforeJobflow(Context context, BatchInfo batch, Jobflow jobflow) {
        LOG.debug("enabling {}", HadoopFormatExtension.class.getName()); //$NON-NLS-1$
        HadoopFormatExtension defaults = HadoopFormatExtension.DEFAULT;
        ClassDescription input = getFormat(context, KEY_INPUT_FORMAT, defaults.getInputFormat());
        ClassDescription output = getFormat(context, KEY_OUTPUT_FORMAT, defaults.getOutputFormat());
        HadoopFormatExtension extension = new HadoopFormatExtension(input, output);
        context.registerExtension(HadoopFormatExtension.class, extension);
    }

    private ClassDescription getFormat(Context context, String key, ClassDescription defaultValue) {
        String value = context.getOptions().get(key, null);
        if (value == null) {
            return defaultValue;
        }
        return new ClassDescription(value);
    }
}
