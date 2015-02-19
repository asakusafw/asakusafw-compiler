package com.asakusafw.lang.compiler.core.dummy;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.core.BatchCompiler;

/**
 * Mock implementation of {@link JobflowProcessor}.
 */
public class SimpleBatchProcessor implements BatchProcessor {

    private static final Location MARKER = Location.of(SimpleBatchProcessor.class.getName(), '.');

    /**
     * Returns whether the processor was activated in the context.
     * @param context the current context
     * @return {@code true} if this processor was activated
     */
    public static boolean contains(BatchCompiler.Context context) {
        return containsFile(context);
    }

    private static boolean containsFile(BatchCompiler.Context context) {
        File file = context.getOutput().toFile(MARKER);
        return file.isFile();
    }

    @Override
    public void process(Context context, BatchReference source) throws IOException {
        try (OutputStream output = context.addResourceFile(MARKER)) {
            output.write("testing".getBytes("UTF-8"));
        }
    }
}
