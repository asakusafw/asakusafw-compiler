package com.asakusafw.lang.compiler.tester.executor;

import java.io.IOException;
import java.util.Map;

import com.asakusafw.lang.compiler.tester.TesterContext;

/**
 * Executes a compilation artifact for testing.
 * @param <T> the artifact type
 */
public interface ArtifactExecutor<T> {

    /**
     * Executes the artifact with empty batch arguments.
     * @param context the current tester context
     * @param artifact the target artifact
     * @throws InterruptedException if interrupted while executing the artifact
     * @throws IOException if failed to execute the target artifact
     */
    void execute(TesterContext context, T artifact) throws InterruptedException, IOException;

    /**
     * Executes the artifact with empty batch arguments.
     * @param context the current tester context
     * @param artifact the target artifact
     * @param arguments the batch arguments
     * @throws InterruptedException if interrupted while executing the artifact
     * @throws IOException if failed to execute the target artifact
     */
    void execute(
            TesterContext context, T artifact,
            Map<String, String> arguments) throws InterruptedException, IOException;
}
