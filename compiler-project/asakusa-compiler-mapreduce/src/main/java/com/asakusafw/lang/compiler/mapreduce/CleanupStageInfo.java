package com.asakusafw.lang.compiler.mapreduce;

/**
 * Represents a cleanup stage structure.
 * @see CleanupStageEmitter
 */
public class CleanupStageInfo {

    /**
     * The default cleanup stage ID.
     */
    public static final String DEFAULT_STAGE_ID = "cleanup"; //$NON-NLS-1$

    final StageInfo meta;

    final String cleanupPath;

    /**
     * Creates a new instance.
     * @param meta meta information for the stage
     * @param baseOutputPath the cleanup target path
     */
    public CleanupStageInfo(StageInfo meta, String baseOutputPath) {
        this.meta = meta;
        this.cleanupPath = baseOutputPath;
    }
}
