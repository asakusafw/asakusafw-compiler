package com.asakusafw.lang.compiler.api.mock;

import java.io.File;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An abstract super interface of mock context for processors.
 */
public interface MockProcessorContext {

    /**
     * Returns the base output directory in this context.
     * @return the base output directory
     */
    File getOutputDirectory();

    /**
     * Returns an output file.
     * @param location the output location (relative from the base output directory)
     * @return the related output file (may not exist)
     */
    File getOutputFile(Location location);
}
