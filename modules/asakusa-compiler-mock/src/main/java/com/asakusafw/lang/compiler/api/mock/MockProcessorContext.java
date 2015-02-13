package com.asakusafw.lang.compiler.api.mock;

import java.io.File;

/**
 * An abstract super interface of mock context for processors.
 */
public interface MockProcessorContext {

    /**
     * Returns the base output directory in this context.
     * @return the base output directory
     */
    File getOutputDirectory();
}
