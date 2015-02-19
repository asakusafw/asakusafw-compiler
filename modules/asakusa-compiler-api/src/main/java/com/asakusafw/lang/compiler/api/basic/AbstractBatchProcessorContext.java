package com.asakusafw.lang.compiler.api.basic;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.common.BasicExtensionContainer;

/**
 * An abstract implementation of {@link com.asakusafw.lang.compiler.api.BatchProcessor.Context}.
 */
public abstract class AbstractBatchProcessorContext extends BasicExtensionContainer
        implements BatchProcessor.Context {

    // no special members
}
