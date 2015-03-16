package com.asakusafw.lang.compiler.core.util;

import java.io.Closeable;

import com.asakusafw.lang.compiler.packaging.FileContainer;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;

/**
 * Provides {@link Closeable} feature for {@link FileContainer}s.
 */
public class FileContainerCleaner implements Closeable {

    private final FileContainer container;

    /**
     * Creates a new instance.
     * @param container the holding file container
     */
    public FileContainerCleaner(FileContainer container) {
        this.container = container;
    }

    /**
     * Returns the holding file container.
     * @return the holding file container
     */
    public FileContainer getContainer() {
        return container;
    }

    @Override
    public void close() {
        ResourceUtil.delete(container.getBasePath());
    }
}
