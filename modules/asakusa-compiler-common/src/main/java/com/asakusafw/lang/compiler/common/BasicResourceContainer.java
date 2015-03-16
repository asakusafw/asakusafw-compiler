package com.asakusafw.lang.compiler.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;

/**
 * A local file system based resource container.
 */
public class BasicResourceContainer implements ResourceContainer {

    private final File basePath;

    /**
     * Creates a new instance.
     * @param basePath the base path
     */
    public BasicResourceContainer(File basePath) {
        this.basePath = basePath;
    }

    /**
     * Returns the base path of this container.
     * @return the base path
     */
    public File getBasePath() {
        return basePath;
    }

    /**
     * Returns a {@link File} for the location.
     * @param location the target location
     * @return the related file path (may or may not exist)
     */
    public File toFile(Location location) {
        File file = new File(basePath, location.toPath(File.separatorChar)).getAbsoluteFile();
        return file;
    }

    @Override
    public OutputStream addResource(Location location) throws IOException {
        File file = toFile(location);
        if (file.exists()) {
            throw new IOException(MessageFormat.format(
                    "generating file already exists: {0}",
                    file));
        }
        File parent = file.getParentFile();
        if (parent.mkdirs() == false && parent.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "failed to prepare a parent directory: {0}",
                    file));
        }
        return new FileOutputStream(file);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Resource({0})", //$NON-NLS-1$
                basePath);
    }
}
