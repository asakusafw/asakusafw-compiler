package com.asakusafw.lang.compiler.api.basic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;

import com.asakusafw.lang.compiler.common.Location;

/**
 * A local file system based resource container.
 */
public class BasicResourceContainer {

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

    /**
     * Adds a new resource file and returns its output stream.
     * @param location the resource path (relative from the base path)
     * @return the output stream to set the target file contents
     * @throws IOException if failed to create a new file
     */
    public OutputStream addResourceFile(Location location) throws IOException {
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
}
