package com.asakusafw.lang.compiler.packaging;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.ResourceContainer;

/**
 * Represents a file container.
 */
public class FileContainer implements ResourceContainer, ResourceRepository {

    private final File basePath;

    /**
     * Creates a new instance.
     * @param basePath the base path
     */
    public FileContainer(File basePath) {
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

    /**
     * Adds a new resource with contents from the input stream.
     * @param location the resource path (relative from the container root)
     * @param contents the resource contents
     * @throws IOException if failed to create a new resource
     */
    public void addResource(Location location, InputStream contents) throws IOException {
        try (OutputStream output = addResource(location)) {
            ResourceUtil.copy(contents, output);
        }
    }

    /**
     * Adds a new resource with contents from the content provider.
     * @param location the resource path (relative from the container root)
     * @param contents the the callback object for preparing resource contents
     * @throws IOException if failed to accept the resource by I/O error
     */
    public void addResource(Location location, ContentProvider contents) throws IOException {
        try (OutputStream output = addResource(location)) {
            contents.writeTo(output);
        }
    }

    /**
     * Creates a {@link ResourceSink} for create resources into this container.
     * @return the created sink
     */
    public ResourceSink createSink() {
        return new FileSink(basePath);
    }

    @Override
    public Cursor createCursor() throws IOException {
        return FileRepository.createCursor(basePath);
    }

    /**
     * Accepts a {@link FileVisitor} in this container.
     * @param visitor the visitor
     * @throws IOException if failed to visit files in this container
     */
    public void accept(FileVisitor visitor) throws IOException {
        ResourceUtil.visit(visitor, basePath);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Resource({0})", //$NON-NLS-1$
                basePath);
    }
}
