package com.asakusafw.lang.compiler.packaging;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An implementation of {@link ResourceSink} which adds resources into directory.
 */
public class FileSink implements ResourceSink {

    private final File root;

    /**
     * Creates a new instance.
     * @param root the base directory of this sink
     */
    public FileSink(File root) {
        this.root = root;
    }

    @Override
    public void add(Location location, InputStream contents) throws IOException {
        File file = ResourceUtil.toFile(root, location);
        try (OutputStream output = ResourceUtil.create(file)) {
            ResourceUtil.copy(contents, output);
        }
    }

    @Override
    public void add(Location location, Callback callback) throws IOException {
        File file = ResourceUtil.toFile(root, location);
        try (OutputStream output = ResourceUtil.create(file)) {
            callback.add(location, output);
        }
    }

    @Override
    public void close() throws IOException {
        return;
    }
}
