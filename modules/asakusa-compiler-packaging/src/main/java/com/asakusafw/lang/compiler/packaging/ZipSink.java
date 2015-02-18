package com.asakusafw.lang.compiler.packaging;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An implementation of {@link ResourceSink} which adds resources into ZIP archive.
 */
public class ZipSink implements ResourceSink {

    private static final Charset ENCODING = Charset.forName("UTF-8"); //$NON-NLS-1$

    private final ZipOutputStream output;

    /**
     * Creates a new instance.
     * @param output the target file
     * @throws IOException if failed to create ZIP file
     */
    public ZipSink(File output) throws IOException {
        this(new ZipOutputStream(ResourceUtil.create(output), ENCODING));
    }

    /**
     * Creates a new instance.
     * @param output the target ZIP output stream
     */
    public ZipSink(ZipOutputStream output) {
        this.output = output;
    }

    @Override
    public void add(Location location, InputStream contents) throws IOException {
        output.putNextEntry(new ZipEntry(location.toPath()));
        ResourceUtil.copy(contents, output);
    }

    @Override
    public void close() throws IOException {
        output.close();
    }
}
