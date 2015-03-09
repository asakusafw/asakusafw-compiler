package com.asakusafw.bridge.redirector;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrite ZIP/JAR file to redirect API invocations.
 */
public class ZipRewriter {

    static final Logger LOG = LoggerFactory.getLogger(ZipRewriter.class);

    static final String CLASS_EXTENSION = ".class"; //$NON-NLS-1$

    final ClassRewriter classRewriter;

    /**
     * Creates a new instance.
     * @param rule the redirect rule
     */
    public ZipRewriter(RedirectRule rule) {
        this.classRewriter = new ClassRewriter(rule);
    }

    /**
     * Rewrite entries in the ZIP archive and write them into the output.
     * @param input the source ZIP archive
     * @param output the target ZIP archive
     * @throws IOException if failed to rewrite by I/O error
     */
    public void rewrite(ZipInputStream input, ZipOutputStream output) throws IOException {
        while (true) {
            ZipEntry entry = input.getNextEntry();
            if (entry == null) {
                break;
            }
            output.putNextEntry(entry);
            if (entry.isDirectory()) {
                continue;
            }
            if (isTarget(entry)) {
                LOG.debug("rewrite: {}", entry.getName());
                classRewriter.rewrite(input, output);
            } else {
                LOG.debug("   copy: {}", entry.getName());
                Util.copy(input, output);
            }
        }
    }

    private boolean isTarget(ZipEntry entry) {
        return entry.getName().endsWith(CLASS_EXTENSION);
    }
}
