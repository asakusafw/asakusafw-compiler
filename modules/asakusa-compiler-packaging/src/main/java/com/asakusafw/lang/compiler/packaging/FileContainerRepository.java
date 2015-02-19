package com.asakusafw.lang.compiler.packaging;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides file containers.
 */
public class FileContainerRepository {

    static final Logger LOG = LoggerFactory.getLogger(FileContainerRepository.class);

    private static final int ATTEMPT_LIMIT = 100;

    private final File root;

    private final Random random;

    /**
     * Creates a new instance.
     * @param root the root folder for file containers
     */
    public FileContainerRepository(File root) {
        this.root = root;
        this.random = new Random();
    }

    /**
     * Returns the file containers' root path.
     * @return the root path
     */
    public File getRoot() {
        return root;
    }

    /**
     * Creates a new file container in this repository.
     * @param prefix the folder path prefix (for hints)
     * @return the created temporary container
     * @throws IOException if failed to create a new temporary folder
     */
    public FileContainer newContainer(String prefix) throws IOException {
        for (int i = 0; i < ATTEMPT_LIMIT; i++) {
            String name = String.format("%s-%08x", prefix, random.nextInt());
            File result = new File(root, name);
            if (result.mkdirs()) {
                assert result.isDirectory();
                return new FileContainer(result);
            } else {
                if (root.getParentFile().isDirectory() == false) {
                    throw new IOException(MessageFormat.format(
                            "failed to create folder: {0}",
                            result));
                }
                // try other
                continue;
            }
        }
        throw new IOException(MessageFormat.format(
                "failed to create temporary folder (attempt limit exceeded): {0}",
                root));
    }

    /**
     * Cleanup this repository.
     * @return {@code true} if cleanup was succeeded, otherwise {@code false}
     */
    public boolean reset() {
        if (root.exists() == false) {
            return true;
        }
        if (ResourceUtil.delete(root) == false) {
            LOG.warn(MessageFormat.format(
                    "failed to delete folder; please delete manually: {0}",
                    root));
            return false;
        }
        return true;
    }
}
