package com.asakusafw.lang.compiler.common.testing;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipInputStream;

import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Deploys files into a temporary directories.
 */
public class FileDeployer implements TestRule {

    private final TemporaryFolder folder = new TemporaryFolder();

    private volatile Class<?> current;

    @Override
    public Statement apply(Statement base, Description description) {
        this.current = description.getTestClass();
        return folder.apply(base, description);
    }

    /**
     * Returns a new folder.
     * @return a new folder
     */
    public File newFolder() {
        try {
            return folder.newFolder();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Returns a file on the temporary directory.
     * @param targetPath the target path
     * @return the file on the temporary directory
     */
    public File getFile(String targetPath) {
        return toOutputFile(targetPath);
    }

    /**
     * Copy a file into the target path.
     * @param sourcePath the source path (relative from class path)
     * @param targetPath the target path (relative from the temporary folder root)
     * @return the copied file
     */
    public File copy(String sourcePath, String targetPath) {
        try (InputStream in = open(sourcePath)) {
            return copy(in, targetPath);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Copies a contents in {@link InputStream} into the target path.
     * @param contents the source contents
     * @param targetPath the target path (relative from the temporary folder root)
     * @return the copied file
     */
    public File copy(InputStream contents, String targetPath) {
        File target = toOutputFile(targetPath);
        FileEditor.copy(contents, target);
        return target;
    }

    /**
     * Extracts a ZIP archive file onto the target path.
     * @param sourcePath the source path (relative from class path)
     * @param targetPath the target path (relative from the temporary folder root)
     * @return the extracted directory
     */
    public File extract(String sourcePath, String targetPath) {
        File outputDirectory = toOutputFile(targetPath);
        try (ZipInputStream in = new ZipInputStream(open(sourcePath))) {
            FileEditor.extract(in, outputDirectory);
        } catch (IOException e) {
            throw new IOError(e);
        }
        return outputDirectory;
    }

    private InputStream open(String sourcePath) {
        Class<?> c = current;
        assertThat("not yet initialized", c, is(notNullValue()));
        InputStream in = c.getResourceAsStream(sourcePath);
        assertThat(sourcePath, in, is(notNullValue()));
        return new BufferedInputStream(in);
    }

    private File toOutputFile(String path) {
        File target = new File(folder.getRoot(), path);
        return target;
    }
}
