package com.asakusafw.lang.compiler.testing;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
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
            throw new AssertionError(e);
        }
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
            while (true) {
                ZipEntry entry = in.getNextEntry();
                if (entry == null) {
                    break;
                }
                File target = new File(outputDirectory, entry.getName());
                if (entry.isDirectory()) {
                    target.mkdirs();
                } else {
                    try (OutputStream out = create(target)) {
                        copy(in, out);
                    }
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return outputDirectory;
    }

    private File copy(InputStream input, String targetPath) {
        File target = toOutputFile(targetPath);
        try (OutputStream output = create(target)) {
            copy(input, output);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return target;
    }

    private void copy(InputStream input, OutputStream output) throws IOException {
        byte[] buf = new byte[256];
        while (true) {
            int read = input.read(buf);
            if (read < 0) {
                break;
            }
            output.write(buf, 0, read);
        }
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

    private OutputStream create(File target) throws IOException {
        target.getAbsoluteFile().getParentFile().mkdirs();
        return new BufferedOutputStream(new FileOutputStream(target));
    }
}
