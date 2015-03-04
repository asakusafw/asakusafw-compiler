package com.asakusafw.lang.compiler.extension.directio.mock;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.asakusafw.runtime.directio.hadoop.HadoopDataSource;

/**
 * Direct I/O runtime context.
 */
public class DirectIoContext implements TestRule {

    private final TemporaryFolder temporary = new TemporaryFolder();

    @Override
    public Statement apply(final Statement base, final Description description) {
        return temporary.apply(base, description);
    }

    /**
     * Returns the root of the Direct I/O working area.
     * @return the root
     */
    public File getRoot() {
        return new File(temporary.getRoot(), "directio");
    }

    /**
     * Returns the Direct I/O root path.
     * @return the root path
     */
    public Path getRootPath() {
        return new Path(getRoot().toURI());
    }

    /**
     * Returns a file on the Direct I/O working area.
     * @param relativePath the relative path from the root
     * @return the target file
     */
    public File file(String relativePath) {
        return new File(getRoot(), relativePath);
    }

    /**
     * Returns a file on the Direct I/O working area.
     * @param relativePath the relative path from the root
     * @return the target file
     */
    public Path path(String relativePath) {
        return new Path(file(relativePath).toURI());
    }

    /**
     * Returns a new configuration.
     * @return the created configuration
     */
    public Configuration newConfiguration() {
        Configuration conf = new Configuration();
        return configure(conf);
    }

    /**
     * Adds Direct I/O settings for the configuration.
     * @param conf the target configuration
     * @return the configured object
     */
    public Configuration configure(Configuration conf) {
        conf.set("com.asakusafw.output.system.dir", new Path(new File(getRoot(), "system").toURI()).toString());
        conf.set("com.asakusafw.directio.root", HadoopDataSource.class.getName());
        conf.set("com.asakusafw.directio.root.path", "/");
        conf.set("com.asakusafw.directio.root.fs.path", getRootPath().toString());
        return conf;
    }
}
