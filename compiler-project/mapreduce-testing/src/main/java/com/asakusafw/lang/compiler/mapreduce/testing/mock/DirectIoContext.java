/**
 * Copyright 2011-2015 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.asakusafw.lang.compiler.mapreduce.testing.mock;

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
        return new File(temporary.getRoot(), "directio"); //$NON-NLS-1$
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
        Path system = new Path(new File(getRoot(), "system").toURI()); //$NON-NLS-1$
        conf.set("com.asakusafw.output.system.dir", system.toString()); //$NON-NLS-1$
        conf.set("com.asakusafw.directio.root", HadoopDataSource.class.getName()); //$NON-NLS-1$
        conf.set("com.asakusafw.directio.root.path", "/"); //$NON-NLS-1$ //$NON-NLS-2$
        conf.set("com.asakusafw.directio.root.fs.path", getRootPath().toString()); //$NON-NLS-1$
        return conf;
    }
}
