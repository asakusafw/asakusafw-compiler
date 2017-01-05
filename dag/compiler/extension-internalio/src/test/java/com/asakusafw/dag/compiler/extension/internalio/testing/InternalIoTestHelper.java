/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.extension.internalio.testing;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.asakusafw.lang.compiler.internalio.InternalExporterDescription;
import com.asakusafw.lang.compiler.internalio.InternalImporterDescription;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;

/**
 * Helper for testing with internal I/O.
 */
public class InternalIoTestHelper implements TestRule {

    private final TemporaryFolder folder = new TemporaryFolder();

    @Override
    public Statement apply(Statement base, Description description) {
        return folder.apply(base, description);
    }

    /**
     * Returns a description of input.
     * @param dataType the data type
     * @param path the input path
     * @return the description
     */
    public ImporterDescription input(Class<?> dataType, String path) {
        return new InternalImporterDescription.Basic(dataType, locate(path).toURI().toString());
    }

    /**
     * Returns a description of output.
     * @param dataType the data type
     * @param path the input path
     * @return the description
     */
    public ExporterDescription output(Class<?> dataType, String path) {
        return new InternalExporterDescription.Basic(dataType, locate(path).toURI().toString());
    }

    /**
     * Prepares input data.
     * @param <T> the data type
     * @param dataType the data type
     * @param path the input path
     * @param action the prepare action
     */
    public <T extends Writable> void input(Class<T> dataType, String path, Action<ModelOutput<T>, ?> action) {
        Configuration conf = new Configuration();
        Path p = new Path(locate(path.replace('*', '_')).toURI());
        try (ModelOutput<T> out = TemporaryStorage.openOutput(conf, dataType, p)) {
            action.perform(out);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Collects output data.
     * @param <T> the data type
     * @param dataType the data type
     * @param path the input path
     * @param action the prepare action
     */
    public <T extends Writable> void output(Class<T> dataType, String path, Action<List<T>, ?> action) {
        Configuration conf = new Configuration();
        Path p = new Path(locate(path).toURI());
        try {
            FileSystem fs = p.getFileSystem(conf);
            FileStatus[] stats = fs.globStatus(p);
            List<T> results = new ArrayList<>();
            for (FileStatus stat : stats) {
                try (ModelInput<T> in = TemporaryStorage.openInput(conf, dataType, stat.getPath())) {
                    while (true) {
                        T buf = dataType.newInstance();
                        if (in.readTo(buf) == false) {
                            break;
                        }
                        results.add(buf);
                    }
                }
            }
            action.perform(results);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private File locate(String path) {
        return new File(folder.getRoot(), path).getAbsoluteFile();
    }
}
