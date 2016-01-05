/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.testdriver;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.extension.testdriver.InternalImporterDescription;
import com.asakusafw.lang.compiler.extension.testdriver.InternalImporterPreparator;
import com.asakusafw.lang.compiler.extension.testdriver.mock.MockTextDefinition;
import com.asakusafw.lang.compiler.mapreduce.testing.windows.WindowsConfigurator;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;
import com.asakusafw.testdriver.core.TestContext;
import com.asakusafw.testdriver.hadoop.ConfigurationFactory;

/**
 * Test for {@link InternalImporterPreparator}.
 */
public class InternalImporterPreparatorTest {

    static {
        WindowsConfigurator.install();
    }

    private static final TestContext EMPTY = new TestContext.Empty();

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    private final ConfigurationFactory factory = ConfigurationFactory.getDefault();

    /**
     * truncate.
     * @throws Exception if test was failed
     */
    @Test
    public void truncate() throws Exception {
        String base = new File(folder.getRoot(), "part").toURI().toString();
        String prefix = base + "-*";
        String actual = base + "-0000";

        InternalImporterPreparator target = new InternalImporterPreparator(factory);
        putText(actual, "Hello, world!");
        File f = new File(URI.create(actual));
        assertThat(f.exists(), is(true));

        target.truncate(new InternalImporterDescription.Basic(Text.class, prefix), EMPTY);
        assertThat(f.exists(), is(false));
    }

    /**
     * prepare.
     * @throws Exception if test was failed
     */
    @Test
    public void prepare() throws Exception {
        String base = new File(folder.getRoot(), "part").toURI().toString();
        String prefix = base + "-*";

        InternalImporterPreparator target = new InternalImporterPreparator(factory);
        try (ModelOutput<Text> open = target.createOutput(
                new MockTextDefinition(),
                new InternalImporterDescription.Basic(Text.class, prefix),
                EMPTY)) {
            open.write(new Text("Hello, world!"));
            open.write(new Text("This is a test."));
        }
        try (ModelInput<Text> input = TemporaryStorage.openInput(
                factory.newInstance(),
                Text.class,
                new Path(InternalImporterPreparator.resolvePathPrefix(EMPTY, prefix)))) {
            Text text = new Text();
            assertThat(input.readTo(text), is(true));
            assertThat(text.toString(), is("Hello, world!"));
            assertThat(input.readTo(text), is(true));
            assertThat(text.toString(), is("This is a test."));
            assertThat(input.readTo(text), is(false));
        }
    }

    private void putText(String path, String... lines) throws IOException {
        try (ModelOutput<Text> output = TemporaryStorage.openOutput(
                factory.newInstance(), Text.class, new Path(path))) {
            for (String s : lines) {
                output.write(new Text(s));
            }
        }
    }
}
