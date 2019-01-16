/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.extension.testdriver.mock.MockTextDefinition;
import com.asakusafw.lang.compiler.internalio.InternalExporterDescription;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;
import com.asakusafw.runtime.windows.WindowsSupport;
import com.asakusafw.testdriver.core.DataModelReflection;
import com.asakusafw.testdriver.core.DataModelSource;
import com.asakusafw.testdriver.core.TestContext;
import com.asakusafw.testdriver.hadoop.ConfigurationFactory;

/**
 * Test for {@link InternalExporterRetriever}.
 */
public class InternalExporterRetrieverTest {

    private static final TestContext EMPTY = new TestContext.Empty();

    /**
     * Support for Windows platform.
     */
    @ClassRule
    public static final WindowsSupport WINDOWS_SUPPORT = new WindowsSupport();

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

        InternalExporterRetriever target = new InternalExporterRetriever(factory);
        putText(actual, "Hello, world!");
        File f = new File(URI.create(actual));
        assertThat(f.exists(), is(true));

        target.truncate(new InternalExporterDescription.Basic(Text.class, prefix), EMPTY);
        assertThat(f.exists(), is(false));
    }

    /**
     * retrieve.
     * @throws Exception if test was failed
     */
    @Test
    public void retrieve() throws Exception {
        String base = new File(folder.getRoot(), "part").toURI().toString();
        String prefix = base + "-*";
        String actual = base + "-0000";

        InternalExporterDescription exporter = new InternalExporterDescription.Basic(Text.class, prefix);
        InternalExporterRetriever retriever = new InternalExporterRetriever(factory);

        putText(actual, "Hello, world!", "This is a test.");

        MockTextDefinition definition = new MockTextDefinition();
        try (DataModelSource result = retriever.createSource(definition, exporter, EMPTY)) {
            DataModelReflection ref;
            ref = result.next();
            assertThat(ref, is(not(nullValue())));
            assertThat(definition.toObject(ref), is(new Text("Hello, world!")));

            ref = result.next();
            assertThat(ref, is(not(nullValue())));
            assertThat(definition.toObject(ref), is(new Text("This is a test.")));

            ref = result.next();
            assertThat(ref, is(nullValue()));
        }
    }

    /**
     * prepare.
     * @throws Exception if test was failed
     */
    @Test
    public void prepare() throws Exception {
        String base = new File(folder.getRoot(), "part").toURI().toString();
        String prefix = base + "-*";

        InternalExporterRetriever target = new InternalExporterRetriever(factory);
        try (ModelOutput<Text> open = target.createOutput(
                new MockTextDefinition(),
                new InternalExporterDescription.Basic(Text.class, prefix),
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
