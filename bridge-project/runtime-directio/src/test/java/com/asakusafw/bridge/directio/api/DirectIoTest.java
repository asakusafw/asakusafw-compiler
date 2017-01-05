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
package com.asakusafw.bridge.directio.api;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.lang.compiler.mapreduce.testing.mock.DirectIoContext;
import com.asakusafw.runtime.core.HadoopConfiguration;
import com.asakusafw.runtime.core.ResourceConfiguration;
import com.asakusafw.runtime.directio.BinaryStreamFormat;
import com.asakusafw.runtime.directio.api.DirectIo;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;

/**
 * Test for {@link com.asakusafw.bridge.directio.api.DirectIo}.
 */
public class DirectIoTest {

    /**
     * testing environment.
     */
    @Rule
    public final DirectIoContext env = new DirectIoContext();

    /**
     * using {@link ResourceBroker} in testing.
     */
    @Rule
    public final ResourceBrokerContext context = new ResourceBrokerContext();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        ResourceBroker.put(Configuration.class, env.newConfiguration());
        put(env.file("testing/a.txt"), "Hello, world!");

        Set<String> results = consume();
        assertThat(results, is(set("Hello, world!")));
    }

    /**
     * missing files.
     * @throws Exception if failed
     */
    @Test
    public void missing() throws Exception {
        ResourceBroker.put(Configuration.class, env.newConfiguration());
        Set<String> results = consume();
        assertThat(results, is(empty()));
    }

    /**
     * multiple files.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        ResourceBroker.put(Configuration.class, env.newConfiguration());
        put(env.file("testing/t1.txt"), "Hello1");
        put(env.file("testing/t2.txt"), "Hello2");
        put(env.file("testing/t3.txt"), "Hello3");

        Set<String> results = consume();
        assertThat(results, is(set("Hello1", "Hello2", "Hello3")));
    }

    /**
     * using asakusa conf.
     * @throws Exception if failed
     */
    @Test
    public void asakusa_conf() throws Exception {
        ResourceBroker.put(ResourceConfiguration.class, new HadoopConfiguration(env.newConfiguration()));
        put(env.file("testing/a.txt"), "Hello, world!");

        Set<String> results = consume();
        assertThat(results, is(set("Hello, world!")));
    }

    private Set<String> consume() throws IOException {
        try (ModelInput<StringBuilder> input = DirectIo.open(MockFormat.class, "testing", "*.txt")) {
            return consume(input);
        }
    }

    /**
     * w/o conf.
     * @throws Exception if failed
     */
    @Test(expected = IllegalStateException.class)
    public void missing_conf() throws Exception {
        DirectIo.open(MockFormat.class, "testing", "*.txt").close();
    }

    private static Set<String> set(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }

    private static File put(File file, String... lines) throws IOException {
        file.getAbsoluteFile().getParentFile().mkdirs();
        try (PrintWriter writer = new PrintWriter(file, "UTF-8")) {
            for (String line : lines) {
                writer.println(line);
            }
        }
        return file;
    }

    private static Set<String> consume(ModelInput<StringBuilder> input) throws IOException {
        Set<String> results = new HashSet<>();
        StringBuilder buf = new StringBuilder();
        while (input.readTo(buf)) {
            results.add(buf.toString());
        }
        return results;
    }

    /**
     * mock data format.
     */
    public static class MockFormat extends BinaryStreamFormat<StringBuilder> {

        @Override
        public Class<StringBuilder> getSupportedType() {
            return StringBuilder.class;
        }

        @Override
        public ModelInput<StringBuilder> createInput(
                Class<? extends StringBuilder> dataType, String path,
                InputStream stream, long offset, long fragmentSize)
                throws IOException, InterruptedException {
            assertThat(offset, is(0L));
            Scanner scanner = new Scanner(stream, "UTF-8");
            return new ModelInput<StringBuilder>() {
                @Override
                public boolean readTo(StringBuilder model) throws IOException {
                    if (scanner.hasNextLine()) {
                        model.setLength(0);
                        model.append(scanner.nextLine());
                        return true;
                    } else {
                        return false;
                    }
                }
                @Override
                public void close() throws IOException {
                    scanner.close();
                }
            };
        }

        @Override
        public ModelOutput<StringBuilder> createOutput(
                Class<? extends StringBuilder> dataType, String path,
                OutputStream stream) throws IOException, InterruptedException {
            throw new UnsupportedOperationException();
        }
    }
}
