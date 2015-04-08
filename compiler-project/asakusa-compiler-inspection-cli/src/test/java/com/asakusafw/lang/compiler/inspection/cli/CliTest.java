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
package com.asakusafw.lang.compiler.inspection.cli;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.inspection.InspectionNode;
import com.asakusafw.lang.compiler.inspection.cli.Cli.Configuration;
import com.asakusafw.lang.compiler.inspection.json.JsonInspectionNodeRepository;
import com.asakusafw.lang.compiler.inspection.processor.StoreProcessor;

/**
 * Test for {@link Cli}.
 */
public class CliTest {

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * parse - minimal case.
     * @throws Exception if failed
     */
    @Test
    public void parse_minimal() throws Exception {
        File input = temporary.newFile();
        Configuration conf = Cli.parse(strings(new Object[] {
                "--input", input,
        }));
        assertThat(conf.input, is(input));
        assertThat(conf.output, is(nullValue()));
        assertThat(conf.path, is(nullValue()));
        assertThat(conf.format, is(nullValue()));
        assertThat(conf.properties.keySet(), hasSize(0));
    }

    /**
     * parse - empty arguments.
     * @throws Exception if failed
     */
    @Test(expected = Exception.class)
    public void parse_empty() throws Exception {
        Cli.parse(strings());
    }

    /**
     * parse - empty input.
     * @throws Exception if failed
     */
    @Test(expected = Exception.class)
    public void parse_empty_input() throws Exception {
        Cli.parse(strings(new Object[] {
                "--input", "",
        }));
    }

    /**
     * parse - full case.
     * @throws Exception if failed
     */
    @Test
    public void parse_full() throws Exception {
        File input = temporary.newFile();
        File output = temporary.newFile();
        Configuration conf = Cli.parse(strings(new Object[] {
                "--input", input,
                "--output", output,
                "--path", "path/testing",
                "--format", "test-format",
                "-P", "a=AAA",
                "-P", "b",
                "--property", "c=CCC",
        }));
        assertThat(conf.input, is(input));
        assertThat(conf.output, is(output));
        assertThat(conf.path, is("path/testing"));
        assertThat(conf.format, is("test-format"));
        assertThat(conf.properties.keySet(), containsInAnyOrder("a", "b", "c"));
        assertThat(conf.properties, hasEntry("a", "AAA"));
        assertThat(conf.properties, hasEntry("b", "true"));
        assertThat(conf.properties, hasEntry("c", "CCC"));
    }

    /**
     * execute - minimal case.
     * @throws Exception if failed
     */
    @Test
    public void execute_minimal() throws Exception {
        File input = dump(new InspectionNode("testing", "TESTING"));
        int status = Cli.execute(strings(new Object[] {
                "--input", input,
        }));
        assertThat(status, is(0));
    }

    /**
     * execute - w/ invalid arguments.
     * @throws Exception if failed
     */
    @Test
    public void execute_invalid_arguments() throws Exception {
        File input = dump(new InspectionNode("testing", "TESTING"));
        int status = Cli.execute(strings(new Object[] {
                "--INVALID", input,
        }));
        assertThat(status, is(not(0)));
    }

    /**
     * execute - w/ invalid arguments.
     * @throws Exception if failed
     */
    @Test
    public void execute_invalid_options() throws Exception {
        File input = dump(new InspectionNode("testing", "TESTING"));
        int status = Cli.execute(strings(new Object[] {
                "--input", input,
                "--format", "<MISSING>",
        }));
        assertThat(status, is(not(0)));
    }

    /**
     * process - w/ simple case.
     * @throws Exception if failed
     */
    @Test
    public void process_simple() throws Exception {
        Configuration conf = new Configuration();
        conf.input = dump(new InspectionNode("testing", "TESTING"));
        conf.format = "json";

        File file = temporary.newFile();
        try (OutputStream output = new FileOutputStream(file)) {
            conf.defaultOutput = output;
            Cli.process(conf);
        }

        InspectionNode node = load(file);
        assertThat(node.getId(), is("testing"));
        assertThat(node.getTitle(), is("TESTING"));
    }

    /**
     * process - w/ output.
     * @throws Exception if failed
     */
    @Test
    public void process_output() throws Exception {
        Configuration conf = new Configuration();
        conf.input = dump(new InspectionNode("testing", "TESTING"));
        conf.output = temporary.newFile();
        conf.format = "json";
        Cli.process(conf);

        InspectionNode node = load(conf.output);
        assertThat(node.getId(), is("testing"));
        assertThat(node.getTitle(), is("TESTING"));
    }

    /**
     * process - w/ path.
     * @throws Exception if failed
     */
    @Test
    public void process_path() throws Exception {
        Configuration conf = new Configuration();
        conf.input = dump(new InspectionNode("a", "A")
                .withElement(new InspectionNode("b", "B")
                        .withElement(new InspectionNode("c", "C")
                            .withElement(new InspectionNode("d", "D")))));
        conf.output = temporary.newFile();
        conf.path = "b/c";
        conf.format = "json";
        Cli.process(conf);

        InspectionNode node = load(conf.output);
        assertThat(node.getId(), is("c"));
        assertThat(node.getTitle(), is("C"));
    }

    /**
     * process - w/ format.
     * @throws Exception if failed
     */
    @Test
    public void process_format() throws Exception {
        Configuration conf = new Configuration();
        conf.input = dump(new InspectionNode("testing", "TESTING"));
        conf.output = temporary.newFile();
        conf.format = StoreProcessor.class.getName();
        Cli.process(conf);

        InspectionNode node = load(conf.output);
        assertThat(node.getId(), is("testing"));
        assertThat(node.getTitle(), is("TESTING"));
    }

    private String[] strings(Object... values) {
        String[] results = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            results[i] = String.valueOf(values[i]);
        }
        return results;
    }

    private File dump(InspectionNode node) throws IOException {
        File file = temporary.newFile();
        try (OutputStream output = new FileOutputStream(file)) {
            new JsonInspectionNodeRepository().store(output, node);
        }
        return file;
    }

    private InspectionNode load(File file) throws IOException {
        try (InputStream input = new FileInputStream(file)) {
            return new JsonInspectionNodeRepository().load(input);
        }
    }
}
