/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.bridge.launch;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link HadoopPropertiesOption}.
 */
public class HadoopPropertiesOptionTest {

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * commands.
     * @throws Exception if failed
     */
    @Test
    public void commands() throws Exception {
        HadoopPropertiesOption option = new HadoopPropertiesOption();
        assertThat(option.getCommands(), containsInAnyOrder("--hadoop-conf"));
    }

    /**
     * simple pair.
     * @throws Exception if failed
     */
    @Test
    public void empty_value() throws Exception {
        HadoopPropertiesOption option = new HadoopPropertiesOption();
        option.accept("--hadoop-conf", "");

        Map<String, String> result = option.resolve();
        assertThat(result.keySet(), hasSize(0));
    }

    /**
     * simple pair.
     * @throws Exception if failed
     */
    @Test
    public void pair() throws Exception {
        HadoopPropertiesOption option = new HadoopPropertiesOption();
        option.accept("--hadoop-conf", "a=A");

        Map<String, String> result = option.resolve();
        assertThat(result.keySet(), hasSize(1));
        assertThat(result, hasEntry("a", "A"));
    }

    /**
     * pairs.
     * @throws Exception if failed
     */
    @Test
    public void pair_many() throws Exception {
        HadoopPropertiesOption option = new HadoopPropertiesOption();
        option.accept("--hadoop-conf", "a=A");
        option.accept("--hadoop-conf", "b=B");
        option.accept("--hadoop-conf", "c=C");

        Map<String, String> result = option.resolve();
        assertThat(result.keySet(), hasSize(3));
        assertThat(result, hasEntry("a", "A"));
        assertThat(result, hasEntry("b", "B"));
        assertThat(result, hasEntry("c", "C"));
    }

    /**
     * w/ default value.
     * @throws Exception if failed
     */
    @Test
    public void default_value() throws Exception {
        HadoopPropertiesOption option = new HadoopPropertiesOption();
        option.accept("--hadoop-conf", "a");

        Map<String, String> result = option.resolve();
        assertThat(result.keySet(), hasSize(1));
        assertThat(result, hasEntry("a", "true"));
    }

    /**
     * using file.
     * @throws Exception if failed
     */
    @Test
    public void file() throws Exception {
        File file = put(new String[] {
                "<configuration>",
                "    <property>",
                "        <name>a</name>",
                "        <value>A</value>",
                "    </property>",
                "    <property>",
                "        <name>b</name>",
                "        <value>B</value>",
                "    </property>",
                "</configuration>",
        });

        HadoopPropertiesOption option = new HadoopPropertiesOption();
        option.accept("--hadoop-conf", "@" + file.getAbsolutePath());

        Map<String, String> result = option.resolve();
        assertThat(result.toString(), result.keySet(), hasSize(2));
        assertThat(result, hasEntry("a", "A"));
        assertThat(result, hasEntry("b", "B"));
    }

    /**
     * using file.
     * @throws Exception if failed
     */
    @Test
    public void file_missing() throws Exception {
        HadoopPropertiesOption option = new HadoopPropertiesOption();
        option.accept("--hadoop-conf", "@MISSING");

        Map<String, String> result = option.resolve();
        assertThat(result.toString(), result.keySet(), hasSize(0));
    }

    /**
     * pairs.
     * @throws Exception if failed
     */
    @Test
    public void overwrite() throws Exception {
        HadoopPropertiesOption option = new HadoopPropertiesOption();
        option.accept("--hadoop-conf", "a=A");
        option.accept("--hadoop-conf", "b=B");
        option.accept("--hadoop-conf", "a=C");

        Map<String, String> result = option.resolve();
        assertThat(result.keySet(), hasSize(2));
        assertThat(result, hasEntry("a", "C"));
        assertThat(result, hasEntry("b", "B"));
    }

    private File put(String... contents) throws IOException {
        File file = temporary.newFile();
        try (PrintWriter w = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"))) {
            for (String s : contents) {
                w.println(s);
            }
        }
        return file;
    }
}
