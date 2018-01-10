/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link EnginePropertiesOption}.
 */
public class EnginePropertiesOptionTest {

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
        EnginePropertiesOption option = new EnginePropertiesOption();
        assertThat(option.getCommands(), containsInAnyOrder("--engine-conf"));
    }

    /**
     * simple pair.
     * @throws Exception if failed
     */
    @Test
    public void empty_value() throws Exception {
        EnginePropertiesOption option = new EnginePropertiesOption();
        option.accept("--engine-conf", "");

        Map<String, String> result = option.resolve();
        assertThat(result.keySet(), hasSize(0));
    }

    /**
     * simple pair.
     * @throws Exception if failed
     */
    @Test
    public void pair() throws Exception {
        EnginePropertiesOption option = new EnginePropertiesOption();
        option.accept("--engine-conf", "a=A");

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
        EnginePropertiesOption option = new EnginePropertiesOption();
        option.accept("--engine-conf", "a=A");
        option.accept("--engine-conf", "b=B");
        option.accept("--engine-conf", "c=C");

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
        EnginePropertiesOption option = new EnginePropertiesOption();
        option.accept("--engine-conf", "a");

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
        Properties p = new Properties();
        p.setProperty("a", "A");
        p.setProperty("b", "B");
        File file = put(p);

        EnginePropertiesOption option = new EnginePropertiesOption();
        option.accept("--engine-conf", "@" + file.getAbsolutePath());

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
    public void file_skip_empty() throws Exception {
        Properties p = new Properties();
        p.setProperty("a", "A");
        File f = put(p);

        EnginePropertiesOption option = new EnginePropertiesOption();
        option.accept("--engine-conf", "@|" + f.getAbsolutePath());

        Map<String, String> result = option.resolve();
        assertThat(result.toString(), result.keySet(), hasSize(1));
        assertThat(result, hasEntry("a", "A"));
    }

    /**
     * using file.
     * @throws Exception if failed
     */
    @Test
    public void file_first() throws Exception {
        Properties p1 = new Properties();
        p1.setProperty("a", "A");
        File f1 = put(p1);

        Properties p2 = new Properties();
        p2.setProperty("b", "B");
        File f2 = put(p2);

        EnginePropertiesOption option = new EnginePropertiesOption();
        option.accept("--engine-conf", "@" + f1.getAbsolutePath() + "|" + f2.getAbsolutePath());

        Map<String, String> result = option.resolve();
        assertThat(result.toString(), result.keySet(), hasSize(1));
        assertThat(result, hasEntry("a", "A"));
    }

    /**
     * using file.
     * @throws Exception if failed
     */
    @Test
    public void file_second() throws Exception {
        Properties p = new Properties();
        p.setProperty("a", "A");
        File f = put(p);

        EnginePropertiesOption option = new EnginePropertiesOption();
        option.accept("--engine-conf", "@" + f.getAbsolutePath() + ".missing|" + f.getAbsolutePath());

        Map<String, String> result = option.resolve();
        assertThat(result.toString(), result.keySet(), hasSize(1));
        assertThat(result, hasEntry("a", "A"));
    }

    /**
     * using file.
     * @throws Exception if failed
     */
    @Test
    public void file_missing() throws Exception {
        EnginePropertiesOption option = new EnginePropertiesOption();
        option.accept("--engine-conf", "@MISSING");

        Map<String, String> result = option.resolve();
        assertThat(result.toString(), result.keySet(), hasSize(0));
    }

    /**
     * pairs.
     * @throws Exception if failed
     */
    @Test
    public void overwrite() throws Exception {
        EnginePropertiesOption option = new EnginePropertiesOption();
        option.accept("--engine-conf", "a=A");
        option.accept("--engine-conf", "b=B");
        option.accept("--engine-conf", "a=C");

        Map<String, String> result = option.resolve();
        assertThat(result.keySet(), hasSize(2));
        assertThat(result, hasEntry("a", "C"));
        assertThat(result, hasEntry("b", "B"));
    }

    private File put(Properties properties) throws IOException {
        File file = temporary.newFile();
        try (OutputStream output = new FileOutputStream(file)) {
            properties.store(output, null);
        }
        return file;
    }
}
