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
package com.asakusafw.bridge.launch;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link ExtensionLoader}.
 */
public class ExtensionLoaderTest {

    private static final Charset ENCODING = StandardCharsets.UTF_8;

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private final Map<String, String> env = new LinkedHashMap<>();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        register(env, "testing", put("Hello, world!"));

        ExtensionLoader loader = new ExtensionLoader(env);
        assertThat(loader.getAvailableExtensions(), containsInAnyOrder("testing"));
        assertThat(loader.isAvailable("testing"), is(true));
        assertThat(loader.isAvailable("other"), is(false));
        assertThat(get(loader, "testing"), is("Hello, world!"));
    }

    /**
     * ignore other environment variables.
     * @throws Exception if failed
     */
    @Test
    public void not_extension() throws Exception {
        env.put("INVALID_testing", put("Hello, world!").getAbsolutePath());

        ExtensionLoader loader = new ExtensionLoader(env);
        assertThat(loader.getAvailableExtensions(), is(empty()));
    }

    /**
     * ignore missing files.
     * @throws Exception if failed
     */
    @Test
    public void missing() throws Exception {
        register(env, "testing", put("Hello, world!"));
        register(env, "missing", new File(temporary.newFolder(), "MISSING"));

        ExtensionLoader loader = new ExtensionLoader(env);
        assertThat(loader.getAvailableExtensions(), containsInAnyOrder("testing"));
        assertThat(loader.isAvailable("testing"), is(true));
        assertThat(loader.isAvailable("missing"), is(false));
    }

    private void register(Map<String, String> map, String extension, File file) {
        map.put(ExtensionLoader.ENV_EXTENSION_PREFIX + extension, file.getAbsolutePath());
    }

    private File put(String contents) throws IOException {
        File file = temporary.newFile();
        try (OutputStream output = new FileOutputStream(file)) {
            output.write(contents.getBytes(ENCODING));
        }
        return file;
    }

    private String get(ExtensionLoader loader, String name) throws IOException {
        try (InputStream input = loader.open(name)) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buf = new byte[256];
            while (true) {
                int read = input.read(buf);
                if (read < 0) {
                    break;
                }
                output.write(buf, 0, read);
            }
            return new String(output.toByteArray(), ENCODING);
        }
    }
}
