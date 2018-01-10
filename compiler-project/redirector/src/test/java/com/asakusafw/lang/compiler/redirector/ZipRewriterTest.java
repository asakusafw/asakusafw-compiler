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
package com.asakusafw.lang.compiler.redirector;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.redirector.mock.MockCallee0;
import com.asakusafw.lang.compiler.redirector.mock.MockCallee1;
import com.asakusafw.lang.compiler.redirector.mock.MockCallee2;
import com.asakusafw.lang.compiler.redirector.mock.MockCallee3;
import com.asakusafw.lang.compiler.redirector.mock.MockCaller;

/**
 * Test for {@link ZipRewriter}.
 */
public class ZipRewriterTest {

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        Map<String, byte[]> contents = new LinkedHashMap<>();
        contents.put("a.bin", new byte[] { 1, 2, 3 });
        addClass(contents, MockCaller.class);

        RedirectRule rule = new RedirectRule();
        rule.add(MockCallee0.class.getName(), MockCallee2.class.getName());

        File file = save(contents);
        new ZipRewriter(rule).rewrite(file);

        assertThat(apply(file), is("0:2:1"));
        assertThat(dump(file), hasEntry("a.bin", new byte[] { 1, 2, 3 }));
    }

    /**
     * multiple files.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        Map<String, byte[]> contents = new LinkedHashMap<>();
        contents.put("a.bin", new byte[] { 1, 2, 3 });
        addClass(contents, MockCaller.class);

        RedirectRule rule = new RedirectRule();
        rule.add(MockCallee0.class.getName(), MockCallee2.class.getName());
        rule.add(MockCallee1.class.getName(), MockCallee3.class.getName());

        File file = save(contents);
        new ZipRewriter(rule).rewrite(file);

        assertThat(apply(file), is("0:2:3"));
        assertThat(dump(file), hasEntry("a.bin", new byte[] { 1, 2, 3 }));
    }

    private String apply(File file) throws IOException {
        Map<String, byte[]> contents = dump(file);
        String name = VolatileClassLoader.toPath(MockCaller.class);
        assertThat(contents, hasKey(name));

        VolatileClassLoader loader = new VolatileClassLoader(getClass().getClassLoader());
        Class<?> aClass = loader.forceLoad(contents.get(name));
        try {
            return aClass.newInstance().toString();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private Map<String, byte[]> dump(File file) throws IOException, FileNotFoundException {
        Map<String, byte[]> contents;
        try (ZipInputStream input = new ZipInputStream(new FileInputStream(file))) {
            contents = ZipUtil.dump(input);
        }
        return contents;
    }

    private File save(Map<String, byte[]> contents) throws IOException {
        File file = temporary.newFile();
        try (ZipOutputStream output = new ZipOutputStream(new FileOutputStream(file))) {
            ZipUtil.load(output, contents);
        }
        return file;
    }

    private void addClass(Map<String, byte[]> contents, Class<?> aClass) throws IOException {
        String path = VolatileClassLoader.toPath(aClass);
        byte[] bin = VolatileClassLoader.dump(aClass);
        contents.put(path, bin);
    }
}
