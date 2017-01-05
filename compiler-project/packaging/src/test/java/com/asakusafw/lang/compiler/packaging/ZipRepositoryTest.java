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
package com.asakusafw.lang.compiler.packaging;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.testing.FileDeployer;
import com.asakusafw.lang.compiler.packaging.ResourceRepository.Cursor;

/**
 * Test for {@link ZipRepository}.
 */
public class ZipRepositoryTest {

    /**
     * temporary deployer.
     */
    @Rule
    public FileDeployer deployer = new FileDeployer();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        ZipRepository repository = new ZipRepository(open("single.zip"));
        Cursor cur = repository.createCursor();
        Map<String, List<String>> entries = drain(cur);

        Map<String, List<String>> expected = new HashMap<>();
        expected.put("hello.txt", Arrays.asList("Hello, world!"));

        assertThat(entries, is(expected));
    }

    /**
     * read multiple files.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        ZipRepository repository = new ZipRepository(open("multiple.zip"));
        Cursor cur = repository.createCursor();
        Map<String, List<String>> entries = drain(cur);

        Map<String, List<String>> expected = new HashMap<>();
        expected.put("a.txt", Arrays.asList("aaa"));
        expected.put("b.txt", Arrays.asList("bbb"));
        expected.put("c.txt", Arrays.asList("ccc"));

        assertThat(entries, is(expected));
    }

    /**
     * read directory structure.
     * @throws Exception if failed
     */
    @Test
    public void structured() throws Exception {
        ZipRepository repository = new ZipRepository(open("structured.zip"));
        Cursor cur = repository.createCursor();
        Map<String, List<String>> entries = drain(cur);

        Map<String, List<String>> expected = new HashMap<>();
        expected.put("a.txt", Arrays.asList("aaa"));
        expected.put("a/b.txt", Arrays.asList("bbb"));
        expected.put("a/b/c.txt", Arrays.asList("ccc"));

        assertThat(entries, is(expected));
    }

    /**
     * not a zip archive.
     * @throws Exception if failed
     */
    @Test(expected = IOException.class)
    public void notarchive() throws Exception {
        ZipRepository repository = new ZipRepository(open("notarchive.zip"));
        Cursor cur = repository.createCursor();
        drain(cur);
    }

    /**
     * missing file.
     * @throws Exception if failed
     */
    @Test(expected = IOException.class)
    public void missing() throws Exception {
        ZipRepository file = new ZipRepository(deployer.getFile("missing"));
        fail(file.toString());
    }

    /**
     * comparing.
     * @throws Exception if failed
     */
    @Test
    public void compare() throws Exception {
        File a = deployer.getFile("a");
        File b = deployer.getFile("b");
        a.createNewFile();
        b.createNewFile();

        ZipRepository repo = new ZipRepository(a);
        assertThat(repo.toString(), repo, is(new ZipRepository(a)));
        assertThat(repo.hashCode(), is(new ZipRepository(a).hashCode()));
        assertThat(repo, is(not(new ZipRepository(b))));
    }

    private File open(String name) {
        String path = "ResourceRepository.files/" + name;
        return deployer.copy(path, name);
    }

    private Map<String, List<String>> drain(Cursor cur) throws IOException {
        try {
            Map<String, List<String>> entries = new TreeMap<>();
            while (cur.next()) {
                Location location = cur.getLocation();
                try (Scanner scanner = new Scanner(cur.openResource(), "UTF-8")) {
                    List<String> contents = new ArrayList<>();
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine();
                        contents.add(line);
                    }
                    entries.put(location.toPath('/'), contents);
                }
            }
            return entries;
        } finally {
            cur.close();
        }
    }
}
