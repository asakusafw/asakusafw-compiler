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
package com.asakusafw.lang.compiler.packaging;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.testing.FileDeployer;
import com.asakusafw.lang.compiler.packaging.ResourceRepository.Cursor;

/**
 * Test for {@link FileRepository}.
 */
public class FileRepositoryTest {

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
        FileRepository repository = new FileRepository(open("single.zip"));
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
        FileRepository repository = new FileRepository(open("multiple.zip"));
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
        FileRepository repository = new FileRepository(open("structured.zip"));
        Cursor cur = repository.createCursor();
        Map<String, List<String>> entries = drain(cur);

        Map<String, List<String>> expected = new HashMap<>();
        expected.put("a.txt", Arrays.asList("aaa"));
        expected.put("a/b.txt", Arrays.asList("bbb"));
        expected.put("a/b/c.txt", Arrays.asList("ccc"));

        assertThat(entries, is(expected));
    }

    /**
     * empty directory.
     * @throws Exception if failed
     */
    @Test
    public void empty_repo() throws Exception {
        File dir = deployer.getFile("empty");
        dir.mkdirs();
        FileRepository repository = new FileRepository(dir);
        Cursor cur = repository.createCursor();
        Map<String, List<String>> entries = drain(cur);

        Map<String, List<String>> expected = new HashMap<>();

        assertThat(entries, is(expected));
    }

    /**
     * missing file.
     * @throws Exception if failed
     */
    @Test(expected = IOException.class)
    public void missing() throws Exception {
        FileRepository file = new FileRepository(deployer.getFile("missing"));
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
        a.mkdirs();
        b.mkdirs();

        FileRepository repo = new FileRepository(a);
        assertThat(repo.toString(), repo, is(new FileRepository(a)));
        assertThat(repo.hashCode(), is(new FileRepository(a).hashCode()));
        assertThat(repo, is(not(new FileRepository(b))));
    }

    /**
     * visits all.
     * @throws Exception if failed
     */
    @Test
    public void visit_all() throws Exception {
        FileRepository repository = new FileRepository(open("structured.zip"));
        final Set<String> locations = new HashSet<>();
        repository.accept(new FileVisitor() {
            @Override
            public boolean process(Location location, File file) throws IOException {
                if (file.isFile()) {
                    locations.add(location.toPath());
                }
                return true;
            }
        });
        assertThat(locations, containsInAnyOrder("a.txt", "a/b.txt", "a/b/c.txt"));
    }

    /**
     * visits flat.
     * @throws Exception if failed
     */
    @Test
    public void visit_flat() throws Exception {
        FileRepository repository = new FileRepository(open("structured.zip"));
        final Set<String> locations = new HashSet<>();
        repository.accept(new FileVisitor() {
            @Override
            public boolean process(Location location, File file) throws IOException {
                if (file.isFile()) {
                    locations.add(location.toPath());
                }
                return false;
            }
        });
        assertThat(locations, containsInAnyOrder("a.txt"));
    }

    private File open(String name) {
        String path = "ResourceRepository.files/" + name;
        return deployer.extract(path, name);
    }

    private Map<String, List<String>> drain(Cursor cur) throws IOException {
        try {
            Map<String, List<String>> entries = new TreeMap<>();
            while (cur.next()) {
                Location location = cur.getLocation();
                InputStream input = cur.openResource();
                try {
                    List<String> contents = new ArrayList<>();
                    Scanner scanner = new Scanner(input, "UTF-8");
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine();
                        contents.add(line);
                    }
                    entries.put(location.toPath('/'), contents);
                    scanner.close();
                } finally {
                    input.close();
                }
            }
            return entries;
        } finally {
            cur.close();
        }
    }
}
