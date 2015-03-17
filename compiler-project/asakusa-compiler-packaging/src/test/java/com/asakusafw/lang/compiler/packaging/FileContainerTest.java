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
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.testing.FileDeployer;
import com.asakusafw.lang.compiler.common.testing.FileEditor;

/**
 * Test for {@link FileContainer}.
 */
public class FileContainerTest extends ResourceTestRoot {

    /**
     * temporary deployer.
     */
    @Rule
    public FileDeployer deployer = new FileDeployer();

    /**
     * adds a resource by output stream.
     * @throws Exception if failed
     */
    @Test
    public void add_output() throws Exception {
        File base = deployer.getFile("base");
        FileContainer container = new FileContainer(base);
        try (OutputStream output = container.addResource(Location.of("a.txt"))) {
            output.write("Hello, world!".getBytes(ENCODING));
        }
        assertThat(FileEditor.get(new File(base, "a.txt")), contains("Hello, world!"));
    }

    /**
     * adds a resource by input stream.
     * @throws Exception if failed
     */
    @Test
    public void add_input() throws Exception {
        File base = deployer.getFile("base");
        FileContainer container = new FileContainer(base);
        ByteArrayItem item = item("a.txt", "Hello, world!");
        try (InputStream input = item.openResource()) {
            container.addResource(item.getLocation(), input);
        }
        assertThat(FileEditor.get(new File(base, "a.txt")), contains("Hello, world!"));
    }

    /**
     * adds a resource by content provider.
     * @throws Exception if failed
     */
    @Test
    public void add_provider() throws Exception {
        File base = deployer.getFile("base");
        FileContainer container = new FileContainer(base);
        ByteArrayItem item = item("a.txt", "Hello, world!");
        container.addResource(item.getLocation(), item);
        assertThat(FileEditor.get(new File(base, "a.txt")), contains("Hello, world!"));
    }

    /**
     * adds a resource twice.
     * @throws Exception if failed
     */
    @Test(expected = IOException.class)
    public void add_conflict() throws Exception {
        File base = deployer.getFile("base");
        FileContainer container = new FileContainer(base);
        ByteArrayItem item = item("a.txt", "Hello, world!");
        container.addResource(item.getLocation(), item);
        container.addResource(item.getLocation(), item);
    }

    /**
     * use as repository.
     * @throws Exception if failed
     */
    @Test
    public void repository() throws Exception {
        FileContainer container = new FileContainer(open("structured.zip"));
        Map<String, String> dump = dump(container);
        assertThat(dump.keySet(), hasSize(3));
        assertThat(dump, hasEntry("a.txt", "aaa"));
        assertThat(dump, hasEntry("a/b.txt", "bbb"));
        assertThat(dump, hasEntry("a/b/c.txt", "ccc"));
    }

    /**
     * use as repository.
     * @throws Exception if failed
     */
    @Test
    public void repository_missing() throws Exception {
        FileContainer container = new FileContainer(deployer.getFile("missing"));
        Map<String, String> dump = dump(container);
        assertThat(dump.keySet(), hasSize(0));
    }

    /**
     * use as sink.
     * @throws Exception if failed
     */
    @Test
    public void sink() throws Exception {
        FileContainer container = new FileContainer(open("structured.zip"));
        FileContainer target = new FileContainer(deployer.getFile("target"));
        try (ResourceSink sink = target.createSink()) {
            ResourceUtil.copy(container, sink);
        }
        Map<String, String> dump = dump(target);
        assertThat(dump.keySet(), hasSize(3));
        assertThat(dump, hasEntry("a.txt", "aaa"));
        assertThat(dump, hasEntry("a/b.txt", "bbb"));
        assertThat(dump, hasEntry("a/b/c.txt", "ccc"));
    }

    /**
     * visit.
     * @throws Exception if failed
     */
    @Test
    public void visit() throws Exception {
        FileContainer container = new FileContainer(open("structured.zip"));
        final Set<String> locations = new HashSet<>();
        container.accept(new FileVisitor() {
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
     * equality.
     * @throws Exception if failed
     */
    @Test
    public void equality() throws Exception {
        File a = deployer.getFile("a");
        File b = deployer.getFile("b");
        a.mkdirs();
        b.mkdirs();

        FileContainer repo = new FileContainer(a);
        assertThat(repo.toString(), repo, is(new FileContainer(a)));
        assertThat(repo.hashCode(), is(new FileContainer(a).hashCode()));
        assertThat(repo, is(not(new FileContainer(b))));
    }

    private File open(String name) {
        String path = "ResourceRepository.files/" + name;
        return deployer.extract(path, name);
    }
}
