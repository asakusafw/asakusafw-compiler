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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.testing.FileDeployer;

/**
 * Test for {@link ResourceUtil}.
 */
public class ResourceUtilTest extends ResourceTestRoot {

    /**
     * temporary deployer.
     */
    @Rule
    public FileDeployer deployer = new FileDeployer();

    /**
     * item from stream.
     * @throws Exception if failed
     */
    @Test
    public void item_stream() throws Exception {
        ResourceItem item;
        try (InputStream in = new ByteArrayInputStream("Hello, world!".getBytes(ENCODING))) {
            item = ResourceUtil.toItem(Location.of("a.txt"), in);
        }
        assertThat(item, hasLocation("a.txt"));
        assertThat(item, hasContents("Hello, world!"));
    }

    /**
     * item from properties.
     * @throws Exception if failed
     */
    @Test
    public void item_properties() throws Exception {
        Properties props = new Properties();
        props.setProperty("testing", "Hello, world!");

        ResourceItem item = ResourceUtil.toItem(Location.of("testing.properties"), props);
        assertThat(item, hasLocation("testing.properties"));
        Properties restored = new Properties();
        try (InputStream input = item.openResource()) {
            restored.load(input);
        }
        assertThat(restored, is(props));
    }

    /**
     * item from a class.
     * @throws Exception if failed
     */
    @Test
    public void item_class() throws Exception {
        File lib = deployer.copy(local("hello.jar"), "lib.jar");
        try (URLClassLoader loader = loader(lib)) {
            Class<?> aClass = loader.loadClass("com.example.Hello");
            UrlItem item = ResourceUtil.toItem(aClass);

            assertThat(item.getLocation(), is(Location.of("com/example/Hello.class")));

            // can open?
            item.openResource().close();
        }
    }

    /**
     * repository from directory.
     * @throws Exception if failed
     */
    @Test
    public void repository_directory() throws Exception {
        File file = deployer.extract(local("files.zip"), "files");
        ResourceRepository repository = ResourceUtil.toRepository(file);

        Map<String, String> items = dump(repository);
        assertThat(items.keySet(), hasSize(3));
        assertThat(items, hasEntry("a.txt", "aaa"));
        assertThat(items, hasEntry("b.txt", "bbb"));
        assertThat(items, hasEntry("c.txt", "ccc"));
    }

    /**
     * repository from archive.
     * @throws Exception if failed
     */
    @Test
    public void repository_archive() throws Exception {
        File file = deployer.copy(local("files.zip"), "files.zip");
        ResourceRepository repository = ResourceUtil.toRepository(file);

        Map<String, String> items = dump(repository);
        assertThat(items.keySet(), hasSize(3));
        assertThat(items, hasEntry("a.txt", "aaa"));
        assertThat(items, hasEntry("b.txt", "bbb"));
        assertThat(items, hasEntry("c.txt", "ccc"));
    }

    /**
     * copies repository into sink.
     * @throws Exception if failed
     */
    @Test
    public void copy_to_sink() throws Exception {
        File file = deployer.copy(local("files.zip"), "files.zip");
        ResourceRepository source = ResourceUtil.toRepository(file);

        File target = deployer.newFolder();
        try (ResourceSink sink = new FileSink(target)) {
            ResourceUtil.copy(source, sink);
        }

        Map<String, String> items = dump(new FileRepository(target));
        assertThat(items.keySet(), hasSize(3));
        assertThat(items, hasEntry("a.txt", "aaa"));
        assertThat(items, hasEntry("b.txt", "bbb"));
        assertThat(items, hasEntry("c.txt", "ccc"));
    }

    /**
     * library from class.
     * @throws Exception if failed
     */
    @Test
    public void directory_by_class() throws Exception {
        File lib = deployer.extract(local("hello.jar"), "lib");
        try (URLClassLoader loader = loader(lib)) {
            Class<?> a = loader.loadClass("com.example.Hello");
            Class<?> b = loader.loadClass("com.example.Hello$World");
            assertThat(ResourceUtil.findLibraryByClass(a), is(lib));
            assertThat(ResourceUtil.findLibraryByClass(b), is(lib));
        }
    }

    /**
     * library from class.
     * @throws Exception if failed
     */
    @Test
    public void archive_by_class() throws Exception {
        File lib = deployer.copy(local("hello.jar"), "lib.jar");
        try (URLClassLoader loader = loader(lib)) {
            Class<?> a = loader.loadClass("com.example.Hello");
            Class<?> b = loader.loadClass("com.example.Hello$World");
            assertThat(ResourceUtil.findLibraryByClass(a), is(lib));
            assertThat(ResourceUtil.findLibraryByClass(b), is(lib));
        }
    }

    /**
     * libraries from resource.
     * @throws Exception if failed
     */
    @Test
    public void library_by_resource() throws Exception {
        File a = deployer.copy(local("files.zip"), "a.zip");
        File b = deployer.copy(local("structured.zip"), "b.zip");
        File c = deployer.extract(local("files.zip"), "c");
        File d = deployer.extract(local("hello.jar"), "d");
        try (URLClassLoader loader = loader(a, b, c, d)) {
            Set<File> results = ResourceUtil.findLibrariesByResource(loader, Location.of("a.txt"));
            assertThat(results, containsInAnyOrder(a, b, c));
        }
    }

    /**
     * delete file.
     * @throws Exception if failed
     */
    @Test
    public void delete_file() throws Exception {
        File file = deployer.copy(local("files.zip"), "files.zip");

        assertThat(file.exists(), is(true));

        assertThat(ResourceUtil.delete(file), is(true));
        assertThat(file.exists(), is(false));

        assertThat(ResourceUtil.delete(file), is(false));
    }

    /**
     * delete directory.
     * @throws Exception if failed
     */
    @Test
    public void delete_directory() throws Exception {
        File file = deployer.extract(local("files.zip"), "files.zip");

        assertThat(file.exists(), is(true));

        assertThat(ResourceUtil.delete(file), is(true));
        assertThat(file.exists(), is(false));

        assertThat(ResourceUtil.delete(file), is(false));
    }

    private String local(String name) {
        return String.format("ResourceUtil.files/%s", name);
    }

    private URLClassLoader loader(File... files) throws MalformedURLException {
        List<URL> urls = new ArrayList<>();
        for (File file : files) {
            urls.add(file.toURI().toURL());
        }
        return URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]));
    }
}
