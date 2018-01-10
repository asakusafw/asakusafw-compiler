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
package com.asakusafw.lang.compiler.core.basic;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.jar.JarFile;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.ResourceContainer;
import com.asakusafw.lang.compiler.packaging.ByteArrayItem;
import com.asakusafw.lang.compiler.packaging.FileContainer;
import com.asakusafw.lang.compiler.packaging.ResourceRepository;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;
import com.asakusafw.lang.compiler.packaging.ZipRepository;

/**
 * Test for {@link JobflowPackager}.
 */
public class JobflowPackagerTest {

    private static final Charset ENCODING = StandardCharsets.UTF_8;

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        FileContainer batch = container();
        FileContainer jobflow = container();
        put(jobflow, "test.txt", "Hello, world!");

        Map<String, String> results = dump(process(batch, jobflow));
        assertThat(results, hasEntry("test.txt", "Hello, world!"));
        assertThat(results, not(hasKey(JarFile.MANIFEST_NAME)));
        assertThat(results, not(hasKey(JobflowPackager.FRAGMENT_MARKER.toPath())));
    }

    /**
     * w/ manifest.
     * @throws Exception if failed
     */
    @Test
    public void w_manifest() throws Exception {
        FileContainer batch = container();
        FileContainer jobflow = container();
        put(jobflow, "test.txt", "Hello, world!");
        put(jobflow, JarFile.MANIFEST_NAME, "manifest");

        Map<String, String> results = dump(process(batch, jobflow));
        assertThat(results, hasEntry("test.txt", "Hello, world!"));
        assertThat(results, hasKey(JarFile.MANIFEST_NAME));
    }

    /**
     * w/ fragment marker.
     * @throws Exception if failed
     */
    @Test
    public void w_marker() throws Exception {
        FileContainer batch = container();
        FileContainer jobflow = container();
        put(jobflow, "test.txt", "Hello, world!");
        put(jobflow, JobflowPackager.FRAGMENT_MARKER.toPath(), "marker");

        Map<String, String> results = dump(process(batch, jobflow));
        assertThat(results, hasEntry("test.txt", "Hello, world!"));
        assertThat(results, hasKey(JobflowPackager.FRAGMENT_MARKER.toPath()));
    }

    /**
     * embed.
     * @throws Exception if failed
     */
    @Test
    public void embed() throws Exception {
        FileContainer batch = container();
        FileContainer jobflow = container();
        FileContainer embed = container();
        put(jobflow, "test.txt", "Hello, world!");
        put(jobflow, "embed.txt", "Hello, embed!");

        Map<String, String> results = dump(process(batch, jobflow, embed));
        assertThat(results, hasEntry("test.txt", "Hello, world!"));
        assertThat(results, hasEntry("embed.txt", "Hello, embed!"));
    }

    /**
     * embed w/ manifest.
     * @throws Exception if failed
     */
    @Test
    public void embed_w_manifest() throws Exception {
        FileContainer batch = container();
        FileContainer jobflow = container();
        FileContainer embed = container();
        put(jobflow, "test.txt", "Hello, world!");
        put(embed, "embed.txt", "Hello, embed!");
        put(embed, JarFile.MANIFEST_NAME, "manifest");

        Map<String, String> results = dump(process(batch, jobflow, embed));
        assertThat(results, hasEntry("test.txt", "Hello, world!"));
        assertThat(results, hasEntry("embed.txt", "Hello, embed!"));
        assertThat(results, not(hasKey(JarFile.MANIFEST_NAME)));
    }

    /**
     * embed w/ manifest.
     * @throws Exception if failed
     */
    @Test
    public void embed_w_marker() throws Exception {
        FileContainer batch = container();
        FileContainer jobflow = container();
        FileContainer embed = container();
        put(jobflow, "test.txt", "Hello, world!");
        put(embed, "embed.txt", "Hello, embed!");
        put(embed, JobflowPackager.FRAGMENT_MARKER.toPath(), "marker");

        Map<String, String> results = dump(process(batch, jobflow, embed));
        assertThat(results, hasEntry("test.txt", "Hello, world!"));
        assertThat(results, hasEntry("embed.txt", "Hello, embed!"));
        assertThat(results, not(hasKey(JobflowPackager.FRAGMENT_MARKER.toPath())));
    }

    ResourceRepository process(FileContainer batch, FileContainer jobflow, ResourceRepository... embedded) {
        String flowId = "testing";
        try {
            new JobflowPackager().process(flowId, batch, jobflow, Arrays.asList(embedded));
            return new ZipRepository(batch.toFile(JobflowPackager.getLibraryLocation(flowId)));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private FileContainer container() {
        try {
            return new FileContainer(folder.newFolder());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void put(ResourceContainer container, String path, String contents) {
        try (OutputStream output = container.addResource(Location.of(path))) {
            output.write(contents.getBytes(ENCODING));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private Map<String, String> dump(ResourceRepository repository) {
        Map<String, String> results = new LinkedHashMap<>();
        try (ResourceRepository.Cursor cursor = repository.createCursor()) {
            while (cursor.next()) {
                try (InputStream input = cursor.openResource()) {
                    ByteArrayItem item = ResourceUtil.toItem(cursor.getLocation(), input);
                    results.put(cursor.getLocation().toPath(), new String(item.getContents(), ENCODING));
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return results;
    }
}
