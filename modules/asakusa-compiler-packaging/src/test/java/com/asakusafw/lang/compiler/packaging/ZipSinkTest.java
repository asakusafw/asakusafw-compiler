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
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Test for {@link ZipSink}.
 */
public class ZipSinkTest extends ResourceTestRoot {

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        File base = folder.newFile();
        try (ZipSink sink = new ZipSink(base)) {
            put(sink, item("a.txt", "A"));
        }
        Map<String, String> items = dump(new ZipRepository(base));
        assertThat(items.keySet(), hasSize(1));
        assertThat(items, hasEntry("a.txt", "A"));
    }

    /**
     * multiple items.
     * @throws Exception if failed
     */
    @Test
    public void many() throws Exception {
        File base = folder.newFile();
        try (ZipSink sink = new ZipSink(base)) {
            put(sink, item("a.txt", "A"));
            put(sink, item("b.txt", "B"));
            put(sink, item("c.txt", "C"));
        }
        Map<String, String> items = dump(new ZipRepository(base));
        assertThat(items.keySet(), hasSize(3));
        assertThat(items, hasEntry("a.txt", "A"));
        assertThat(items, hasEntry("b.txt", "B"));
        assertThat(items, hasEntry("c.txt", "C"));
    }

    /**
     * add using callback.
     * @throws Exception if failed
     */
    @Test
    public void callback() throws Exception {
        File base = folder.newFile();
        try (ZipSink sink = new ZipSink(base)) {
            sink.add(Location.of("a.txt"), callback("A"));
            sink.add(Location.of("b.txt"), callback("B"));
            sink.add(Location.of("c.txt"), callback("C"));
        }
        Map<String, String> items = dump(new ZipRepository(base));
        assertThat(items.keySet(), hasSize(3));
        assertThat(items, hasEntry("a.txt", "A"));
        assertThat(items, hasEntry("b.txt", "B"));
        assertThat(items, hasEntry("c.txt", "C"));
    }
}
