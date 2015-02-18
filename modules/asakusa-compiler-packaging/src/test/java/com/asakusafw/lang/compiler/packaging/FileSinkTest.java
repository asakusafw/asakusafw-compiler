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

/**
 * Test for {@link FileSink}.
 */
public class FileSinkTest extends ResourceTestRoot {

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
        File base = folder.newFolder();
        try (FileSink sink = new FileSink(base)) {
            put(sink, item("a.txt", "A"));
        }
        Map<String, String> items = dump(new FileRepository(base));
        assertThat(items.keySet(), hasSize(1));
        assertThat(items, hasEntry("a.txt", "A"));
    }

    /**
     * empty items.
     * @throws Exception if failed
     */
    @Test
    public void no_items() throws Exception {
        File base = folder.newFolder();
        new FileSink(base).close();
        Map<String, String> items = dump(new FileRepository(base));
        assertThat(items.keySet(), hasSize(0));
    }

    /**
     * multiple items.
     * @throws Exception if failed
     */
    @Test
    public void many() throws Exception {
        File base = folder.newFolder();
        try (FileSink sink = new FileSink(base)) {
            put(sink, item("a.txt", "A"));
            put(sink, item("b.txt", "B"));
            put(sink, item("c.txt", "C"));
        }
        Map<String, String> items = dump(new FileRepository(base));
        assertThat(items.keySet(), hasSize(3));
        assertThat(items, hasEntry("a.txt", "A"));
        assertThat(items, hasEntry("b.txt", "B"));
        assertThat(items, hasEntry("c.txt", "C"));
    }
}
