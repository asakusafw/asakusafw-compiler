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

import org.junit.Test;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Test for {@link ResourceItemSink}.
 */
public class ResourceItemSinkTest extends ResourceTestRoot {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (ResourceItemSink sink = new ResourceItemSink()) {
            put(sink, item("a.txt", "A"));

            assertThat(sink.getItems(), hasSize(1));
            assertThat(sink.find(Location.of("a.txt")), hasContents("A"));
        }
    }

    /**
     * empty.
     * @throws Exception if failed
     */
    @Test
    public void no_items() throws Exception {
        try (ResourceItemSink sink = new ResourceItemSink()) {
            assertThat(sink.getItems(), hasSize(0));
        }
    }

    /**
     * multiple items.
     * @throws Exception if failed
     */
    @Test
    public void many() throws Exception {
        try (ResourceItemSink sink = new ResourceItemSink()) {
            put(sink, item("a.txt", "A"));
            put(sink, item("b.txt", "B"));
            put(sink, item("c.txt", "C"));

            assertThat(sink.getItems(), hasSize(3));
            assertThat(sink.find(Location.of("a.txt")), hasContents("A"));
            assertThat(sink.find(Location.of("b.txt")), hasContents("B"));
            assertThat(sink.find(Location.of("c.txt")), hasContents("C"));
        }
    }

    /**
     * add using callback.
     * @throws Exception if failed
     */
    @Test
    public void callback() throws Exception {
        try (ResourceItemSink sink = new ResourceItemSink()) {
            sink.add(Location.of("a.txt"), callback("A"));
            sink.add(Location.of("b.txt"), callback("B"));
            sink.add(Location.of("c.txt"), callback("C"));

            assertThat(sink.getItems(), hasSize(3));
            assertThat(sink.find(Location.of("a.txt")), hasContents("A"));
            assertThat(sink.find(Location.of("b.txt")), hasContents("B"));
            assertThat(sink.find(Location.of("c.txt")), hasContents("C"));
        }
    }
}
