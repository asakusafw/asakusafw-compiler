/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import java.io.ByteArrayOutputStream;

import org.junit.Test;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Test for {@link ByteArrayItem}.
 */
public class ByteArrayItemTest extends ResourceTestRoot {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        byte[] contents = "Hello, world!".getBytes(ENCODING);
        ByteArrayItem item = new ByteArrayItem(Location.of("test.txt"), contents);
        assertThat(item.toString(), item, hasLocation("test.txt"));
        assertThat(item.toString(), item, hasContents("Hello, world!"));

        assertThat(item.getContents(), is(contents));

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        item.writeTo(buf);
    }

    /**
     * comparing.
     * @throws Exception if failed
     */
    @Test
    public void compare() throws Exception {
        ByteArrayItem item = item("a", "A");
        assertThat(item, is(item("a", "A")));
        assertThat(item.hashCode(), is(item("a", "A").hashCode()));
        assertThat(item, is(not(item("a", "B"))));
        assertThat(item, is(not(item("b", "A"))));
        assertThat(item, is(not(item("b", "B"))));
    }
}
