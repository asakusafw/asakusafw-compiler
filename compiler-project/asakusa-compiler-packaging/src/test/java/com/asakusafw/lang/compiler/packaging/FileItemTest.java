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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link FileItem}.
 */
public class FileItemTest extends ResourceTestRoot {

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
        FileItem item = item(base, "test.txt", "Hello, world!");
        assertThat(item.toString(), item, hasLocation("test.txt"));
        assertThat(item.toString(), item, hasContents("Hello, world!"));
    }

    /**
     * comparing.
     * @throws Exception if failed
     */
    @Test
    public void compare() throws Exception {
        File base = folder.newFolder();
        FileItem item = item(base, "a");
        assertThat(item, is(item(base, "a")));
        assertThat(item.hashCode(), is(item(base, "a").hashCode()));
        assertThat(item, is(not(item(base, "b"))));
    }
}
