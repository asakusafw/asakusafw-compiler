/**
 * Copyright 2011-2016 Asakusa Framework Team.
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.testing.FileEditor;

/**
 * Test for {@link FilePackageBuilder}.
 */
public class FilePackageBuilderTest extends ResourceTestRoot {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    /**
     * adds repository.
     * @throws Exception if failed
     */
    @Test
    public void repository() throws Exception {
        FilePackageBuilder builder = new FilePackageBuilder();
        builder.add(repository(new ResourceItem[] {
                item("hello1.txt", "Hello1"),
                item("hello2.txt", "Hello2"),
                item("hello3.txt", "Hello3"),
        }));

        File base = folder.getRoot();
        builder.build(base);

        assertThat(FileEditor.get(new File(base, "hello1.txt")), contains("Hello1"));
        assertThat(FileEditor.get(new File(base, "hello2.txt")), contains("Hello2"));
        assertThat(FileEditor.get(new File(base, "hello3.txt")), contains("Hello3"));
    }

    /**
     * adds item.
     * @throws Exception if failed
     */
    @Test
    public void item() throws Exception {
        FilePackageBuilder builder = new FilePackageBuilder();
        builder.add(item("hello.txt", "Hello, world!"));

        File base = folder.getRoot();
        builder.build(base);

        assertThat(FileEditor.get(new File(base, "hello.txt")), contains("Hello, world!"));
    }

    /**
     * adds provider.
     * @throws Exception if failed
     */
    @Test
    public void provider() throws Exception {
        FilePackageBuilder builder = new FilePackageBuilder();
        builder.add(Location.of("hello.txt"), provider("Hello, world!"));

        File base = folder.getRoot();
        builder.build(base);

        assertThat(FileEditor.get(new File(base, "hello.txt")), contains("Hello, world!"));
    }

    /**
     * adds visitor.
     * @throws Exception if failed
     */
    @Test
    public void visitor() throws Exception {
        FilePackageBuilder builder = new FilePackageBuilder();
        builder.add(item("hello.txt", "Hello"));
        builder.add(item("hello.csv", "Hello"));
        builder.add(item("hello.bin", "Hello"));
        builder.add(new FileVisitor() {
            @Override
            public boolean process(Location location, File file) throws IOException {
                if (file.getName().endsWith(".bin")) {
                    assertThat(file.delete(), is(true));
                }
                return false;
            }
        });

        File base = folder.getRoot();
        builder.build(base);

        assertThat(new File(base, "hello.txt").exists(), is(true));
        assertThat(new File(base, "hello.csv").exists(), is(true));
        assertThat(new File(base, "hello.bin").exists(), is(false));
    }
}
