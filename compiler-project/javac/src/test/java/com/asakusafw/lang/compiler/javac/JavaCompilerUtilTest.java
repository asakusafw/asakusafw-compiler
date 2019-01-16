/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.javac;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.lang.compiler.common.testing.FileDeployer;

/**
 * Test for {@link JavaCompilerUtil}.
 */
public class JavaCompilerUtilTest {

    /**
     * temporary deployer.
     */
    @Rule
    public final FileDeployer deployer = new FileDeployer();

    /**
     * simple case.
     */
    @Test
    public void simple() {
        List<File> libraries = JavaCompilerUtil.getLibraries(getClass().getClassLoader());
        assertThat(libraries.toString(), find(libraries, getClass().getName()), is(notNullValue()));
    }

    /**
     * using jar.
     * @throws Exception if failed
     */
    @Test
    public void jar() throws Exception {
        File lib = deployer.copy("example.jar", "example.jar");
        try (URLClassLoader loader = loader(Arrays.asList(lib))) {
            List<File> libraries = JavaCompilerUtil.getLibraries(loader);
            assertThat(find(libraries, "com.example.Hello"), is(notNullValue()));
        }
    }

    /**
     * using directory.
     * @throws Exception if failed
     */
    @Test
    public void directory() throws Exception {
        File lib = deployer.extract("example.jar", "example");
        try (URLClassLoader loader = loader(Arrays.asList(lib))) {
            List<File> libraries = JavaCompilerUtil.getLibraries(loader);
            assertThat(find(libraries, "com.example.Hello"), is(notNullValue()));
        }
    }

    /**
     * using unknown file.
     * @throws Exception if failed
     */
    @Test
    public void unknown_file() throws Exception {
        File lib = deployer.copy("example.jar", "example.UNKNWON");
        try (URLClassLoader loader = loader(Arrays.asList(lib))) {
            List<File> libraries = JavaCompilerUtil.getLibraries(loader);
            assertThat(find(libraries, "com.example.Hello"), is(nullValue()));
        }
    }

    /**
     * include/exclude extension libraries.
     */
    @Test
    public void extension_libraries() {
        List<File> include = JavaCompilerUtil.getLibraries(getClass().getClassLoader(), true);
        List<File> exclude = JavaCompilerUtil.getLibraries(getClass().getClassLoader(), false);
        assertThat(exclude.toString(), find(exclude, getClass().getName()), is(notNullValue()));
        assertThat(new HashSet<>(include).containsAll(exclude), is(true));
    }

    private URLClassLoader loader(List<File> files) {
        List<URL> urls = new ArrayList<>();
        for (File file : files) {
            try {
                urls.add(file.toURI().toURL());
            } catch (MalformedURLException e) {
                throw new AssertionError(e);
            }
        }
        return URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]), ClassLoader.getSystemClassLoader());
    }

    private File find(List<File> libraries, String className) {
        String relative = className.replace('.', '/') + ".class";
        for (File file : libraries) {
            if (file.isFile()) {
                try (ZipFile zip = new ZipFile(file)) {
                    ZipEntry entry = zip.getEntry(relative);
                    if (entry != null) {
                        return file;
                    }
                } catch (IOException e) {
                    throw new AssertionError(MessageFormat.format(
                            "failed to open zip: {0}",
                            file), e);
                }
            } else if (file.isDirectory()) {
                File f = new File(file, relative);
                if (f.isFile()) {
                    return file;
                }
            }
        }
        return null;
    }
}
