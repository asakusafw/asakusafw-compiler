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
package com.asakusafw.lang.compiler.tester;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link TesterContext}.
 */
public class TesterContextTest {

    private static final String ENV_HOME = "ASAKUSA_HOME";

    private static final String ENV_APPS = "ASAKUSA_BATCHAPPS_HOME";

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
        File home = folder.newFolder().getAbsoluteFile();

        Map<String, String> env = new LinkedHashMap<>();
        env.put(ENV_HOME, home.getPath());
        TesterContext context = new TesterContext(ClassLoader.getSystemClassLoader(), env);

        assertThat(context.getClassLoader(), is(ClassLoader.getSystemClassLoader()));
        assertThat(context.getEnvironmentVariables(), hasEntry(ENV_HOME, home.getPath()));
        assertThat(context.getFrameworkHome(), is(home));
        assertThat(context.getBatchApplicationHome(), is(new File(home, "batchapps")));
    }

    /**
     * w/ application home.
     * @throws Exception if failed
     */
    @Test
    public void application_home() throws Exception {
        File home = folder.newFolder().getAbsoluteFile();
        File apps = folder.newFolder().getAbsoluteFile();

        Map<String, String> env = new LinkedHashMap<>();
        env.put(ENV_HOME, home.getPath());
        env.put(ENV_APPS, apps.getPath());
        TesterContext context = new TesterContext(ClassLoader.getSystemClassLoader(), env);

        assertThat(context.getClassLoader(), is(ClassLoader.getSystemClassLoader()));
        assertThat(context.getEnvironmentVariables(), hasEntry(ENV_HOME, home.getPath()));
        assertThat(context.getEnvironmentVariables(), hasEntry(ENV_APPS, apps.getPath()));
        assertThat(context.getFrameworkHome(), is(home));
        assertThat(context.getBatchApplicationHome(), is(apps));
    }

    /**
     * w/o framework home.
     * @throws Exception if failed
     */
    @Test(expected = RuntimeException.class)
    public void no_home() throws Exception {
        Map<String, String> env = new LinkedHashMap<>();
        TesterContext context = new TesterContext(ClassLoader.getSystemClassLoader(), env);
        context.getFrameworkHome();
    }

    /**
     * using temporary files.
     * @throws Exception if failed
     */
    @Test
    public void temporary_files() throws Exception {
        File f0 = folder.newFile();
        File f1 = folder.newFolder();
        File f2 = folder.newFolder();
        new File(f2, "contents.txt").createNewFile();

        Map<String, String> env = new LinkedHashMap<>();
        TesterContext context = new TesterContext(ClassLoader.getSystemClassLoader(), env);
        context.addTemporaryFile(f0);
        context.addTemporaryFile(f1);
        context.addTemporaryFile(f2);

        assertThat(f0.exists(), is(true));
        assertThat(f1.exists(), is(true));
        assertThat(f2.exists(), is(true));
        context.removeTemporaryFiles();
        assertThat(f0.exists(), is(false));
        assertThat(f1.exists(), is(false));
        assertThat(f2.exists(), is(false));
    }
}
