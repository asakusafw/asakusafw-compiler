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
package com.asakusafw.lang.compiler.core.participant;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.Callable;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.core.CompilerTestRoot;
import com.asakusafw.lang.compiler.core.JobflowCompiler;
import com.asakusafw.lang.compiler.core.basic.BasicJobflowCompiler;
import com.asakusafw.lang.compiler.core.dummy.SimpleExternalPortProcessor;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.packaging.FileContainer;

/**
 * Test for {@link JavaSourceExtensionParticipant}.
 */
public class JavaSourceExtensionParticipantTest extends CompilerTestRoot {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        final ClassDescription aClass = new ClassDescription("com.example.JavaSourceExtension");
        initialize(aClass, new String[] {
                "package com.example;",
                String.format(
                        "public class %s implements java.util.concurrent.Callable<String> {",
                        aClass.getSimpleName()),
                "    public String call() { return \"a\"; }",
                "}",
        });
        FileContainer output = container();
        JobflowCompiler.Context context = new JobflowCompiler.Context(context(true), output);
        new BasicJobflowCompiler().compile(
                context,
                batchInfo("b"),
                jobflow("testing"));

        try (URLClassLoader loader = loader(output.getBasePath())) {
            Object result = aClass.resolve(loader).asSubclass(Callable.class).newInstance().call();
            assertThat(result, is((Object) "a"));
        }
    }

    /**
     * w/ invalid version
     * @throws Exception if failed
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_version() throws Exception {
        final ClassDescription aClass = new ClassDescription("com.example.JavaSourceExtension");
        initialize(aClass, new String[] {
                "package com.example;",
                String.format("public class %s {}", aClass.getSimpleName()),
        });
        options.withProperty(JavaSourceExtensionParticipant.KEY_VERSION, "__INVALID__");
        FileContainer output = container();
        JobflowCompiler.Context context = new JobflowCompiler.Context(context(true), output);
        new BasicJobflowCompiler().compile(
                context,
                batchInfo("b"),
                jobflow("testing"));
    }

    /**
     * w/ invalid bootclasspath
     * @throws Exception if failed
     */
    @Test(expected = DiagnosticException.class)
    public void invalid_bootclasspath() throws Exception {
        final ClassDescription aClass = new ClassDescription("com.example.JavaSourceExtension");
        initialize(aClass, new String[] {
                "package com.example;",
                String.format("public class %s {}", aClass.getSimpleName()),
        });
        options.withProperty(JavaSourceExtensionParticipant.KEY_BOOT_CLASSPATH, container().getBasePath().getPath());
        FileContainer output = container();

        JobflowCompiler.Context context = new JobflowCompiler.Context(context(true), output);
        new BasicJobflowCompiler().compile(
                context,
                batchInfo("b"),
                jobflow("testing"));
    }

    private void initialize(final ClassDescription aClass, final String... lines) {
        externalPortProcessors.add(new SimpleExternalPortProcessor());
        compilerParticipants.add(new JavaSourceExtensionParticipant());
        jobflowProcessors.add(new JobflowProcessor() {
            @Override
            public void process(Context context, Jobflow source) throws IOException {
                JavaSourceExtension extension = context.getExtension(JavaSourceExtension.class);
                assertThat(extension, is(notNullValue()));
                try (PrintWriter pw = new PrintWriter(extension.addJavaFile(aClass))) {
                    for (String line : lines) {
                        pw.println(line);
                    }
                }
            }
        });

    }

    private URLClassLoader loader(File path) {
        try {
            return URLClassLoader.newInstance(new URL[] { path.toURI().toURL() });
        } catch (MalformedURLException e) {
            throw new AssertionError(e);
        }
    }
}
