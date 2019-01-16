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
package com.asakusafw.lang.compiler.javac.testing;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.asakusafw.lang.compiler.javac.BasicJavaCompilerSupport;
import com.asakusafw.lang.compiler.javac.JavaCompilerUtil;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Provides java compiler for testing.
 */
public class JavaCompiler implements JavaSourceExtension, TestRule {

    private final TemporaryFolder temporary = new TemporaryFolder();

    ClassLoader classLoader;

    private BasicJavaCompilerSupport entity;

    @Override
    public Statement apply(Statement base, Description description) {
        return temporary.apply(new Statement() {
            @Override
            public void evaluate() throws Throwable {
                classLoader = description.getTestClass().getClassLoader();
                base.evaluate();
            }
        }, description);
    }

    @Override
    public Writer addJavaFile(ClassDescription aClass) throws IOException {
        if (entity == null) {
            assertThat(classLoader, is(notNullValue()));
            List<File> classpath = JavaCompilerUtil.getLibraries(classLoader);
            entity = new BasicJavaCompilerSupport(temporary.newFolder(), classpath, temporary.newFolder());
        }
        return entity.addJavaFile(aClass);
    }

    /**
     * Compiles and returns the compiler output path.
     * @return the output path
     * @throws IOException if failed
     */
    public File compile() throws IOException {
        if (entity == null) {
            return temporary.newFolder();
        } else {
            entity.process();
            File result = entity.getDestinationPath();
            entity = null;
            return result;
        }
    }

    /**
     * Compiles and returns the class loader for compiled objects.
     * @return the class loader
     * @throws IOException if failed
     */
    public URLClassLoader load() throws IOException {
        assertThat(classLoader, is(notNullValue()));
        File bin = compile();
        return URLClassLoader.newInstance(new URL[] { bin.toURI().toURL() }, classLoader);
    }
}
