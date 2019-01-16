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
package com.asakusafw.dag.compiler.jdbc;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.testing.MockClassGeneratorContext;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.jdbc.JdbcDagTestRoot;
import com.asakusafw.lang.compiler.common.BasicResourceContainer;
import com.asakusafw.lang.compiler.common.ResourceContainer;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * A common utilities for testing JDBC DAG Compiler.
 */
public abstract class JdbcDagCompilerTestRoot extends JdbcDagTestRoot {

    /**
     * Columns mappings.
     */
    public static final List<Tuple<String, PropertyName>> MAPPINGS =
            mappings("M_KEY:key", "M_SORT:sort", "M_VALUE:value");

    /**
     * Classpath.
     */
    @Rule
    public final TemporaryFolder classpath = new TemporaryFolder();

    private ClassGeneratorContext context;

    /**
     * Returns a class generator context.
     * @return the current context
     */
    public ClassGeneratorContext context() {
        if (context == null) {
            context = new MockClassGeneratorContext(
                    getClass().getClassLoader(),
                    classpath());
        }
        return context;
    }

    /**
     * Returns the classpath container.
     * @return the classpath container
     */
    public ResourceContainer classpath() {
        return new BasicResourceContainer(classpath.getRoot());
    }

    /**
     * Adds the generated class.
     * @param data the class data
     */
    public void add(ClassData data) {
        data.dump(classpath());
    }

    /**
     * Adds a generated class and load.
     * @param data the class data
     * @param action the action for the created class loader
     */
    public void add(ClassData data, Action<Class<?>, ?> action) {
        add(data);
        loading(data.getDescription(), action);
    }

    /**
     * Loads a generated class.
     * @param description the target class
     * @param action the action for the created class loader
     */
    public void loading(ClassDescription description, Action<Class<?>, ?> action) {
        loading(cl -> {
            Class<?> aClass = description.resolve(cl);
            action.perform(aClass);
        });
    }

    /**
     * Creates a new class loader for loading previously generated classes.
     * @param action the action for the created class loader
     */
    public void loading(Action<ClassLoader, ?> action) {
        try (URLClassLoader loader = URLClassLoader.newInstance(new URL[] { classpath.getRoot().toURI().toURL() })) {
            action.perform(loader);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Returns property names.
     * @param names the name strings
     * @return the names
     */
    public static List<PropertyName> names(String... names) {
        return Stream.of(names)
                .map(PropertyName::of)
                .collect(Collectors.toList());
    }

    /**
     * Returns property mappings.
     * @param mappings the column-name:property-name pairs
     * @return the mappings
     */
    public static List<Tuple<String, PropertyName>> mappings(String... mappings) {
        return Stream.of(mappings)
                .map(s -> s.split(":", 2))
                .peek(ss -> Invariants.require(ss.length == 2))
                .map(ss -> new Tuple<>(ss[0], PropertyName.of(ss[1])))
                .collect(Collectors.toList());
    }

    /**
     * Returns a matcher which checks whether the target is a cache of the original class data.
     * @param origin the original node
     * @return the matcher
     */
    public static Matcher<ClassData> cacheOf(ClassData origin) {
        return new BaseMatcher<ClassData>() {
            @Override
            public boolean matches(Object item) {
                ClassData a = (ClassData) item;
                ClassData b = origin;
                return a.hasContents() == false
                        && b.hasContents()
                        && a.getDescription().equals(b.getDescription());
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("cache of ").appendValue(origin);
            }
        };
    }
}
