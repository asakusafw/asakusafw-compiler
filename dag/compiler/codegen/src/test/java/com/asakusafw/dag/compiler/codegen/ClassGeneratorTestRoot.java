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
package com.asakusafw.dag.compiler.codegen;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.compiler.codegen.OperatorNodeGenerator.NodeInfo;
import com.asakusafw.dag.compiler.codegen.testing.MockClassGeneratorContext;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.lang.compiler.common.BasicResourceContainer;
import com.asakusafw.lang.compiler.common.ResourceContainer;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Group;
import com.asakusafw.lang.compiler.model.graph.Groups;
import com.asakusafw.lang.utils.common.Action;

/**
 * Test root for class generators.
 */
public abstract class ClassGeneratorTestRoot {

    private static final String TEMPORARY = "com.asakusafw.generating.Temporary";

    private final AtomicInteger counter = new AtomicInteger();

    /**
     * Classpath.
     */
    @Rule
    public final TemporaryFolder classpath = new TemporaryFolder();

    private ClassGeneratorContext session;

    /**
     * Returns a class generator context.
     * @return the new context
     */
    public final ClassGeneratorContext context() {
        session = session != null ? session : new MockClassGeneratorContext(
                getClass().getClassLoader(),
                classpath());
        return session;
    }

    /**
     * Returns the classpath container.
     * @return the classpath container
     */
    public ResourceContainer classpath() {
        return new BasicResourceContainer(classpath.getRoot());
    }

    /**
     * Adds a temporary class into the current classpath.
     * @param callback the callback
     * @return the target class
     */
    public ClassDescription add(Generating callback) {
        return add(new ClassDescription(TEMPORARY + counter.incrementAndGet()), callback);
    }

    /**
     * Adds a class into the current classpath.
     * @param target the target class
     * @param callback the callback
     * @return the target class
     */
    public ClassDescription add(ClassDescription target, Generating callback) {
        try {
            return add(callback.perform(target));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Adds a temporary class into the current classpath.
     * @param data the class data
     * @return the target class
     */
    public ClassDescription add(ClassData data) {
        data.dump(new BasicResourceContainer(classpath.getRoot()));
        return data.getDescription();
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
     * Creates an adapter object.
     * @param <T> the adapter type
     * @param adapterClass the adapter class
     * @param context the context object
     * @return the created adapter
     */
    @SuppressWarnings("unchecked")
    public <T> T adapter(Class<?> adapterClass, VertexProcessorContext context) {
        try {
            return (T) adapterClass.getConstructor(VertexProcessorContext.class).newInstance(context);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Parses a group elements.
     * @param expressions each element starts with one of {@code '=', '+', '-'}
     * @return the group
     */
    public static Group group(String... expressions) {
        List<String> group = new ArrayList<>();
        List<String> order = new ArrayList<>();
        for (String s : expressions) {
            char operator = s.charAt(0);
            switch (operator) {
            case '=':
                group.add(s.substring(1));
                break;
            case '+':
            case '-':
                order.add(s);
                break;
            default:
                group.add(s);
                break;
            }
        }
        return Groups.parse(group, order);
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

    /**
     * Returns a matcher which checks whether the target node uses a cache of the original node.
     * @param origin the original node
     * @return the matcher
     */
    public static Matcher<NodeInfo> useCacheOf(NodeInfo origin) {
        return new BaseMatcher<NodeInfo>() {
            @Override
            public boolean matches(Object item) {
                ClassData a = ((NodeInfo) item).getClassData();
                ClassData b = origin.getClassData();
                return a.hasContents() == false
                        && b.hasContents()
                        && a.getDescription().equals(b.getDescription());
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("using cache of ").appendValue(origin.getClassData());
            }
        };
    }

    /**
     * Generating classes callback.
     */
    @FunctionalInterface
    public interface Generating {

        /**
         * Performs class generation.
         * @param target the target class
         * @return the generated class data
         * @throws IOException if I/O exception was occurred
         */
        ClassData perform(ClassDescription target) throws IOException;
    }
}
