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
package com.asakusafw.lang.compiler.core;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.Predicate;
import com.asakusafw.lang.compiler.common.testing.FileDeployer;
import com.asakusafw.lang.compiler.packaging.FileRepository;
import com.asakusafw.lang.compiler.packaging.ResourceRepository;
import com.asakusafw.lang.compiler.packaging.ZipRepository;

/**
 * Test for {@link ProjectRepository}.
 */
public class ProjectRepositoryTest {

    private static final Predicate<Object> ANY = new Predicate<Object>() {
        @Override
        public boolean apply(Object argument) {
            return true;
        }
    };

    /**
     * temporary file deployer.
     */
    @Rule
    public final FileDeployer deployer = new FileDeployer();

    /**
     * explore.
     * @throws Exception if failed
     */
    @Test
    public void explore() throws Exception {
        try (ProjectRepository repo = ProjectRepository.builder(getClass().getClassLoader())
                .explore(deployer.copy("example.jar", "example.jar"))
                .build()) {
            assertThat(repo.getClassLoader(), hasClass("com.example.Hello"));
            assertThat(locations(repo.getProjectContents()), hasItem("com/example/Hello.class"));
            assertThat(locations(repo.getProjectContents()), hasItem("com/example/Hello$World.class"));
            assertThat(locations(repo.getEmbeddedContents()), is(empty()));
            assertThat(locations(repo.getAttachedLibraries()), is(empty()));
            Set<Class<?>> classes = repo.getProjectClasses(new Predicate<Class<?>>() {
                @Override
                public boolean apply(Class<?> argument) {
                    return argument.getDeclaringClass() == null;
                }
            });
            assertThat(names(classes), containsInAnyOrder("com.example.Hello"));
        }
    }

    /**
     * explore.
     * @throws Exception if failed
     */
    @Test
    public void explore_dir() throws Exception {
        try (ProjectRepository repo = ProjectRepository.builder(getClass().getClassLoader())
                .explore(deployer.extract("example.jar", "example"))
                .build()) {
            assertThat(repo.getClassLoader(), hasClass("com.example.Hello"));
            assertThat(locations(repo.getProjectContents()), hasItem("com/example/Hello.class"));
            assertThat(locations(repo.getProjectContents()), hasItem("com/example/Hello$World.class"));
            assertThat(locations(repo.getEmbeddedContents()), is(empty()));
            assertThat(locations(repo.getAttachedLibraries()), is(empty()));
            Set<Class<?>> classes = repo.getProjectClasses(new Predicate<Class<?>>() {
                @Override
                public boolean apply(Class<?> argument) {
                    return argument.getDeclaringClass() == null;
                }
            });
            assertThat(names(classes), containsInAnyOrder("com.example.Hello"));
        }
    }

    /**
     * embed.
     * @throws Exception if failed
     */
    @Test
    public void embed() throws Exception {
        try (ProjectRepository repo = ProjectRepository.builder(getClass().getClassLoader())
                .embed(deployer.copy("example.jar", "example.jar"))
                .build()) {
            assertThat(repo.getClassLoader(), hasClass("com.example.Hello"));
            assertThat(locations(repo.getProjectContents()), is(empty()));
            assertThat(locations(repo.getEmbeddedContents()), hasItem("com/example/Hello.class"));
            assertThat(locations(repo.getAttachedLibraries()), is(empty()));
            assertThat(repo.getProjectClasses(ANY), is(empty()));
        }
    }

    /**
     * embed.
     * @throws Exception if failed
     */
    @Test
    public void embed_dir() throws Exception {
        try (ProjectRepository repo = ProjectRepository.builder(getClass().getClassLoader())
                .embed(deployer.extract("example.jar", "example"))
                .build()) {
            assertThat(repo.getClassLoader(), hasClass("com.example.Hello"));
            assertThat(locations(repo.getProjectContents()), is(empty()));
            assertThat(locations(repo.getEmbeddedContents()), hasItem("com/example/Hello.class"));
            assertThat(locations(repo.getAttachedLibraries()), is(empty()));
            assertThat(repo.getProjectClasses(ANY), is(empty()));
        }
    }

    /**
     * embed.
     * @throws Exception if failed
     */
    @Test
    public void embed_repo() throws Exception {
        try (ProjectRepository repo = ProjectRepository.builder(getClass().getClassLoader())
                .embed(new FileRepository(deployer.extract("example.jar", "example.jar")))
                .build()) {
            assertThat("embed(ResourceRepository) does not change class loaders",
                    repo.getClassLoader(), not(hasClass("com.example.Hello")));
            assertThat(locations(repo.getProjectContents()), is(empty()));
            assertThat(locations(repo.getEmbeddedContents()), hasItem("com/example/Hello.class"));
            assertThat(locations(repo.getAttachedLibraries()), is(empty()));
            assertThat(repo.getProjectClasses(ANY), is(empty()));
        }
    }

    /**
     * attach.
     * @throws Exception if failed
     */
    @Test
    public void attach() throws Exception {
        try (ProjectRepository repo = ProjectRepository.builder(getClass().getClassLoader())
                .attach(deployer.copy("example.jar", "example.jar"))
                .build()) {
            assertThat(repo.getClassLoader(), hasClass("com.example.Hello"));
            assertThat(locations(repo.getProjectContents()), is(empty()));
            assertThat(locations(repo.getEmbeddedContents()), is(empty()));
            assertThat(locations(repo.getAttachedLibraries()), is(not(empty())));
            assertThat(deepLocations(repo.getAttachedLibraries()), hasItem("com/example/Hello.class"));
            assertThat(repo.getProjectClasses(ANY), is(empty()));
        }
    }

    /**
     * attach.
     * @throws Exception if failed
     */
    @Test
    public void attach_dir() throws Exception {
        try (ProjectRepository repo = ProjectRepository.builder(getClass().getClassLoader())
                .attach(deployer.extract("example.jar", "example"))
                .build()) {
            assertThat(repo.getClassLoader(), hasClass("com.example.Hello"));
            assertThat(locations(repo.getProjectContents()), is(empty()));
            assertThat(locations(repo.getEmbeddedContents()), is(empty()));
            assertThat(locations(repo.getAttachedLibraries()), is(not(empty())));
            assertThat(deepLocations(repo.getAttachedLibraries()), hasItem("com/example/Hello.class"));
            assertThat(repo.getProjectClasses(ANY), is(empty()));
        }
    }

    /**
     * external.
     * @throws Exception if failed
     */
    @Test
    public void external() throws Exception {
        try (ProjectRepository repo = ProjectRepository.builder(getClass().getClassLoader())
                .external(deployer.copy("example.jar", "example.jar"))
                .build()) {
            assertThat(repo.getClassLoader(), hasClass("com.example.Hello"));
            assertThat(locations(repo.getProjectContents()), is(empty()));
            assertThat(locations(repo.getEmbeddedContents()), is(empty()));
            assertThat(locations(repo.getAttachedLibraries()), is(empty()));
            assertThat(repo.getProjectClasses(ANY), is(empty()));
        }
    }

    /**
     * external.
     * @throws Exception if failed
     */
    @Test
    public void external_dir() throws Exception {
        try (ProjectRepository repo = ProjectRepository.builder(getClass().getClassLoader())
                .external(deployer.extract("example.jar", "example"))
                .build()) {
            assertThat(repo.getClassLoader(), hasClass("com.example.Hello"));
            assertThat(locations(repo.getProjectContents()), is(empty()));
            assertThat(locations(repo.getEmbeddedContents()), is(empty()));
            assertThat(locations(repo.getAttachedLibraries()), is(empty()));
            assertThat(repo.getProjectClasses(ANY), is(empty()));
        }
    }

    private Matcher<ClassLoader> hasClass(final String name) {
        return new BaseMatcher<ClassLoader>() {

            @Override
            public boolean matches(Object item) {
                ClassLoader loader = (ClassLoader) item;
                try {
                    loader.loadClass(name);
                    return true;
                } catch (ClassNotFoundException e) {
                    return false;
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("can load " + name);
            }
        };
    }

    private Set<String> locations(Collection<? extends ResourceRepository> repositories) throws IOException {
        Set<String> results = new LinkedHashSet<>();
        for (ResourceRepository repository : repositories) {
            try (ResourceRepository.Cursor cursor = repository.createCursor()) {
                while (cursor.next()) {
                    results.add(cursor.getLocation().toPath());
                }
            }
        }
        return results;
    }

    private Set<String> deepLocations(Collection<? extends ResourceRepository> repositories) throws IOException {
        List<ResourceRepository> deep = new ArrayList<>();
        for (ResourceRepository repository : repositories) {
            try (ResourceRepository.Cursor cursor = repository.createCursor()) {
                while (cursor.next()) {
                    File copy;
                    try (InputStream contents = cursor.openResource()) {
                        copy = deployer.copy(contents, Location.of("prefix").append(cursor.getLocation()).toPath());
                    }
                    deep.add(new ZipRepository(copy));
                }
            }
        }
        return locations(deep);
    }

    private Set<String> names(Collection<? extends Class<?>> classes) {
        Set<String> results = new HashSet<>();
        for (Class<?> aClass : classes) {
            results.add(aClass.getName());
        }
        return results;
    }
}
