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
package com.asakusafw.lang.compiler.core;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.JarOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.packaging.FileItem;
import com.asakusafw.lang.compiler.packaging.FileRepository;
import com.asakusafw.lang.compiler.packaging.ResourceItem;
import com.asakusafw.lang.compiler.packaging.ResourceItemRepository;
import com.asakusafw.lang.compiler.packaging.ResourceRepository;
import com.asakusafw.lang.compiler.packaging.ResourceSink;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;
import com.asakusafw.lang.compiler.packaging.ZipSink;

/**
 * Provides project information.
 */
public class ProjectRepository implements Closeable {

    static final Logger LOG = LoggerFactory.getLogger(ProjectRepository.class);

    static final String EXTENSION_CLASS = ".class"; //$NON-NLS-1$

    static final String EXTENSION_ARCHIVE = ".jar"; //$NON-NLS-1$

    private final ClassLoader classLoader;

    private final List<ResourceRepository> projectContents;

    private final List<ResourceRepository> embeddedContents;

    private final List<ResourceRepository> attachedLibraries;

    private final List<File> temporaryLibraries;

    /**
     * Creates a new instance.
     * Clients use {@link #builder(ClassLoader)} instead of directly use this constructor.
     * @param classLoader the project class loader
     * @param projectContents repositories which provide project contents
     * @param embeddedContents repositories which provide embedded contents for each jobflow package
     * @param attachedLibraries a repository which provides attached library files
     * @param temporaryLibraries temporary library files which must be delete on dispose this object
     */
    public ProjectRepository(
            ClassLoader classLoader,
            Collection<? extends ResourceRepository> projectContents,
            Collection<? extends ResourceRepository> embeddedContents,
            Collection<? extends ResourceRepository> attachedLibraries,
            Collection<File> temporaryLibraries) {
        this.classLoader = classLoader;
        this.projectContents = Collections.unmodifiableList(new ArrayList<>(projectContents));
        this.embeddedContents = Collections.unmodifiableList(new ArrayList<>(embeddedContents));
        this.attachedLibraries = Collections.unmodifiableList(new ArrayList<>(attachedLibraries));
        this.temporaryLibraries = new ArrayList<>(temporaryLibraries);
    }

    /**
     * Creates a new builder.
     * @param parent the parent class loader
     * @return the created builder
     */
    public static Builder builder(ClassLoader parent) {
        return new Builder(parent);
    }

    /**
     * Returns the project class loader.
     * @return the project class loader
     */
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Returns the project contents.
     * @return the project contents
     */
    public List<ResourceRepository> getProjectContents() {
        return projectContents;
    }

    /**
     * Returns the embedded contents.
     * @return the embedded contents
     */
    public List<ResourceRepository> getEmbeddedContents() {
        return embeddedContents;
    }

    /**
     * Returns the attached library resources.
     * @return the attached library resources
     */
    public List<ResourceRepository> getAttachedLibraries() {
        return attachedLibraries;
    }

    /**
     * Returns the project classes.
     * @param predicate the class acceptor
     * @return loaded classes
     * @throws IOException if failed to obtain classes
     */
    public Set<Class<?>> getProjectClasses(Predicate<? super Class<?>> predicate) throws IOException {
        Set<Class<?>> results = new HashSet<>();
        for (ResourceRepository repository : projectContents) {
            try (ResourceRepository.Cursor cursor = repository.createCursor()) {
                while (cursor.next()) {
                    Location location = cursor.getLocation();
                    Class<?> aClass = loadClassFile(location);
                    if (aClass == null || results.contains(aClass)) {
                        continue;
                    }
                    if (predicate.test(aClass) == false) {
                        continue;
                    }
                    results.add(aClass);
                }
            }
        }
        return results;
    }

    private Class<?> loadClassFile(Location location) {
        if (location.getName().endsWith(EXTENSION_CLASS) == false) {
            return null;
        }
        String name = location.toPath('.');
        assert name.length() >= EXTENSION_CLASS.length();
        name = name.substring(0, name.length() - EXTENSION_CLASS.length());

        try {
            LOG.trace("loading project class: {}", name); //$NON-NLS-1$
            return getClassLoader().loadClass(name);
        } catch (ClassNotFoundException e) {
            LOG.warn(MessageFormat.format(
                    "failed to load project class: {0}",
                    name));
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        if (classLoader instanceof Closeable) {
            ((Closeable) classLoader).close();
        }
        delete(temporaryLibraries);
    }

    static void delete(List<File> files) {
        for (Iterator<File> iter = files.iterator(); iter.hasNext();) {
            File file = iter.next();
            if (file.exists()) {
                if (ResourceUtil.delete(file) == false) {
                    LOG.warn(MessageFormat.format(
                            "failed to delete a temporary file: {0}",
                            file));
                }
            }
            iter.remove();
        }
    }

    /**
     * A builder for {@link ProjectRepository}.
     */
    public static class Builder {

        private static final String TEMP_FILE_PREFIX = "asakusa-lib"; //$NON-NLS-1$

        private final ClassLoader baseClassLoader;

        private final Set<File> libraryFiles = new LinkedHashSet<>();

        private final Set<File> projectContents = new LinkedHashSet<>();

        private final Set<File> embeddedContents = new LinkedHashSet<>();

        private final Set<File> attachedLibraries = new LinkedHashSet<>();

        private final Set<ResourceRepository> embeddedItems = new LinkedHashSet<>();

        /**
         * Creates a new instance.
         * @param baseClassLoader the base class loader
         */
        public Builder(ClassLoader baseClassLoader) {
            this.baseClassLoader = baseClassLoader;
        }

        /**
         * Uses the library as batch class search path.
         * This file will not be included into results.
         * @param file the library file or directory
         * @return this
         * @throws IllegalArgumentException if the target file does not exist
         */
        public Builder explore(File file) {
            checkFile(file);
            projectContents.add(file);
            libraryFiles.add(file);
            return this;
        }

        /**
         * Embeds the library into each jobflow package.
         * @param file the library file or directory
         * @return this
         * @throws IllegalArgumentException if the target file does not exist
         */
        public Builder embed(File file) {
            checkFile(file);
            embeddedContents.add(file);
            libraryFiles.add(file);
            return this;
        }

        /**
         * Embeds items in the repository into each jobflow package.
         * Note that, items in the repository will not be included into
         * {@link ProjectRepository#getClassLoader() the project class loader}.
         * @param repository the repository
         * @return this
         */
        public Builder embed(ResourceRepository repository) {
            embeddedItems.add(repository);
            return this;
        }

        /**
         * Attaches the library into each batch package.
         * @param file the library file or directory
         * @return this
         * @throws IllegalArgumentException if the target file does not exist
         */
        public Builder attach(File file) {
            checkFile(file);
            attachedLibraries.add(file);
            libraryFiles.add(file);
            return this;
        }

        /**
         * Uses the external library file.
         * This file will not be included into results.
         * @param file the library file or directory
         * @return this
         * @throws IllegalArgumentException if the target file does not exist
         */
        public Builder external(File file) {
            checkFile(file);
            libraryFiles.add(file);
            return this;
        }

        private void checkFile(File file) {
            if (file.exists() == false) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "library file is not found: {0}",
                        file));
            }
        }

        /**
         * Builds a class loader from added libraries.
         * @return the built class loader
         */
        public URLClassLoader buildClassLoader() {
            List<URL> urls = new ArrayList<>();
            for (File file : libraryFiles) {
                try {
                    URL location = file.toURI().toURL();
                    urls.add(location);
                } catch (MalformedURLException e) {
                    LOG.warn(MessageFormat.format(
                            "failed to obtain the URL of library file: {0}",
                            file), e);
                    continue;
                }
            }
            return URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]), baseClassLoader);
        }

        /**
         * Builds {@link ProjectRepository} object from added libraries.
         * @return the built object
         * @throws IOException if failed to load libraries
         */
        public ProjectRepository build() throws IOException {
            Set<ResourceRepository> project = buildRepositories(projectContents);
            Set<ResourceRepository> embedded = buildRepositories(embeddedContents);
            embedded.addAll(embeddedItems);
            List<ResourceItem> attachedItems = new ArrayList<>();
            List<File> temporary = new ArrayList<>();
            boolean success = false;
            try {
                for (File file : attachedLibraries) {
                    if (file.isFile()) {
                        attachedItems.add(new FileItem(Location.of(file.getName()), file));
                    } else if (file.isDirectory()) {
                        File jar = buildJar(file);
                        temporary.add(jar);
                        attachedItems.add(new FileItem(Location.of(file.getName() + EXTENSION_ARCHIVE), jar));
                    } else {
                        continue;
                    }
                }
                URLClassLoader classLoader = buildClassLoader();
                success = true;
                return new ProjectRepository(
                        classLoader,
                        project,
                        embedded,
                        Collections.singleton(new ResourceItemRepository(attachedItems, false)),
                        temporary);
            } finally {
                if (success == false) {
                    delete(temporary);
                }
            }
        }

        private Set<ResourceRepository> buildRepositories(Set<File> files) throws IOException {
            Set<ResourceRepository> results = new LinkedHashSet<>();
            for (File file : files) {
                if (file.exists() == false) {
                    continue;
                }
                ResourceRepository repo = ResourceUtil.toRepository(file);
                results.add(repo);
            }
            return results;
        }

        private File buildJar(File file) throws IOException {
            ResourceRepository source = new FileRepository(file);
            return buildJar(source);
        }

        private File buildJar(ResourceRepository source) throws IOException {
            File temporary = File.createTempFile(TEMP_FILE_PREFIX, EXTENSION_ARCHIVE);
            boolean success = false;
            try {
                try (JarOutputStream output = new JarOutputStream(new FileOutputStream(temporary));
                        ResourceSink sink = new ZipSink(output)) {
                    ResourceUtil.copy(source, sink);
                }
                success = true;
            } finally {
                if (success == false) {
                    ResourceUtil.delete(temporary);
                }
            }
            return temporary;
        }
    }
}
