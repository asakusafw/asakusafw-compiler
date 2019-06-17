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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.MessageFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for {@link JavaCompilerSupport}.
 * @since 0.1.0
 * @version 0.5.4
 */
public final class JavaCompilerUtil {

    static final Logger LOG = LoggerFactory.getLogger(JavaCompilerUtil.class);

    static final String EXTENSION_ZIP = ".zip"; //$NON-NLS-1$

    static final String EXTENSION_JAR = ".jar"; //$NON-NLS-1$

    private JavaCompilerUtil() {
        return;
    }

    /**
     * Returns library files from the target class loader.
     * @param classLoader the target class loader
     * @return the related library files
     * @see #getLibraries(ClassLoader, boolean)
     */
    public static List<File> getLibraries(ClassLoader classLoader) {
        return getLibraries(classLoader, false);
    }

    /**
     * Returns library files from the target class loader.
     * @param classLoader the target class loader
     * @param includeExtensionLibraries {@code true} to include extension libraries, otherwise {@code false}
     * @return the related library files
     */
    public static List<File> getLibraries(ClassLoader classLoader, boolean includeExtensionLibraries) {
        return getLibraries(classLoader, includeExtensionLibraries, false);
    }

    /**
     * Returns library files from the target class loader.
     * @param classLoader the target class loader
     * @param includeExtensionLibraries {@code true} to include extension libraries, otherwise {@code false}
     * @param resolveManifestClasspath resolves {@code Class-Path} entries in {@code MANIFEST.MF}
     * @return the related library files
     */
    public static List<File> getLibraries(
            ClassLoader classLoader,
            boolean includeExtensionLibraries,
            boolean resolveManifestClasspath) {
        List<File> results = new ArrayList<>();
        List<URLClassLoader> loaders = getClassLoaders(classLoader, includeExtensionLibraries);
        for (int i = loaders.size() - 1; i >= 0; i--) {
            URLClassLoader current = loaders.get(i);
            LOG.trace("analyzing class loader: {}", current); //$NON-NLS-1$
            URL[] urls = current.getURLs();
            for (URL url : urls) {
                LOG.trace("  analyzing class library: {}", url); //$NON-NLS-1$
                File file = toFile(url);
                if (file == null) {
                    LOG.trace("  class library does not have valid file location: {}", url); //$NON-NLS-1$
                    continue;
                }
                if (file.isFile() && !isArchiveName(file.getName())) {
                    LOG.trace("  class library is not valid archive: {}", file); //$NON-NLS-1$
                    continue;
                }
                if (resolveManifestClasspath) {
                    // collect all Class-Path entries
                    results.addAll(collectClassPath(file));
                } else {
                    results.add(file);
                }
            }
        }
        return results;
    }

    private static boolean isArchiveName(String name) {
        String s = name.toLowerCase();
        return s.endsWith(EXTENSION_JAR) || s.endsWith(EXTENSION_ZIP);
    }

    private static List<URLClassLoader> getClassLoaders(ClassLoader classLoader, boolean includeExtensionLibraries) {
        List<URLClassLoader> results = new ArrayList<>();
        ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        for (ClassLoader current = classLoader; current != null; current = current.getParent()) {
            if (current instanceof URLClassLoader) {
                results.add((URLClassLoader) current);
            } else {
                LOG.debug("unsupported library class loader: {}", current); //$NON-NLS-1$
            }
            if (includeExtensionLibraries == false && current == systemClassLoader) {
                break;
            }
        }
        return results;
    }

    private static List<File> collectClassPath(File path) {
        List<File> results = new ArrayList<>();
        Deque<File> work = new ArrayDeque<>();
        work.addFirst(path);
        while (!work.isEmpty()) {
            File entry = work.removeFirst();
            results.add(entry);
            // find "Class-Path" entries from MANIFEST.MF by DFS
            if (entry.isFile() && isArchiveName(entry.getName())) {
                List<File> classpath = extractClassPathAttribute(entry);
                for (int i = classpath.size() - 1; i >= 0; --i) {
                    work.addFirst(classpath.get(i));
                }
            }
        }
        return results;
    }

    private static List<File> extractClassPathAttribute(File file) {
        try (JarFile jar = new JarFile(file)) {
            Manifest manifest = jar.getManifest();
            if (manifest != null) {
                Attributes attrs = manifest.getMainAttributes();
                String classpath = (String) attrs.get(Attributes.Name.CLASS_PATH);
                if (classpath != null) {
                    LOG.debug("extracting Class-Path entries: {}", file);
                    File base = file.getParentFile();
                    return extractClassPathAttributeValue(base, classpath);
                }
            }
        } catch (IOException e) {
            LOG.warn(MessageFormat.format(
                    "failed to extract Class-Path entry: {0}",
                    file), e);
        }
        return Collections.emptyList();
    }

    private static List<File> extractClassPathAttributeValue(File base, String entries) {
        URL baseUrl;
        try {
            baseUrl = new URL(base.toURI().toASCIIString());
        } catch (MalformedURLException e) {
            LOG.warn(MessageFormat.format(
                    "invalid library URL: {0}",
                    base), e);
            return Collections.emptyList();
        }
        return Arrays.stream(entries.split("\\s+")) //$NON-NLS-1$
                .map(String::trim)
                .filter(it -> !it.isEmpty())
                .flatMap(it -> {
                    LOG.trace("resolving Class-Path entry: {}", it);
                    URL url;
                    try {
                        url = new URL(baseUrl, it);
                    } catch (MalformedURLException e) {
                        LOG.warn(MessageFormat.format(
                                "invalid library URL: {0}",
                                it), e);
                        return Stream.empty();
                    }
                    File file = toFile(url);
                    if (file != null) {
                        return Stream.of(file);
                    }
                    return Stream.empty();
                })
                .collect(Collectors.toList());
    }

    private static File toFile(URL url) {
        String protocol = url.getProtocol();
        if (protocol == null || protocol.equals("file") == false) { //$NON-NLS-1$
            LOG.debug("unsupported library URL: {}", url); //$NON-NLS-1$
            return null;
        }
        try {
            URI uri = url.toURI();
            File file = new File(uri);
            return file;
        } catch (URISyntaxException | IllegalArgumentException e) {
            LOG.warn(MessageFormat.format(
                    "invalid library URL: {0}",
                    url), e);
            return null;
        }
    }
}
