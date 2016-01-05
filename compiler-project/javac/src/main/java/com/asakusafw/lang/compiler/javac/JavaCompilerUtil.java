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
package com.asakusafw.lang.compiler.javac;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for {@link JavaCompilerSupport}.
 */
public final class JavaCompilerUtil {

    static final Logger LOG = LoggerFactory.getLogger(JavaCompilerUtil.class);

    static final Pattern PATTERN_ARCHIVE = Pattern.compile(".+\\.(zip|jar)"); //$NON-NLS-1$

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
        List<File> results = new ArrayList<>();
        List<URLClassLoader> loaders = getClassLoaders(classLoader, includeExtensionLibraries);
        for (int i = loaders.size() - 1; i >= 0; i--) {
            URLClassLoader current = loaders.get(i);
            LOG.trace("analyzing class loader: {}", current); //$NON-NLS-1$
            for (URL url : current.getURLs()) {
                LOG.trace("  analyzing class library: {}", url); //$NON-NLS-1$
                File file = toFile(url);
                if (file == null) {
                    LOG.trace("  class library does not have valid file location: {}", url); //$NON-NLS-1$
                    continue;
                }
                if (file.isFile() && PATTERN_ARCHIVE.matcher(file.getName()).matches() == false) {
                    LOG.trace("  class library is not valid archive: {}", file); //$NON-NLS-1$
                    continue;
                }
                results.add(file);
            }
        }
        return results;
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
        } catch (URISyntaxException e) {
            LOG.warn(MessageFormat.format(
                    "invalid library URL: {0}",
                    url), e);
            return null;
        }
    }
}
