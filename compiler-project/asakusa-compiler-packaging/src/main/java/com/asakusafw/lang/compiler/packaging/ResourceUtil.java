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
package com.asakusafw.lang.compiler.packaging;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Utilities for packaging.
 */
public final class ResourceUtil {

    static final Logger LOG = LoggerFactory.getLogger(ResourceUtil.class);

    private static final String CLASS_EXTENSION = ".class"; //$NON-NLS-1$

    private ResourceUtil() {
        return;
    }

    /**
     * Creates a {@link ResourceItem} from the input stream.
     * @param location the resource location
     * @param contents the resource contents
     * @return the created item
     * @throws IOException if failed to load resource contents from the input stream
     */
    public static ByteArrayItem toItem(Location location, InputStream contents) throws IOException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            ResourceUtil.copy(contents, output);
            return new ByteArrayItem(location, output.toByteArray());
        }
    }

    /**
     * Creates a {@link ResourceItem} from the properties object.
     * @param location the resource location
     * @param properties the properties object
     * @return the created item
     */
    public static ByteArrayItem toItem(Location location, Properties properties) {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            properties.store(output, null);
            return new ByteArrayItem(location, output.toByteArray());
        } catch (IOException e) {
            // may not occur
            throw new AssertionError(e);
        }
    }

    /**
     * Creates a {@link ResourceItem} from the class file.
     * @param aClass the target class
     * @return the created item
     * @throws IOException if failed to locate the target class file
     */
    public static UrlItem toItem(Class<?> aClass) throws IOException {
        Location location = toResourceLocation(aClass);
        URL url = toUrl(aClass);
        if (url == null) {
            throw new IOException(MessageFormat.format(
                    "Failed to locate the class file: {0}",
                    aClass.getName()));
        }
        return new UrlItem(location, url);
    }

    /**
     * Creates a {@link ResourceRepository} from file.
     * @param file the target file
     * @return a {@link ResourceRepository} for enumerating contents in the file
     * @throws IOException if failed to create a {@link ResourceRepository}
     */
    public static ResourceRepository toRepository(File file) throws IOException {
        if (file.isDirectory()) {
            return new FileRepository(file);
        } else if (file.isFile()) {
            return new ZipRepository(file);
        } else {
            throw new FileNotFoundException(file.toString());
        }
    }

    /**
     * Copies resources from {@link ResourceRepository} into {@link ResourceSink}.
     * @param source the source repository
     * @param sink the target sink
     * @throws IOException if error occurred while copying resources
     */
    public static void copy(ResourceRepository source, ResourceSink sink) throws IOException {
        try (ResourceRepository.Cursor cursor = source.createCursor()) {
            while (cursor.next()) {
                try (InputStream contents = cursor.openResource()) {
                    sink.add(cursor.getLocation(), contents);
                }
            }
        }
    }

    /**
     * Returns library file which contains the target class.
     * @param aClass the target class
     * @return the related library file, or {@code null} if the library file is not found
     */
    public static File findLibraryByClass(Class<?> aClass) {
        URL resource = toUrl(aClass);
        if (resource == null) {
            LOG.warn(MessageFormat.format(
                    "Failed to locate the class file: {0}",
                    aClass.getName()));
            return null;
        }
        String resourcePath = toResourceLocation(aClass).toPath();
        return findLibraryFromUrl(resource, resourcePath);
    }

    private static Location toResourceLocation(Class<?> aClass) {
        return Location.of(aClass.getName().replace('.', '/') + CLASS_EXTENSION);
    }

    private static URL toUrl(Class<?> aClass) {
        String className = aClass.getName();
        int start = className.lastIndexOf('.') + 1;
        String name = className.substring(start);
        URL resource = aClass.getResource(name + CLASS_EXTENSION);
        return resource;
    }

    /**
     * Returns library paths from the element resource location.
     * @param classLoader the target class loader
     * @param location the element resource location
     * @return the libraries which contain the specified resource, or an empty set if there is no such a resource
     * @throws IOException if failed to obtain libraries by I/O error
     */
    public static Set<File> findLibrariesByResource(ClassLoader classLoader, Location location) throws IOException {
        String path = location.toPath('/');
        Enumeration<URL> resources = classLoader.getResources(path);
        Set<File> results = new LinkedHashSet<>();
        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            File library = findLibraryFromUrl(url, path);
            if (library != null) {
                LOG.debug("found library of \"{}\": {}", path, library); //$NON-NLS-1$
                results.add(library);
            }
        }
        return results;
    }

    private static File findLibraryFromUrl(URL resource, String resourcePath) {
        assert resource != null;
        assert resourcePath != null;
        String protocol = resource.getProtocol();
        if (protocol.equals("file")) { //$NON-NLS-1$
            try {
                File file = new File(resource.toURI());
                return toClassPathRoot(file, resourcePath);
            } catch (URISyntaxException e) {
                LOG.warn(MessageFormat.format(
                        "Failed to locate the library path (cannot convert to local file): {0}",
                        resource), e);
                return null;
            }
        }
        if (protocol.equals("jar")) { //$NON-NLS-1$
            String path = resource.getPath();
            return toClassPathRoot(path, resourcePath);
        } else {
            LOG.warn(MessageFormat.format(
                    "Failed to locate the library path (unsupported protocol {0}): {1}",
                    resource,
                    resourcePath));
            return null;
        }
    }

    private static File toClassPathRoot(File resourceFile, String resourcePath) {
        assert resourceFile != null;
        assert resourcePath != null;
        assert resourceFile.isFile();
        File current = resourceFile.getParentFile();
        assert current != null && current.isDirectory() : resourceFile;
        for (int start = resourcePath.indexOf('/'); start >= 0; start = resourcePath.indexOf('/', start + 1)) {
            current = current.getParentFile();
            if (current == null || current.isDirectory() == false) {
                LOG.warn(MessageFormat.format(
                        "Failed to locate the library path: {0} ({1})",
                        resourceFile,
                        resourcePath));
                return null;
            }
        }
        return current;
    }

    private static File toClassPathRoot(String uriQualifiedPath, String resourceName) {
        assert uriQualifiedPath != null;
        assert resourceName != null;
        int entry = uriQualifiedPath.lastIndexOf('!');
        String qualifier;
        if (entry >= 0) {
            qualifier = uriQualifiedPath.substring(0, entry);
        } else {
            qualifier = uriQualifiedPath;
        }
        URI archive;
        try {
            archive = new URI(qualifier);
        } catch (URISyntaxException e) {
            LOG.warn(MessageFormat.format(
                    "Failed to locate the JAR library file {0}: {1}",
                    qualifier,
                    resourceName),
                    e);
            throw new UnsupportedOperationException(qualifier, e);
        }
        if (archive.getScheme().equals("file") == false) { //$NON-NLS-1$
            LOG.warn(MessageFormat.format(
                    "Failed to locate the library path (unsupported protocol {0}): {1}",
                    archive,
                    resourceName));
            return null;
        }
        File file = new File(archive);
        assert file.isFile() : file;
        return file;
    }

    /**
     * Deletes file recursively.
     * @param file the target file
     * @return {@code true} if file is successfully deleted, otherwise {@code false}
     */
    public static boolean delete(File file) {
        if (file.exists() == false) {
            return false;
        }
        boolean deleted = true;
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleted &= delete(child);
            }
        }
        if (deleted && file.delete() == false) {
            LOG.warn(MessageFormat.format(
                    "failed to delete file: {0}",
                    file));
        }
        return deleted;
    }

    /**
     * Creates a new directory.
     * @param path the target path
     * @throws IOException if failed to create a directory
     */
    static void mkdir(File path) throws IOException {
        if (path.mkdirs() == false && path.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "failed to create directory: {0}",
                    path));
        }
    }

    /**
     * Returns the file of the location.
     * @param base the base directory
     * @param location the relative location from the base directory
     * @return the related file
     */
    static File toFile(File base, Location location) {
        return new File(base, location.toPath(File.separatorChar));
    }

    /**
     * Creates a file.
     * @param file the target file
     * @return the output stream for the created file
     * @throws IOException if failed to create the file by I/O error
     */
    static OutputStream create(File file) throws IOException {
        mkdir(file.getAbsoluteFile().getParentFile());
        return new FileOutputStream(file);
    }

    /**
     * Copies contents via stream.
     * @param input the input
     * @param output the output
     * @throws IOException if failed to copy by I/O error
     */
    static void copy(InputStream input, OutputStream output) throws IOException {
        byte[] buf = new byte[256];
        while (true) {
            int read = input.read(buf);
            if (read < 0) {
                break;
            }
            output.write(buf, 0, read);
        }
    }

    static void visit(FileVisitor visitor, File file) throws IOException {
        if (file.exists() == false) {
            return;
        }
        if (file.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "visit target must be a directory: {0}",
                    file));
        }
        visitElements(visitor, null, file);
    }

    private static void visitElements(FileVisitor visitor, Location current, File directory) throws IOException {
        assert directory.isDirectory();
        for (File file : directory.listFiles()) {
            Location location = new Location(current, file.getName());
            boolean enter = visitor.process(location, file);
            if (enter && file.isDirectory()) {
                visitElements(visitor, location, file);
            }
        }
    }
}
