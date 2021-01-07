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
package com.asakusafw.lang.compiler.packaging;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.ResourceContainer;

/**
 * Represents a file container.
 */
public class FileContainer implements ResourceContainer, ResourceRepository {

    private final File basePath;

    /**
     * Creates a new instance.
     * @param basePath the base path
     */
    public FileContainer(File basePath) {
        this.basePath = basePath;
    }

    /**
     * Returns the base path of this container.
     * @return the base path
     */
    public File getBasePath() {
        return basePath;
    }

    /**
     * Returns a {@link File} for the location.
     * @param location the target location
     * @return the related file path (may or may not exist)
     */
    public File toFile(Location location) {
        File file = new File(basePath, location.toPath(File.separatorChar)).getAbsoluteFile();
        return file;
    }

    @Override
    public OutputStream addResource(Location location) throws IOException {
        File file = toFile(location);
        if (file.exists()) {
            throw new IOException(MessageFormat.format(
                    "generating file already exists: {0}",
                    file));
        }
        return ResourceUtil.create(file);
    }

    /**
     * Adds a new resource with contents from the input stream.
     * @param location the resource path (relative from the container root)
     * @param contents the resource contents
     * @throws IOException if failed to create a new resource
     */
    public void addResource(Location location, InputStream contents) throws IOException {
        try (OutputStream output = addResource(location)) {
            ResourceUtil.copy(contents, output);
        }
    }

    /**
     * Adds a new resource with contents from the content provider.
     * @param location the resource path (relative from the container root)
     * @param contents the callback object for preparing resource contents
     * @throws IOException if failed to accept the resource by I/O error
     */
    public void addResource(Location location, ContentProvider contents) throws IOException {
        try (OutputStream output = addResource(location)) {
            contents.writeTo(output);
        }
    }

    /**
     * Creates a {@link ResourceSink} for create resources into this container.
     * @return the created sink
     */
    public ResourceSink createSink() {
        return new FileSink(basePath);
    }

    @Override
    public Cursor createCursor() throws IOException {
        return FileRepository.createCursor(basePath);
    }

    /**
     * Accepts a {@link FileVisitor} in this container.
     * @param visitor the visitor
     * @throws IOException if failed to visit files in this container
     */
    public void accept(FileVisitor visitor) throws IOException {
        ResourceUtil.visit(visitor, basePath);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + basePath.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FileContainer other = (FileContainer) obj;
        if (!basePath.equals(other.basePath)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Resource({0})", //$NON-NLS-1$
                basePath);
    }
}
