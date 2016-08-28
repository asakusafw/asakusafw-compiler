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
package com.asakusafw.lang.compiler.packaging;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An implementation of {@link ResourceRepository} on a local directory.
 */
public class FileRepository implements ResourceRepository {

    static final Logger LOG = LoggerFactory.getLogger(FileRepository.class);

    private final File root;

    /**
     * Creates a new instance.
     * @param root the repository root directory
     * @throws IOException if the directory is not valid
     */
    public FileRepository(File root) throws IOException {
        if (root.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "{0} must be a valid directory",
                    root));
        }
        this.root = root.getAbsoluteFile().getCanonicalFile();
    }

    @Override
    public Cursor createCursor() throws IOException {
        return createCursor(root);
    }

    /**
     * Creates a cursor for enumerating files in the directory.
     * @param root the base directory
     * @return the created cursor
     */
    public static Cursor createCursor(File root) {
        if (root.isDirectory() == false) {
            return Cursor.EMPTY;
        }
        List<FileItem> results = new ArrayList<>();
        collect(results, null, root);
        Collections.sort(results, (o1, o2) -> o1.getLocation().compareTo(o2.getLocation()));
        return new ResourceItemRepository.ItemCursor(results.iterator());
    }

    private static void collect(List<FileItem> results, Location location, File file) {
        assert results != null;
        assert file != null;
        if (file.isFile()) {
            assert location != null : file;
            results.add(new FileItem(location, file));
        } else if (file.isDirectory()) {
            // the current location may be null
            for (File child : file.listFiles()) {
                Location enter = new Location(location, child.getName());
                collect(results, enter, child);
            }
        }
    }
    /**
     * Accepts a {@link FileVisitor} in this repository.
     * @param visitor the visitor
     * @throws IOException if failed to visit files in this repository
     */
    public void accept(FileVisitor visitor) throws IOException {
        ResourceUtil.visit(visitor, root);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + root.hashCode();
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
        FileRepository other = (FileRepository) obj;
        if (!root.equals(other.root)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "File({0})", //$NON-NLS-1$
                root);
    }
}
