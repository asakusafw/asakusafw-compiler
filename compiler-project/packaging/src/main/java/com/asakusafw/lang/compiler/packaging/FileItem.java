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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An implementation of {@link ResourceItem} using local file.
 */
public class FileItem implements ResourceItem {

    private final Location location;

    private final File file;

    /**
     * Creates a new instance.
     * @param location the file location
     * @param file the target file
     */
    public FileItem(Location location, File file) {
        this.location = location;
        this.file = file;
    }

    @Override
    public Location getLocation() {
        return location;
    }

    @Override
    public InputStream openResource() throws IOException {
        return new FileInputStream(file);
    }

    @Override
    public void writeTo(OutputStream output) throws IOException {
        try (InputStream input = openResource()) {
            ResourceUtil.copy(input, output);
        }
    }

    /**
     * Returns the file.
     * @return the file
     */
    public File getFile() {
        return file;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + location.hashCode();
        result = prime * result + file.hashCode();
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
        FileItem other = (FileItem) obj;
        if (!location.equals(other.location)) {
            return false;
        }
        if (!file.equals(other.file)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "FileItem({0}=>{1})", //$NON-NLS-1$
                location,
                file);
    }
}
