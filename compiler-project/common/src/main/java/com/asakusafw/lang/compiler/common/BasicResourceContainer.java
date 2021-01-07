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
package com.asakusafw.lang.compiler.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;

/**
 * A local file system based resource container.
 */
public class BasicResourceContainer implements ResourceContainer {

    private final File basePath;

    /**
     * Creates a new instance.
     * @param basePath the base path
     */
    public BasicResourceContainer(File basePath) {
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
        File parent = file.getParentFile();
        if (parent.mkdirs() == false && parent.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "failed to prepare a parent directory: {0}",
                    file));
        }
        return new FileOutputStream(file);
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "Resource({0})", //$NON-NLS-1$
                basePath);
    }
}
