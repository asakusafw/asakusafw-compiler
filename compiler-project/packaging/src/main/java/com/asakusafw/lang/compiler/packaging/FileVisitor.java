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
package com.asakusafw.lang.compiler.packaging;

import java.io.File;
import java.io.IOException;

import com.asakusafw.lang.compiler.common.Location;

/**
 * A file visitor in containers.
 */
@FunctionalInterface
public interface FileVisitor {

    /**
     * Processes the target file.
     * @param location the resource path (relative from the container root)
     * @param file the target file
     * @return {@code true} if also visits each element in the current directory
     * @throws IOException if failed to process the target file
     */
    boolean process(Location location, File file) throws IOException;
}