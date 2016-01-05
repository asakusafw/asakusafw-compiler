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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import com.asakusafw.lang.compiler.common.Location;

/**
 * Accepts resources.
 */
public interface ResourceSink extends Closeable {

    /**
     * Accepts an resource.
     * @param location the resource location
     * @param contents the resource contents (will be consumed immediately)
     * @throws IOException if failed to accept the resource by I/O error
     */
    void add(Location location, InputStream contents) throws IOException;

    /**
     * Accepts an resource.
     * @param location the resource location
     * @param provider the callback object for preparing resource contents
     * @throws IOException if failed to accept the resource by I/O error
     */
    void add(Location location, ContentProvider provider) throws IOException;
}
