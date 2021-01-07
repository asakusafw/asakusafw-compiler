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
package com.asakusafw.lang.compiler.core.util;

import java.io.Closeable;

import com.asakusafw.lang.compiler.packaging.FileContainer;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;

/**
 * Provides {@link Closeable} feature for {@link FileContainer}s.
 */
public class FileContainerCleaner implements Closeable {

    private final FileContainer container;

    /**
     * Creates a new instance.
     * @param container the holding file container
     */
    public FileContainerCleaner(FileContainer container) {
        this.container = container;
    }

    /**
     * Returns the holding file container.
     * @return the holding file container
     */
    public FileContainer getContainer() {
        return container;
    }

    @Override
    public void close() {
        ResourceUtil.delete(container.getBasePath());
    }
}
