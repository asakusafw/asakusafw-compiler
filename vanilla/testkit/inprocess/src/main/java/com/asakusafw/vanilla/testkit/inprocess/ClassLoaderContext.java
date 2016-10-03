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
package com.asakusafw.vanilla.testkit.inprocess;

import java.io.IOException;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Configures the thread context class loader.
 * @since 0.4.0
 */
class ClassLoaderContext implements AutoCloseable {

    private final URLClassLoader active;

    private final ClassLoader escaped;

    /**
     * Creates a new instance.
     * @param newClassLoader the new context class loader
     */
    ClassLoaderContext(URLClassLoader newClassLoader) {
        this.active = newClassLoader;
        this.escaped = swap(newClassLoader);
    }

    /**
     * Returns the active class loader.
     * @return the active class loader
     */
    public ClassLoader getClassLoader() {
        return active;
    }

    private static ClassLoader swap(final ClassLoader classLoader) {
        return AccessController.doPrivileged((PrivilegedAction<ClassLoader>) () -> {
            ClassLoader old = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(classLoader);
            return old;
        });
    }

    @Override
    public void close() throws IOException {
        try {
            swap(escaped);
        } finally {
            active.close();
        }
    }
}