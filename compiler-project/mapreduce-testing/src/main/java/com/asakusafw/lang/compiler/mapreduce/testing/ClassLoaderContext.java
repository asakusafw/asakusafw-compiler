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
package com.asakusafw.lang.compiler.mapreduce.testing;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Configures the thread context class loader.
 */
public class ClassLoaderContext implements AutoCloseable {

    private final ClassLoader escaped;

    /**
     * Creates a new instance.
     * @param newClassLoader the new context class loader
     */
    public ClassLoaderContext(ClassLoader newClassLoader) {
        this.escaped = swap(newClassLoader);
    }

    /**
     * Restore the context class loader.
     */
    @Override
    public void close() {
        swap(escaped);
    }

    private static ClassLoader swap(ClassLoader classLoader) {
        return AccessController.doPrivileged((PrivilegedAction<ClassLoader>) () -> {
            ClassLoader old = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(classLoader);
            return old;
        });
    }
}