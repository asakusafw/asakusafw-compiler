package com.asakusafw.lang.compiler.mapreduce;

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

    private static ClassLoader swap(final ClassLoader classLoader) {
        return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
            @Override
            public ClassLoader run() {
                ClassLoader old = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(classLoader);
                return old;
            }
        });
    }
}