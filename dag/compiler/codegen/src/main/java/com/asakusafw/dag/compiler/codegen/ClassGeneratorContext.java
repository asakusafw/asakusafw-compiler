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
package com.asakusafw.dag.compiler.codegen;

import java.util.Optional;
import java.util.function.Supplier;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An abstract super interface of class generator contexts.
 * @since 0.4.0
 */
public interface ClassGeneratorContext {

    /**
     * Returns the current class loader.
     * @return the current class loader
     */
    ClassLoader getClassLoader();

    /**
     * Returns the current data model loader.
     * @return the current data model loader
     */
    DataModelLoader getDataModelLoader();

    /**
     * Returns a unique class name for the target category.
     * @param category the category name (must be a sub-package name)
     * @return the unique class name
     */
    default ClassDescription getClassName(String category) {
        return getClassName(category, null);
    }

    /**
     * Returns a unique class name for the target category.
     * @param category the category name (must be a sub-package name)
     * @param hint the simple class name hint (nullable)
     * @return the unique class name
     */
    ClassDescription getClassName(String category, String hint);

    /**
     * Adds a new Java class file.
     * @param data the class file contents
     * @return the target class description
     * @throws DiagnosticException if an error was occurred while adding the class file
     */
    ClassDescription addClassFile(ClassData data);

    /**
     * Returns a cached class for the specified key.
     * @param key the cache key
     * @return the corresponded cache, or empty if there is no such a cached class
     * @see #addCache(Object, ClassDescription)
     */
    Optional<ClassDescription> findCache(Object key);

    /**
     * Adds a class cache for the specified key only if cache is enabled.
     * @param key the cache key
     * @param target the target class
     * @see #findCache(Object)
     */
    void addCache(Object key, ClassDescription target);

    /**
     * Returns a cached class or add a new class data if it has not been cached.
     * @param key the cache key
     * @param defaultValue the default class data if missing cached class
     * @return the added class data
     * @see #addCache(Object, ClassDescription)
     */
    default ClassData cache(Object key, Supplier<? extends ClassData> defaultValue) {
        return findCache(key)
                .map(ClassData::new)
                .orElseGet(() -> {
                    ClassData data = defaultValue.get();
                    addCache(key, data.getDescription());
                    return data;
                });
    }

    /**
     * Forwarding for {@link ClassGeneratorContext}.
     */
    public interface Forward extends ClassGeneratorContext {

        /**
         * Returns the forwarding target.
         * @return the forwarding target
         */
        ClassGeneratorContext getForward();

        @Override
        default ClassLoader getClassLoader() {
            return getForward().getClassLoader();
        }

        @Override
        default DataModelLoader getDataModelLoader() {
            return getForward().getDataModelLoader();
        }

        @Override
        default ClassDescription getClassName(String category) {
            return ClassGeneratorContext.super.getClassName(category);
        }

        @Override
        default ClassDescription getClassName(String category, String hint) {
            return getForward().getClassName(category, hint);
        }

        @Override
        default ClassDescription addClassFile(ClassData data) {
            return getForward().addClassFile(data);
        }

        @Override
        default void addCache(Object key, ClassDescription target) {
            getForward().addCache(key, target);
        }

        @Override
        default Optional<ClassDescription> findCache(Object key) {
            return getForward().findCache(key);
        }

        @Override
        default ClassData cache(Object key, Supplier<? extends ClassData> defaultValue) {
            return ClassGeneratorContext.super.cache(key, defaultValue);
        }
    }
}
