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
package com.asakusafw.lang.compiler.common;

/**
 * Provides API extensions.
 */
public interface ExtensionContainer {

    /**
     * Returns an extension service.
     * @param extension the extension type
     * @param <T> the extension type
     * @return the extension service, or {@code null} if it is not found
     */
    <T> T getExtension(Class<T> extension);

    /**
     * Editable {@link ExtensionContainer}.
     */
    public interface Editable extends ExtensionContainer {

        /**
         * Registers an extension service.
         * @param <T> extension type
         * @param extension extension type
         * @param service extension service, or {@code null} to remove the extension service
         */
        <T> void registerExtension(Class<T> extension, T service);
    }
}
