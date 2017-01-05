/**
 * Copyright 2011-2017 Asakusa Framework Team.
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

import java.util.HashMap;
import java.util.Map;

/**
 * A basic implementation of {@link ExtensionContainer}.
 */
public class BasicExtensionContainer implements ExtensionContainer.Editable {

    private final Map<Class<?>, Object> extensions = new HashMap<>();

    @Override
    public <T> void registerExtension(Class<T> extension, T service) {
        if (service == null) {
            extensions.remove(extension);
        } else {
            extensions.put(extension, service);
        }
    }

    @Override
    public <T> T getExtension(Class<T> extension) {
        Object service = extensions.get(extension);
        if (service == null) {
            return null;
        }
        return extension.cast(service);
    }
}
