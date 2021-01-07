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
package com.asakusafw.vanilla.bootstrap;

/**
 * Represents that execution preconditions are not satisfied.
 * @since 0.5.0
 */
public class ConfigurationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance.
     * @param message the exception message
     */
    public ConfigurationException(String message) {
        super(message);
    }

    /**
     * Creates a new instance.
     * @param message the exception message
     * @param cause the original cause (nullable)
     */
    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
