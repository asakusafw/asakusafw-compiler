/**
 * Copyright 2011-2015 Asakusa Framework Team.
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

import java.io.Serializable;

/**
 * Represents a diagnostic information.
 */
public interface Diagnostic extends Serializable {

    /**
     * Returns the severity of this.
     * @return the severity
     */
    Level getLevel();

    /**
     * Returns the diagnostic message.
     * @return the diagnostic message
     */
    String getMessage();

    /**
     * Returns the optional causal information.
     * @return the causal exception, or {@code null} if it is not defined
     */
    Exception getException();

    /**
     * Represents severity of {@link Diagnostic}.
     */
    public static enum Level {

        /**
         * Information.
         */
        INFO,

        /**
         * Warning.
         */
        WARN,

        /**
         * Error.
         */
        ERROR,
    }
}
