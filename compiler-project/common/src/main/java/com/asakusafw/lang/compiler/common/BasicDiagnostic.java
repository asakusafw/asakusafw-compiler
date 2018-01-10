/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import java.text.MessageFormat;

/**
 * A basic implementation of {@link Diagnostic}.
 */
public final class BasicDiagnostic implements Diagnostic {

    private static final long serialVersionUID = -3671335650818430741L;

    private Level level;

    private String message;

    private Exception cause;

    /**
     * Creates a new instance for serializer.
     */
    protected BasicDiagnostic() {
        return;
    }

    /**
     * Creates a new instance.
     * @param level the severity
     * @param message the diagnostic message
     */
    public BasicDiagnostic(Level level, String message) {
        this.level = level;
        this.message = message;
    }

    /**
     * Creates a new instance.
     * @param level the severity
     * @param message the diagnostic message
     * @param cause the causal exception
     */
    public BasicDiagnostic(Level level, String message, Exception cause) {
        this.level = level;
        this.message = message;
        this.cause = cause;
    }

    @Override
    public Level getLevel() {
        return level;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public Exception getException() {
        return cause;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "[{0}] {1}", //$NON-NLS-1$
                level,
                message);
    }
}