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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Represents an exception with {@link Diagnostic}s.
 * The compiler may throw this exception from anywhere.
 */
public class DiagnosticException extends RuntimeException implements Adaptable {

    private static final long serialVersionUID = 1L;

    private static final Diagnostic.Level[] SEARCH_ORDER = new Diagnostic.Level[] {
        Diagnostic.Level.ERROR,
        Diagnostic.Level.WARN,
    };

    private static final Diagnostic[] EMPTY = new Diagnostic[0];

    private final Diagnostic[] diagnostics;

    /**
     * Creates a new instance for serializer.
     */
    protected DiagnosticException() {
        this.diagnostics = EMPTY;
    }

    /**
     * Creates a new instance.
     * @param severity the severity
     * @param message the message
     */
    public DiagnosticException(Diagnostic.Level severity, String message) {
        this(new BasicDiagnostic(severity, message, null));
    }

    /**
     * Creates a new instance.
     * @param severity the severity
     * @param message the message
     * @param cause the original cause
     */
    public DiagnosticException(Diagnostic.Level severity, String message, Exception cause) {
        this(new BasicDiagnostic(severity, message, cause));
    }

    /**
     * Creates a new instance.
     * @param diagnostics the diagnostics
     */
    public DiagnosticException(Diagnostic... diagnostics) {
        this(Arrays.asList(diagnostics));
    }

    /**
     * Creates a new instance.
     * @param diagnostics diagnostics
     */
    public DiagnosticException(Collection<? extends Diagnostic> diagnostics) {
        super(extractMessage(diagnostics), extractCause(diagnostics));
        this.diagnostics = diagnostics.toArray(new Diagnostic[diagnostics.size()]);
    }

    /**
     * Returns the diagnostics of this exception.
     * @return the diagnostics
     */
    public List<Diagnostic> getDiagnostics() {
        return Collections.unmodifiableList(Arrays.asList(diagnostics));
    }

    private static String extractMessage(Collection<? extends Diagnostic> diagnostics) {
        Diagnostic diagnostic = first(diagnostics);
        if (diagnostic == null) {
            return null;
        }
        if (diagnostics.size() == 1) {
            return diagnostic.getMessage();
        } else {
            return MessageFormat.format(
                    "{0}, ...and {1} more", //$NON-NLS-1$
                    diagnostic.getMessage(),
                    diagnostics.size() - 1);
        }
    }

    private static Throwable extractCause(Collection<? extends Diagnostic> diagnostics) {
        Diagnostic diagnostic = first(diagnostics);
        if (diagnostic == null) {
            return null;
        }
        return diagnostic.getException();
    }

    private static Diagnostic first(Collection<? extends Diagnostic> diagnostics) {
        for (Diagnostic.Level level : SEARCH_ORDER) {
            for (Diagnostic diagnostic : diagnostics) {
                if (diagnostic.getLevel() == level) {
                    return diagnostic;
                }
            }
        }
        return null;
    }

    @Override
    public <T> Optional<T> findAdapter(Class<T> type) {
        return Adaptables.find(type, this, Arrays.stream(diagnostics));
    }
}
