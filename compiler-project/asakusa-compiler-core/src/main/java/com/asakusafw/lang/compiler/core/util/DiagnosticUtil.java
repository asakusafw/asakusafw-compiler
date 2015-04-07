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
package com.asakusafw.lang.compiler.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Diagnostic;

/**
 * Utilities for {@link Diagnostic}.
 */
public final class DiagnosticUtil {

    static final Logger LOG = LoggerFactory.getLogger(DiagnosticUtil.class);

    private DiagnosticUtil() {
        return;
    }

    /**
     * Logs a target diagnostic object.
     * @param diagnostic the target diagnostic object
     */
    public static void log(Diagnostic diagnostic) {
        log(LOG, diagnostic);
    }

    /**
     * Logs a target diagnostic object.
     * @param logger the target logger
     * @param diagnostic the target diagnostic object
     */
    public static void log(Logger logger, Diagnostic diagnostic) {
        switch (diagnostic.getLevel()) {
        case ERROR:
            if (diagnostic.getException() == null) {
                logger.error(diagnostic.getMessage());
            } else {
                logger.error(diagnostic.getMessage(), diagnostic.getException());
            }
            break;
        case WARN:
            if (diagnostic.getException() == null) {
                logger.warn(diagnostic.getMessage());
            } else {
                logger.warn(diagnostic.getMessage(), diagnostic.getException());
            }
            break;
        case INFO:
            if (diagnostic.getException() == null) {
                logger.info(diagnostic.getMessage());
            } else {
                logger.info(diagnostic.getMessage(), diagnostic.getException());
            }
            break;
        default:
            throw new AssertionError(diagnostic);
        }
    }
}
