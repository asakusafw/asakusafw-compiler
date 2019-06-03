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
package com.asakusafw.bridge.api;

import java.io.IOException;

import com.asakusafw.bridge.broker.ResourceBroker;
import com.asakusafw.runtime.core.Report.FailedException;
import com.asakusafw.runtime.core.Report.Level;
import com.asakusafw.runtime.core.ResourceConfiguration;

/**
 * Provides reporting feature for Asakusa applications.
 * <p>
 * Clients can use this class <em>only in operator methods</em>.
 * </p>
 *
 * <h3> requirements </h3>
 * <p>
 * This API requires that {@link ResourceConfiguration Asakusa configuration} object has been registered to
 * {@link ResourceBroker}.
 * </p>
 * @since 0.1.0
 * @version 0.1.1
 * @deprecated this API is not for application developers,
 *      please use {@link com.asakusafw.runtime.core.Report} instead
 */
@Deprecated
public final class Report {

    private Report() {
        return;
    }

    /**
     * Log a message at {@code INFO} level.
     * @param message the message
     */
    public static void info(String message) {
        try {
            ReportAdapter.delegate().report(Level.INFO, message);
        } catch (IOException e) {
            throw new FailedException(e);
        }
    }

    /**
     * Log a message with exception object at {@code INFO} level.
     * @param message the message
     * @param throwable attached exception object (nullable)
     */
    public static void info(String message, Throwable throwable) {
        try {
            ReportAdapter.delegate().report(Level.INFO, message, throwable);
        } catch (IOException e) {
            throw new FailedException(e);
        }
    }

    /**
     * Log a message at {@code WARN} level.
     * @param message the message
     */
    public static void warn(String message) {
        try {
            ReportAdapter.delegate().report(Level.WARN, message);
        } catch (IOException e) {
            throw new FailedException(e);
        }
    }

    /**
     * Log a message with exception object at {@code WARN} level.
     * @param message the message
     * @param throwable attached exception object (nullable)
     */
    public static void warn(String message, Throwable throwable) {
        try {
            ReportAdapter.delegate().report(Level.WARN, message, throwable);
        } catch (IOException e) {
            throw new FailedException(e);
        }
    }

    /**
     * Log a message at {@code ERROR} level.
     * @param message the message
     */
    public static void error(String message) {
        try {
            ReportAdapter.delegate().report(Level.ERROR, message);
        } catch (IOException e) {
            throw new FailedException(e);
        }
    }

    /**
     * Log a message with exception object at {@code ERROR} level.
     * @param message the message
     * @param throwable attached exception object (nullable)
     */
    public static void error(String message, Throwable throwable) {
        try {
            ReportAdapter.delegate().report(Level.ERROR, message, throwable);
        } catch (IOException e) {
            throw new FailedException(e);
        }
    }
}
