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
package com.asakusafw.bridge.api;

import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.runtime.core.Report.Level;

/**
 * Provides reporting feature for Asakusa applications.
 * <p>
 * Clients can use this class <em>only in operator methods</em>.
 * </p>
 */
public final class Report {

    static final Logger LOG = LoggerFactory.getLogger(Report.class);

    private Report() {
        return;
    }

    /**
     * Log a message at {@code INFO} level.
     * @param message the message
     */
    public static void info(String message) {
        report(Level.INFO, message);
    }

    /**
     * Log a message with exception object at {@code INFO} level.
     * @param message the message
     * @param throwable attached exception object (nullable)
     */
    public static void info(String message, Throwable throwable) {
        report(Level.INFO, message, throwable);
    }

    /**
     * Log a message at {@code WARN} level.
     * @param message the message
     */
    public static void warn(String message) {
        report(Level.WARN, message);
    }

    /**
     * Log a message with exception object at {@code WARN} level.
     * @param message the message
     * @param throwable attached exception object (nullable)
     */
    public static void warn(String message, Throwable throwable) {
        report(Level.WARN, message, throwable);
    }

    /**
     * Log a message at {@code ERROR} level.
     * @param message the message
     */
    public static void error(String message) {
        report(Level.ERROR, message);
    }

    /**
     * Log a message with exception object at {@code ERROR} level.
     * @param message the message
     * @param throwable attached exception object (nullable)
     */
    public static void error(String message, Throwable throwable) {
        report(Level.ERROR, message, throwable);
    }

    private static void report(Level level, String message) {
        if (level == Level.ERROR) {
            if (LOG.isErrorEnabled()) {
                LOG.error(message, new Exception("error"));
            }
        } else if (level == Level.WARN) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(message, new Exception("warn"));
            }
        } else if (level == Level.INFO) {
            LOG.info(message);
        } else {
            LOG.error(MessageFormat.format("Unknown level \"{0}\": {1}", level, message));
        }
    }

    private static void report(Level level, String message, Throwable throwable) {
        if (level == Level.ERROR) {
            LOG.error(message, throwable);
        } else if (level == Level.WARN) {
            LOG.warn(message, throwable);
        } else if (level == Level.INFO) {
            LOG.info(message, throwable);
        } else {
            LOG.error(MessageFormat.format("Unknown level \"{0}\": {1}", level, message), throwable);
        }
    }
}
