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
package com.asakusafw.vanilla.core.util;

import java.io.File;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.MessageFormat;
import java.util.Optional;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * Utilities about system properties.
 * @since 0.4.0
 */
public final class SystemProperty {

    /**
     * The system key prefix.
     */
    public static final String KEY_PREFIX = "com.asakusafw.vanilla."; //$NON-NLS-1$

    private SystemProperty() {
        return;
    }

    /**
     * Returns the optional system property value of the given name.
     * @param name the property name
     * @return the related system property value, or empty if it is not defined
     */
    public static Optional<String> find(String name) {
        Arguments.requireNonNull(name);
        return AccessController.doPrivileged((PrivilegedAction<Optional<String>>) () -> {
            return Optionals.of(System.getProperty(name))
                    .map(String::trim);
        });
    }

    /**
     * Returns the mandatory system property value of the given name.
     * @param name the property name
     * @return the related system property value, or empty if it is not defined
     */
    public static String get(String name) {
        return find(name)
                .filter(s -> s.isEmpty() == false)
                .orElseThrow(() -> new IllegalStateException(MessageFormat.format(
                        "mandatory system property \"{0}\" is not defined",
                        name)));
    }

    /**
     * Returns the optional system property value of the given name.
     * @param name the property name
     * @param defaultValue the default value
     * @return the related system property value, or empty if it is not defined
     */
    public static boolean get(String name, boolean defaultValue) {
        return find(name)
                .map(s -> s.isEmpty() || Boolean.parseBoolean(s))
                .orElse(defaultValue);
    }

    /**
     * Returns the optional system property value of the given name.
     * @param name the property name
     * @param defaultValue the default value
     * @return the related system property value, or empty if it is not defined
     */
    public static int get(String name, int defaultValue) {
        return find(name)
                .filter(s -> s.isEmpty() == false)
                .map(Integer::parseInt)
                .orElse(defaultValue);
    }

    /**
     * Returns the optional system property value of the given name.
     * @param name the property name
     * @param defaultValue the default value
     * @return the related system property value, or empty if it is not defined
     */
    public static double get(String name, double defaultValue) {
        return find(name)
                .filter(s -> s.isEmpty() == false)
                .map(Double::parseDouble)
                .orElse(defaultValue);
    }

    /**
     * Returns the system temporary directory.
     * @return the system temporary directory
     */
    public static File getTemporaryDirectory() {
        return new File(SystemProperty.get("java.io.tmpdir")); //$NON-NLS-1$
    }
}
