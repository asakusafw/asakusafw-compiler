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
package com.asakusafw.lang.compiler.common.util;

import java.util.Iterator;
import java.util.Objects;

/**
 * Utilities for String representations.
 */
public final class StringUtil {

    /**
     * The empty string.
     */
    public static final String EMPTY = ""; //$NON-NLS-1$

    private StringUtil() {
        return;
    }

    /**
     * Joins string values with a delimiter.
     * @param delimiter the delimiter
     * @param values the string values
     * @return the joined string value
     */
    public static String join(Object delimiter, Object... values) {
        if (values.length == 0) {
            return EMPTY;
        }
        String d = Objects.toString(delimiter, null);
        StringBuilder buf = new StringBuilder();
        buf.append(values[0]);
        for (int i = 1; i < values.length; i++) {
            if (d != null) {
                buf.append(d);
            }
            buf.append(values[i]);
        }
        return buf.toString();
    }

    /**
     * Joins string values with a delimiter.
     * @param delimiter the delimiter
     * @param values the string values
     * @return the joined string value
     */
    public static String join(Object delimiter, Iterable<?> values) {
        Iterator<?> iterator = values.iterator();
        if (iterator.hasNext() == false) {
            return EMPTY;
        }
        String d = Objects.toString(delimiter, null);
        StringBuilder buf = new StringBuilder();
        buf.append(iterator.next());
        while (iterator.hasNext()) {
            if (d != null) {
                buf.append(d);
            }
            buf.append(iterator.next());
        }
        return buf.toString();
    }
}
