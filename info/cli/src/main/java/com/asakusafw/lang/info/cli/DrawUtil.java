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
package com.asakusafw.lang.info.cli;

final class DrawUtil {

    private DrawUtil() {
        return;
    }

    static String literal(String string) {
        StringBuilder buf = new StringBuilder();
        buf.append('"');
        for (char c : string.toCharArray()) {
            if (c == '\\' || c == '"') {
                buf.append('\\');
                buf.append(c);
            } else if (c == '\n') {
                buf.append('\\');
                buf.append('n');
            } else {
                buf.append(c);
            }
        }
        buf.append('"');
        return buf.toString();
    }

    static String escapeForRecord(CharSequence string) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0, n = string.length(); i < n; i++) {
            char c = string.charAt(i);
            if (c == '{' || c == '<') {
                buf.append('(');
            } else if (c == '}' || c == '>') {
                buf.append(')');
            } else if (c == '|') {
                buf.append('/');
            } else {
                buf.append(c);
            }
        }
        return buf.toString();
    }
}
