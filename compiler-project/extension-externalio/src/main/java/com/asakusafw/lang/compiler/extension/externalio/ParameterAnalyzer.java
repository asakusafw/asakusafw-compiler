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
package com.asakusafw.lang.compiler.extension.externalio;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities about parameters.
 * @since 0.3.0
 */
public final class ParameterAnalyzer {

    private static final Pattern PATTERN_VARIABLE = Pattern.compile("\\$\\{(.*?)\\}"); //$NON-NLS-1$

    private ParameterAnalyzer() {
        return;
    }

    /**
     * Collects variable names in the target string.
     * @param string the target string
     * @return the found variable names
     */
    public static Set<String> collectVariableNames(String string) {
        Set<String> results = new HashSet<>();
        Matcher matcher = PATTERN_VARIABLE.matcher(string);
        int start = 0;
        while (matcher.find(start)) {
            results.add(matcher.group(1));
            start = matcher.end();
        }
        return results;
    }
}
