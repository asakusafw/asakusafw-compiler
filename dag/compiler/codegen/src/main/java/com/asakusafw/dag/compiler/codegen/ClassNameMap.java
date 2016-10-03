/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.dag.compiler.codegen;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * A basic implementation of class name providers.
 * @since 0.4.0
 */
public class ClassNameMap {

    static final Pattern PATTERN_CATEGORY = Pattern.compile("[A-Za-z][A-Za-z0-9_]*(\\.[A-Za-z][A-Za-z0-9_]*)*"); //$NON-NLS-1$

    static final Pattern PATTERN_HINT = Pattern.compile("[A-Za-z][A-Za-z0-9]*"); //$NON-NLS-1$

    private final String prefix;

    private final Map<Tuple<String, String>, AtomicInteger> counters = new HashMap<>();

    /**
     * Creates a new instance.
     * @param prefix the prefix of fully qualified class names to generate
     */
    public ClassNameMap(String prefix) {
        Arguments.requireNonNull(prefix);
        this.prefix = prefix;
    }

    /**
     * Returns a unique class name.
     * @param category the category name
     * @param hint an optional class name hint
     * @return the class name
     */
    public ClassDescription get(String category, String hint) {
        String subpackage = Optionals.of(category)
                .filter(s -> PATTERN_CATEGORY.matcher(s).matches())
                .orElse("_"); //$NON-NLS-1$
        String simpleNamePrefix = Optionals.of(hint)
                .filter(s -> PATTERN_HINT.matcher(s).matches())
                .orElse(""); //$NON-NLS-1$
        int count = counters
                .computeIfAbsent(new Tuple<>(subpackage, simpleNamePrefix), k -> new AtomicInteger())
                .getAndIncrement();
        return new ClassDescription(toClassName(subpackage, simpleNamePrefix, count));
    }

    private String toClassName(String category, String simpleNamePrefix, int count) {
        Invariants.require(count >= 0);
        return String.format("%s%s.%s_%d", prefix, category, simpleNamePrefix, count); //$NON-NLS-1$
    }
}
