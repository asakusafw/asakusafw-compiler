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
package com.asakusafw.lang.compiler.common;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Represents name pattern.
 * @since 0.4.0
 */
public class NamePattern implements Predicate<CharSequence> {

    /**
     * The wildcard character.
     */
    public static final char WILDCARD = '*';

    /**
     * The empty pattern which rejects any names.
     */
    public static final NamePattern EMPTY = new NamePattern(Collections.emptySet());

    private final Set<String> alternatives;

    private final Predicate<CharSequence> delegate;

    /**
     * Creates a new instance.
     * @param alternatives the pattern elements
     */
    public NamePattern(String... alternatives) {
        this(Arrays.asList(Objects.requireNonNull(alternatives)));
    }

    /**
     * Creates a new instance.
     * @param alternatives the pattern elements
     */
    public NamePattern(Collection<String> alternatives) {
        Objects.requireNonNull(alternatives);
        this.alternatives = alternatives.stream()
                .map(String::trim)
                .filter(s -> s.isEmpty() == false)
                .collect(Collectors.collectingAndThen(
                        Collectors.toCollection(TreeSet::new),
                        Collections::unmodifiableSortedSet));
        this.delegate = build(this.alternatives);
    }

    private static Predicate<CharSequence> build(Collection<String> elements) {
        List<String> compiled = elements.stream()
                .map(NamePattern::compile)
                .collect(Collectors.toList());
        if (compiled.isEmpty()) {
            return s -> false;
        }
        Pattern pattern = Pattern.compile(String.join("|", compiled));
        return s -> pattern.matcher(s).matches();
    }

    private static String compile(String pattern) {
        StringBuilder buf = new StringBuilder();
        int start = 0;
        while (true) {
            int found = pattern.indexOf(WILDCARD, start);
            if (found < 0) {
                break;
            }
            buf.append(quote(pattern, start, found));
            buf.append(".*"); //$NON-NLS-1$
            start = found + 1;
        }
        buf.append(quote(pattern, start, pattern.length()));
        return buf.toString();
    }

    private static String quote(String pattern, int start, int end) {
        if (start == end) {
            return ""; //$NON-NLS-1$
        }
        String sub = pattern.substring(start, end);
        for (char c : sub.toCharArray()) {
            if (isMeta(c)) {
                return Pattern.quote(sub);
            }
        }
        return sub;
    }

    private static boolean isMeta(char c) {
        boolean known = ('0' <= c && c <= '9')
                || ('A' <= c && c <= 'Z')
                || ('a' <= c && c <= 'z')
                || c == '-'
                || c == '_'
                || c == ' ';
        return known == false;
    }

    @Override
    public boolean test(CharSequence t) {
        return delegate.test(t);
    }

    /**
     * Returns the pattern elements.
     * @return the pattern elements
     */
    public Set<String> getAlternatives() {
        return alternatives;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "NamePattern({0})", //$NON-NLS-1$
                getAlternatives().stream()
                    .map(s -> String.format("'%s'", s)) //$NON-NLS-1$
                    .collect(Collectors.joining(", "))); //$NON-NLS-1$
    }
}
