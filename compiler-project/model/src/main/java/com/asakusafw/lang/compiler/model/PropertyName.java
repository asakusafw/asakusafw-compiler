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
package com.asakusafw.lang.compiler.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.asakusafw.lang.compiler.common.util.EnumUtil;

/**
 * Represents a property name.
 */
public class PropertyName {

    /**
     * Represents an empty name.
     */
    public static final String EMPTY_NAME = "_"; //$NON-NLS-1$

    static final Option[] EMPTY_OPTIONS = new Option[0];

    static final Pattern TRIM_PATTERN = Pattern.compile("[ _]*(.*?)[ _]*"); //$NON-NLS-1$

    private final List<String> words;

    /**
     * Creates a new instance.
     * @param words words in the name
     */
    public PropertyName(List<String> words) {
        this.words = normalize(words);
    }

    /**
     * Creates a new instance.
     * @param nameString the property name string
     * @return the created instance
     */
    public static PropertyName of(String nameString) {
        return of(nameString, EMPTY_OPTIONS);
    }

    /**
     * Creates a new instance.
     * @param nameString the property name string
     * @param options options for building property name
     * @return the created instance
     */
    public static PropertyName of(String nameString, Option... options) {
        if (nameString.isEmpty()) {
            throw new IllegalArgumentException("nameString must not be empty"); //$NON-NLS-1$
        }
        String s = trimNameString(nameString);
        if (s.indexOf('_') >= 0 || s.toUpperCase().equals(s)) {
            String[] segments = s.split("_"); //$NON-NLS-1$
            return build(Arrays.asList(segments), options);
        } else {
            List<String> segments = new ArrayList<>();
            int start = 0;
            for (int i = 1, n = s.length(); i < n; i++) {
                if (Character.isUpperCase(s.charAt(i))) {
                    segments.add(s.substring(start, i));
                    start = i;
                }
            }
            segments.add(s.substring(start));
            return build(segments, options);
        }
    }

    private static PropertyName build(List<String> segments, Option[] options) {
        Set<Option> opts = EnumUtil.freeze(options);
        return new PropertyName(apply(opts, segments));
    }

    private static List<String> apply(Set<Option> opts, List<String> segments) {
        if (segments.isEmpty()) {
            return segments;
        }
        List<String> results = segments;
        if (opts.contains(Option.KEEP_CASELESS_WORDS) == false) {
            results = combineCaselessSegments(segments);
        }
        return results;
    }

    private static List<String> combineCaselessSegments(List<String> segments) {
        List<String> applied = new ArrayList<>();
        applied.add(segments.get(0));
        for (int i = 1, n = segments.size(); i < n; i++) {
            String s = segments.get(i);
            if (s.isEmpty()) {
                continue;
            }
            if (hasCase(s) == false) {
                int last = applied.size() - 1;
                assert last >= 0;
                applied.set(last, applied.get(last) + s);
            } else {
                applied.add(s);
            }
        }
        return applied;
    }

    private static boolean hasCase(String s) {
        assert s.isEmpty() == false;
        char first = s.charAt(0);
        int type = Character.getType(first);
        return type == Character.UPPERCASE_LETTER || type == Character.LOWERCASE_LETTER;
    }

    private static String trimNameString(String nameString) {
        Matcher matcher = TRIM_PATTERN.matcher(nameString);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return nameString;
    }

    private static List<String> normalize(List<String> segments) {
        List<String> results = new ArrayList<>();
        for (String segment : segments) {
            if (segment.isEmpty() == false) {
                results.add(segment.toLowerCase(Locale.ENGLISH));
            }
        }
        return Collections.unmodifiableList(results);
    }

    /**
     * Returns whether this name is empty or not.
     * @return {@code true} if this name is empty, otherwise {@code false}
     */
    public boolean isEmpty() {
        return words.isEmpty();
    }

    /**
     * Returns in this name.
     * @return the words (lower case)
     */
    public List<String> getWords() {
        return words;
    }

    /**
     * Returns a property name which the specified is inserted into head of this name.
     * The method does not modifies this object.
     * @param word the first word
     * @return the modified name
     */
    public PropertyName addFirst(String word) {
        List<String> results = new ArrayList<>();
        results.add(word);
        results.addAll(words);
        return new PropertyName(results);
    }

    /**
     * Returns a property name which the specified is inserted into tail of this name.
     * The method does not modifies this object.
     * @param word the last word
     * @return the modified name
     */
    public PropertyName addLast(String word) {
        List<String> results = new ArrayList<>();
        results.addAll(words);
        results.add(word);
        return new PropertyName(results);
    }

    /**
     * Returns a property name which the first word is removed from this.
     * The method does not modifies this object.
     * @return the modified name
     */
    public PropertyName removeFirst() {
        if (words.isEmpty()) {
            throw new IllegalStateException();
        }
        return new PropertyName(words.subList(1, words.size()));
    }

    /**
     * Returns a property name which the first word is removed from this.
     * The method does not modifies this object.
     * @return the modified name
     */
    public PropertyName removeLast() {
        if (words.isEmpty()) {
            throw new IllegalStateException();
        }
        return new PropertyName(words.subList(0, words.size() - 1));
    }

    /**
     * Returns the name string as {@code snake_case}.
     * This may returns {@link #EMPTY_NAME} if this name {@link #isEmpty() is empty}.
     * @return the property name
     */
    public String toName() {
        if (words.isEmpty()) {
            return EMPTY_NAME;
        }
        StringBuilder buf = new StringBuilder();
        buf.append(words.get(0));
        for (int i = 1, n = words.size(); i < n; i++) {
            buf.append('_');
            buf.append(words.get(i));
        }
        return buf.toString();
    }

    /**
     * Returns the name string as {@code camelCase}.
     * This may returns {@link #EMPTY_NAME} if this name {@link #isEmpty() is empty}.
     * @return the property name
     */
    public String toMemberName() {
        if (words.isEmpty()) {
            return EMPTY_NAME;
        }
        StringBuilder buf = new StringBuilder();
        buf.append(words.get(0));
        for (int i = 1, n = words.size(); i < n; i++) {
            buf.append(capitalize(words.get(i)));
        }
        return buf.toString();
    }

    private String capitalize(String segment) {
        assert segment != null;
        StringBuilder buf = new StringBuilder(segment.toLowerCase(Locale.ENGLISH));
        buf.setCharAt(0, Character.toUpperCase(buf.charAt(0)));
        return buf.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + words.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PropertyName other = (PropertyName) obj;
        if (!words.equals(other.words)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return toName();
    }

    /**
     * Represents options of {@link PropertyName}.
     */
    public enum Option {

        /**
         * do not combine segments which consist of case-less word.
         */
        KEEP_CASELESS_WORDS,
    }
}
