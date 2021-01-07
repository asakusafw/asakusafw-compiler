/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.function.Predicate;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

/**
 * Test for {@link NamePattern}.
 */
public class NamePatternTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        NamePattern p = p("simple");
        assertThat(p.getAlternatives(), containsInAnyOrder("simple"));
        assertThat(p, accepts("simple"));
        assertThat(p, not(accepts("other")));
    }

    /**
     * w/ multiple alternatives.
     */
    @Test
    public void multiple() {
        NamePattern p = p("a", "b", "c");
        assertThat(p.getAlternatives(), containsInAnyOrder("a", "b", "c"));
        assertThat(p, accepts("a", "b", "c"));
        assertThat(p, not(accepts("d")));
    }

    /**
     * w/ wildcard.
     */
    @Test
    public void wildcard() {
        NamePattern p = p("*");
        assertThat(p.getAlternatives(), containsInAnyOrder("*"));
        assertThat(p, accepts("", "a", "anything"));
    }

    /**
     * w/ partial wildcard.
     */
    @Test
    public void wildcard_partial() {
        NamePattern p = p("a-*-b");
        assertThat(p.getAlternatives(), containsInAnyOrder("a-*-b"));
        assertThat(p, accepts("a--b", "a-c-b", "a-anything-b"));
        assertThat(p, not(accepts("a", "b-b-b", "a-a-a")));
    }

    /**
     * w/ dot (not a wildcard).
     */
    @Test
    public void dot() {
        NamePattern p = p(".");
        assertThat(p.getAlternatives(), containsInAnyOrder("."));
        assertThat(p, accepts("."));
        assertThat(p, not(accepts("a")));
    }

    /**
     * w/ meta-characters.
     */
    @Test
    public void meta() {
        NamePattern p = p("(something)");
        assertThat(p.getAlternatives(), containsInAnyOrder("(something)"));
        assertThat(p, accepts("(something)"));
        assertThat(p, not(accepts("something")));
    }

    private static NamePattern p(String... alternatives) {
        return new NamePattern(alternatives);
    }

    private static Matcher<Predicate<CharSequence>> accepts(String... samples) {
        return new BaseMatcher<Predicate<CharSequence>>() {
            @Override
            public boolean matches(Object item) {
                @SuppressWarnings("unchecked")
                Predicate<? super CharSequence> predicate = (Predicate<? super CharSequence>) item;
                return Arrays.stream(samples)
                        .anyMatch(predicate);
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("accepts any of ").appendValueList("{", ", ", "}", samples);
            }
        };
    }
}
