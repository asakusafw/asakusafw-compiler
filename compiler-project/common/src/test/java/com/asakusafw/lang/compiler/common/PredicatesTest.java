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
package com.asakusafw.lang.compiler.common;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

/**
 * Test for {@link Predicates}.
 */
public class PredicatesTest {

    private static final Predicate<Object> T = Predicates.anything();

    private static final Predicate<Object> F = Predicates.nothing();

    /**
     * anything.
     */
    @Test
    public void anything() {
        assertThat(T.apply("a"), is(true));
    }

    /**
     * anything.
     */
    @Test
    public void nothing() {
        assertThat(F.apply("a"), is(false));
    }

    /**
     * not.
     */
    @Test
    public void not() {
        assertThat(Predicates.not(T), eq(F));
        assertThat(Predicates.not(F), eq(T));
    }

    /**
     * and.
     */
    @Test
    public void and() {
        assertThat(Predicates.and(T, T), eq(T));
        assertThat(Predicates.and(F, T), eq(F));
        assertThat(Predicates.and(T, F), eq(F));
        assertThat(Predicates.and(F, F), eq(F));
    }

    /**
     * or.
     */
    @Test
    public void or() {
        assertThat(Predicates.or(T, T), eq(T));
        assertThat(Predicates.or(F, T), eq(T));
        assertThat(Predicates.or(T, F), eq(T));
        assertThat(Predicates.or(F, F), eq(F));
    }

    private Matcher<Predicate<Object>> eq(Predicate<Object> p) {
        return new FeatureMatcher<Predicate<Object>, Boolean>(is(p.apply(null)), "eq", "eq") {
            @Override
            protected Boolean featureValueOf(Predicate<Object> actual) {
                return actual.apply(null);
            }
        };
    }
}
