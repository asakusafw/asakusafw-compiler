package com.asakusafw.lang.compiler.cli;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Test;

/**
 * Test for {@link ClassNamePredicate}.
 */
public class ClassNamePredicateTest {

    /**
     * just a literal.
     */
    @Test
    public void literal() {
        ClassNamePredicate predicate = ClassNamePredicate.parse("java.lang.String");
        assertThat(predicate.apply(String.class), is(true));
        assertThat(predicate.apply(Object.class), is(false));
        assertThat(predicate.apply(List.class), is(false));
    }

    /**
     * trailing wildcard.
     */
    @Test
    public void trailing() {
        ClassNamePredicate predicate = ClassNamePredicate.parse("java.lang.*");
        assertThat(predicate.apply(String.class), is(true));
        assertThat(predicate.apply(Object.class), is(true));
        assertThat(predicate.apply(List.class), is(false));
    }

    /**
     * leading wildcard.
     */
    @Test
    public void first() {
        ClassNamePredicate predicate = ClassNamePredicate.parse("*Buffer");
        assertThat(predicate.apply(StringBuffer.class), is(true));
        assertThat(predicate.apply(ByteBuffer.class), is(true));
        assertThat(predicate.apply(StringBuilder.class), is(false));
    }

    /**
     * wildcard in middle.
     */
    @Test
    public void middle() {
        ClassNamePredicate predicate = ClassNamePredicate.parse("java.*Buffer");
        assertThat(predicate.apply(StringBuffer.class), is(true));
        assertThat(predicate.apply(ByteBuffer.class), is(true));
        assertThat(predicate.apply(StringBuilder.class), is(false));
    }

    /**
     * multiple wildcards.
     */
    @Test
    public void multiple() {
        ClassNamePredicate predicate = ClassNamePredicate.parse("*.util.*");
        assertThat(predicate.apply(List.class), is(true));
        assertThat(predicate.apply(java.util.Date.class), is(true));
        assertThat(predicate.apply(java.sql.Date.class), is(false));
    }
}
