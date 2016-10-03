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
package com.asakusafw.lang.utils.common;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.junit.Test;

/**
 * Test for {@link Arguments}.
 */
public class ArgumentsTest {

    /**
     * {@link Arguments#require(boolean)}.
     */
    @Test
    public void require() {
        Arguments.require(true);
        catching(() -> Arguments.require(false));
    }

    /**
     * {@link Arguments#require(boolean, String)}.
     */
    @Test
    public void require_message() {
        Arguments.require(true, "OK");
        catching(() -> Arguments.require(false, "NG"));
    }

    /**
     * {@link Arguments#require(boolean, Supplier)}.
     */
    @Test
    public void require_supplier() {
        Arguments.require(true, () -> "OK");
        catching(() -> Arguments.require(false, () -> "NG"));
    }

    /**
     * {@link Arguments#requireNonNull(Object)}.
     */
    @Test
    public void requireNonNull() {
        Arguments.requireNonNull("OK");
        catching(() -> Arguments.requireNonNull(null));
    }

    /**
     * {@link Arguments#requireNonNull(Object, String)}.
     */
    @Test
    public void requireNonNull_message() {
        Arguments.requireNonNull("OK", "OK");
        catching(() -> Arguments.requireNonNull(null, "NG"));
    }

    /**
     * {@link Arguments#requireNonNull(Object, Supplier)}.
     */
    @Test
    public void requireNonNull_supplier() {
        Arguments.requireNonNull("OK", () -> "OK");
        catching(() -> Arguments.requireNonNull(null, () -> "NG"));
    }

    /**
     * {@link Arguments#safe(Callable)}.
     */
    @Test
    public void safe() {
        assertThat(Arguments.safe(() -> 0), is(0));
        catching(() -> Arguments.safe(() -> { throw new IOException(); }));
    }

    /**
     * {@link Arguments#safe(Callable, String)}.
     */
    @Test
    public void safe_message() {
        assertThat(Arguments.safe(() -> 0, "OK"), is(0));
        catching(() -> Arguments.safe(() -> { throw new IOException(); }, "NG"));
    }

    /**
     * {@link Arguments#safe(Callable, Supplier)}.
     */
    @Test
    public void safe_supplier() {
        assertThat(Arguments.safe(() -> 0, () -> "OK"), is(0));
        catching(() -> Arguments.safe(() -> { throw new IOException(); }, () -> "NG"));
    }

    private void catching(RunnableWithException<?> task) {
        assertThat(AssertUtil.catching(task), is(instanceOf(IllegalArgumentException.class)));
    }
}
