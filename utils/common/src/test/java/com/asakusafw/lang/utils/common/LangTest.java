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
package com.asakusafw.lang.utils.common;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.junit.Test;

/**
 * Test for {@link Lang}.
 */
public class LangTest {

    /**
     * safe w/o exception.
     */
    @Test
    public void safe_ok() {
        assertThat(Lang.safe(() -> 0), is(0));
    }

    /**
     * safe w/ exception.
     */
    @Test
    public void safe_raise() {
        boolean thrown = false;
        try {
            Lang.safe((Callable<?>) () -> { throw new IOException(); });
        } catch (AssertionError e) {
            thrown = true;
        }
        assertThat(thrown, is(true));
    }

    /**
     * safe w/o exception.
     */
    @Test
    public void safe_runnable_ok() {
        Lang.safe(() -> { return; });
    }

    /**
     * safe w/ exception.
     */
    @Test
    public void safe_runnable_raise() {
        boolean thrown = false;
        try {
            Lang.safe((RunnableWithException<?>) () -> { throw new IOException(); });
        } catch (AssertionError e) {
            thrown = true;
        }
        assertThat(thrown, is(true));
    }

    /**
     * let w/ action.
     */
    @Test
    public void let_action() {
        assertThat(Lang.let(new StringBuffer(), b -> { b.append("A"); }).toString(), is("A"));
    }
}
