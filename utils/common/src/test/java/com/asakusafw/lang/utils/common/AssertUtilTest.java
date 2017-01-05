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
package com.asakusafw.lang.utils.common;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.junit.Test;

/**
 * Test for {@link AssertUtil}.
 */
public class AssertUtilTest {

    /**
     * catching w/ exception.
     */
    @Test
    public void catching_raise() {
        Exception e = AssertUtil.catching((Callable<?>) () -> { throw new IOException(); });
        assertThat(e, is(instanceOf(IOException.class)));
    }

    /**
     * catching w/ exception.
     */
    @Test
    public void catching_ok() {
        boolean thrown = false;
        try {
            AssertUtil.catching(() -> 0);
        } catch (AssertionError e) {
            thrown = true;
        }
        assertThat(thrown, is(true));
    }

    /**
     * catching w/ exception.
     */
    @Test
    public void catching_runnable_raise() {
        Exception e = AssertUtil.catching((RunnableWithException<?>) () -> { throw new IOException(); });
        assertThat(e, is(instanceOf(IOException.class)));
    }

    /**
     * catching w/ exception.
     */
    @Test
    public void catching_runnable_ok() {
        boolean thrown = false;
        try {
            AssertUtil.catching(() -> { return; });
        } catch (AssertionError e) {
            thrown = true;
        }
        assertThat(thrown, is(true));
    }
}
