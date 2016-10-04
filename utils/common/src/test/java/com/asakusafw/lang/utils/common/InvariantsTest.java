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

import java.util.function.Supplier;

import org.junit.Test;

/**
 * Test for {@link Invariants}.
 */
public class InvariantsTest {

    /**
     * {@link Invariants#require(boolean)}.
     */
    @Test
    public void require() {
        Invariants.require(true);
        catching(() -> Invariants.require(false));
    }

    /**
     * {@link Invariants#require(boolean, String)}.
     */
    @Test
    public void require_message() {
        Invariants.require(true, "OK");
        catching(() -> Invariants.require(false, "NG"));
    }

    /**
     * {@link Invariants#require(boolean, Supplier)}.
     */
    @Test
    public void require_supplier() {
        Invariants.require(true, () -> "OK");
        catching(() -> Invariants.require(false, () -> "NG"));
    }

    private void catching(RunnableWithException<?> task) {
        assertThat(AssertUtil.catching(task), is(instanceOf(IllegalStateException.class)));
    }
}
