/**
 * Copyright 2011-2019 Asakusa Framework Team.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;

/**
 * Test for {@link Suppliers}.
 */
public class SuppliersTest {

    /**
     * supplier w/ empty args.
     */
    @Test
    public void supplier_empty() {
        assertThat(take(Suppliers.supplier()), is(empty()));
    }

    /**
     * supplier w/ 1 arg.
     */
    @Test
    public void supplier_1() {
        assertThat(take(Suppliers.supplier("A")), is(Arrays.asList("A")));
    }

    /**
     * supplier w/ many args.
     */
    @Test
    public void supplier_many() {
        assertThat(take(Suppliers.supplier("A", "B", "C")), is(Arrays.asList("A", "B", "C")));
    }

    /**
     * supplier from iterable.
     */
    @Test
    public void supplier_iterable() {
        assertThat(take(Suppliers.fromIterable(Arrays.asList("A", "B", "C"))), is(Arrays.asList("A", "B", "C")));
    }

    /**
     * supplier from callable.
     */
    @Test
    public void supplier_callable() {
        Supplier<String> s = Suppliers.of("A", "B", "C");
        assertThat(take(Suppliers.fromCallable(() -> s.get())), is(Arrays.asList("A", "B", "C")));
    }

    private <T> List<T> take(Supplier<T> supplier) {
        return Lang.let(new ArrayList<>(), it -> {
            while (true) {
                T t = supplier.get();
                if (t == null) {
                    break;
                }
                it.add(t);
            }
        });
    }
}
