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
package com.asakusafw.bridge.broker;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.Closeable;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

/**
 * Test for {@link ResourceSessionEntity}.
 */
public class ResourceSessionEntityTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (ResourceSessionEntity entity = new ResourceSessionEntity()) {
            assertThat(entity.find(String.class), is(nullValue()));
            entity.put(String.class, "Hello, world!");
            assertThat(entity.find(String.class), is("Hello, world!"));
        }
    }

    /**
     * put and then get.
     * @throws Exception if failed
     */
    @Test
    public void put_get() throws Exception {
        try (ResourceSessionEntity entity = new ResourceSessionEntity()) {
            entity.put(String.class, "Hello, world!");
            assertThat(entity.get(String.class), is("Hello, world!"));
        }
    }

    /**
     * get w/ supplier.
     * @throws Exception if failed
     */
    @Test
    public void get_supplier() throws Exception {
        AtomicBoolean once = new AtomicBoolean();
        Callable<String> supplier = () -> {
            assertThat(once.compareAndSet(false, true), is(true));
            return "Hello, world!";
        };
        try (ResourceSessionEntity entity = new ResourceSessionEntity()) {
            assertThat(entity.get(String.class, supplier), is("Hello, world!"));
            assertThat(entity.get(String.class, supplier), is("Hello, world!"));
            assertThat(entity.get(String.class, supplier), is("Hello, world!"));
        }
    }

    /**
     * close added resources.
     * @throws Exception if failed
     */
    @Test
    public void close_resources() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        try (ResourceSessionEntity entity = new ResourceSessionEntity()) {
            entity.put(Object.class, (Closeable) () -> assertThat(closed.compareAndSet(false, true), is(true)));
            assertThat(closed.get(), is(false));
        }
        assertThat(closed.get(), is(true));
    }

    /**
     * put and then get via reference.
     * @throws Exception if failed
     */
    @Test
    public void put_get_reference() throws Exception {
        try (ResourceSessionEntity entity = new ResourceSessionEntity();
                ResourceSession ref = entity.newReference()) {
            ref.put(String.class, "Hello, world!");
            assertThat(ref.get(String.class), is("Hello, world!"));
            assertThat(ref.find(String.class), is("Hello, world!"));
            assertThat(entity.get(String.class), is("Hello, world!"));
        }
    }

    /**
     * reference counting.
     * @throws Exception if failed
     */
    @Test
    public void close_reference() throws Exception {
        try (ResourceSessionEntity entity = new ResourceSessionEntity()) {
            assertThat(entity.closed, is(false));
            try (ResourceSession ref = entity.newReference()) {
                try (ResourceSession nested = entity.newReference()) {
                    assertThat(entity.closed, is(false));
                }
                assertThat(entity.closed, is(false));
            }
            assertThat(entity.closed, is(true));
        }
    }

    /**
     * put conflict.
     * @throws Exception if failed
     */
    @Test(expected = IllegalStateException.class)
    public void put_conflict() throws Exception {
        try (ResourceSessionEntity entity = new ResourceSessionEntity()) {
            entity.put(String.class, "Hello, world!");
            entity.put(String.class, "Hello, world!");
        }
    }

    /**
     * get absent.
     * @throws Exception if failed
     */
    @Test(expected = NoSuchElementException.class)
    public void get_absent() throws Exception {
        try (ResourceSessionEntity entity = new ResourceSessionEntity()) {
            entity.get(String.class);
        }
    }
}
