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
package com.asakusafw.dag.runtime.jdbc.basic;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import com.asakusafw.dag.runtime.jdbc.ConnectionPool;
import com.asakusafw.dag.runtime.jdbc.JdbcDagTestRoot;

/**
 * Test for {@link SimpleConnectionPool}.
 */
public class SimpleConnectionPoolTest extends JdbcDagTestRoot {

    /**
     * timeout.
     */
    @Rule
    public final TestRule timeout = new Timeout(5000);

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (SimpleConnectionPool pool = newInstance(1)) {
            try (ConnectionPool.Handle handle = pool.acquire()) {
                insert(handle.getConnection(), 1, "1.0", "Hello1");
            }
        }
        assertThat(h2.count(TABLE), is(1));
    }

    /**
     * reuse connections.
     * @throws Exception if failed
     */
    @Test
    public void reuse() throws Exception {
        try (SimpleConnectionPool pool = newInstance(1)) {
            try (ConnectionPool.Handle handle = pool.acquire()) {
                insert(handle.getConnection(), 1, "1.0", "Hello1");
            }
            try (ConnectionPool.Handle handle = pool.acquire()) {
                insert(handle.getConnection(), 2, "2.0", "Hello2");
            }
            try (ConnectionPool.Handle handle = pool.acquire()) {
                insert(handle.getConnection(), 3, "3.0", "Hello3");
            }
        }
        assertThat(h2.count(TABLE), is(3));
    }

    /**
     * connection closed manually.
     * @throws Exception if failed
     */
    @Test
    public void manual_close() throws Exception {
        try (SimpleConnectionPool pool = newInstance(1)) {
            try (ConnectionPool.Handle handle = pool.acquire()) {
                insert(handle.getConnection(), 1, "1.0", "Hello1");
                handle.getConnection().close();
            }
            try (ConnectionPool.Handle handle = pool.acquire()) {
                insert(handle.getConnection(), 2, "2.0", "Hello2");
            }
        }
        assertThat(h2.count(TABLE), is(2));
    }

    /**
     * cascade.
     * @throws Exception if failed
     */
    @Test
    public void cascade() throws Exception {
        try (SimpleConnectionPool pool = newInstance(2)) {
            try (ConnectionPool.Handle handle = pool.acquire()) {
                insert(handle.getConnection(), 1, "1.0", "Hello1");
                try (ConnectionPool.Handle inner = pool.acquire()) {
                    insert(inner.getConnection(), 2, "2.0", "Hello2");
                }
                try (ConnectionPool.Handle inner = pool.acquire()) {
                    insert(inner.getConnection(), 3, "3.0", "Hello3");
                }
            }
        }
        assertThat(h2.count(TABLE), is(3));
    }

    private SimpleConnectionPool newInstance(int connections) {
        return bless(new SimpleConnectionPool(h2.getJdbcUrl(), Collections.emptyMap(), connections));
    }
}
