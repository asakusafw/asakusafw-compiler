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
package com.asakusafw.dag.runtime.jdbc.operation;

import static com.asakusafw.dag.runtime.jdbc.operation.JdbcEnvironmentInstaller.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.Connection;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.ServiceLoader;

import org.junit.Test;

import com.asakusafw.dag.api.processor.basic.BasicProcessorContext;
import com.asakusafw.dag.api.processor.extension.ProcessorContextExtension;
import com.asakusafw.dag.runtime.jdbc.ConnectionPool;
import com.asakusafw.dag.runtime.jdbc.JdbcDagTestRoot;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.basic.BasicConnectionPool;
import com.asakusafw.lang.utils.common.Lang;

/**
 * Test for {@link JdbcEnvironmentInstaller}.
 */
public class JdbcEnvironmentInstallerTest extends JdbcDagTestRoot {

    /**
     * check registered.
     * @throws Exception if failed
     */
    @Test
    public void spi() throws Exception {
        assertThat(
                ServiceLoader.load(ProcessorContextExtension.class),
                hasItem(instanceOf(JdbcEnvironmentInstaller.class)));
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        JdbcEnvironment environment = build(new Object[] {
                q("a", KEY_URL), h2.getJdbcUrl(),
        });
        JdbcProfile profile = environment.getProfile("a");
        try (ConnectionPool.Handle handle = profile.acquire()) {
            assertThat(handle.getConnection().getMetaData().getURL(), is(h2.getJdbcUrl()));
        }
        assertThat(profile.getFetchSize().getAsInt(), is(DEFAULT_FETCH_SIZE));
        assertThat(profile.getBatchInsertSize().getAsInt(), is(DEFAULT_BATCH_INSERT_SIZE));
        assertThat(profile.getMaxInputConcurrency().getAsInt(), is(DEFAULT_INPUT_THREADS));
        assertThat(profile.getMaxOutputConcurrency().getAsInt(), is(DEFAULT_OUTPUT_THREADS));
        assertThat(profile.getOptimizations(), hasSize(0));
        assertThat(profile.getOption(OutputClearKind.class), is(Optional.empty()));
    }

    /**
     * w/ multiple profiles.
     * @throws Exception if failed
     */
    @Test
    public void profiles() throws Exception {
        JdbcEnvironment environment = build(new Object[] {
                q("a", KEY_URL), h2.getJdbcUrl(),
                q("b", KEY_URL), h2.getJdbcUrl(),
                q("c", KEY_URL), h2.getJdbcUrl(),
        });
        environment.getProfile("a");
        environment.getProfile("b");
        environment.getProfile("c");
    }

    /**
     * w/ options.
     * @throws Exception if failed
     */
    @Test(timeout = 5000)
    public void options() throws Exception {
        JdbcEnvironment environment = build(new Object[] {
                q("a", KEY_URL), h2.getJdbcUrl(),
                q("a", KEY_POOL_SIZE), 3,
                q("a", KEY_FETCH_SIZE), 1,
                q("a", KEY_BATCH_INSERT_SIZE), 2,
                q("a", KEY_INPUT_THREADS), 3,
                q("a", KEY_OUTPUT_THREADS), -1,
                q("a", KEY_PROPERTIES + ".testing"), "OK",
                q("a", KEY_POOL_CLASS), BasicConnectionPool.class.getName(),
                q("a", KEY_OUTPUT_CLEAR), "keep",
        });
        JdbcProfile profile = environment.getProfile("a");
        try (ConnectionPool.Handle ha = profile.acquire();
                ConnectionPool.Handle hb = profile.acquire();
                ConnectionPool.Handle hc = profile.acquire()) {
            Lang.pass();
        }
        assertThat(profile.getFetchSize().getAsInt(), is(1));
        assertThat(profile.getBatchInsertSize().getAsInt(), is(2));
        assertThat(profile.getMaxInputConcurrency().getAsInt(), is(3));
        assertThat(profile.getMaxOutputConcurrency(), is(OptionalInt.empty()));
        assertThat(profile.getOption(OutputClearKind.class), is(Optional.of(OutputClearKind.KEEP)));
    }

    /**
     * w/ custom connection pool.
     * @throws Exception if failed
     */
    @Test
    public void custom_connection_pool() throws Exception {
        JdbcEnvironment environment = build(new Object[] {
                q("a", KEY_URL), h2.getJdbcUrl(),
                q("a", KEY_POOL_CLASS), Custom.class.getName(),
        });
        JdbcProfile profile = environment.getProfile("a");
        try (ConnectionPool.Handle handle = profile.acquire()) {
            assertThat(handle, is(instanceOf(Custom.H.class)));
        }
    }

    /**
     * w/ custom connection pool, specifying provider class directly.
     * @throws Exception if failed
     */
    @Test
    public void custom_connection_pool_provider() throws Exception {
        JdbcEnvironment environment = build(new Object[] {
                q("a", KEY_URL), h2.getJdbcUrl(),
                q("a", KEY_POOL_CLASS), Custom.P.class.getName(),
        });
        JdbcProfile profile = environment.getProfile("a");
        try (ConnectionPool.Handle handle = profile.acquire()) {
            assertThat(handle, is(instanceOf(Custom.H.class)));
        }
    }

    @SuppressWarnings("javadoc")
    public static class Custom implements ConnectionPool {

        @Override
        public Handle acquire() throws IOException, InterruptedException {
            return new H();
        }

        @Override
        public String getUrl() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            return;
        }

        public static class P implements Provider {

            @Override
            public ConnectionPool newInstance(String url, Map<String, String> properties, int maxConnections) {
                return new Custom();
            }
        }

        public static class H implements ConnectionPool.Handle {

            @Override
            public Connection getConnection() throws IOException, InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws IOException, InterruptedException {
                return;
            }
        }
    }

    private static String q(String profile, String key) {
        return JdbcEnvironmentInstaller.qualified(profile, key);
    }

    private JdbcEnvironment build(Object... pairs) {
        assert pairs.length % 2 == 0;
        BasicProcessorContext context = new BasicProcessorContext(getClass().getClassLoader());
        for (int i = 0; i < pairs.length; i += 2) {
            context.withProperty(String.valueOf(pairs[i + 0]), String.valueOf(pairs[i + 1]));
        }
        try {
            bless(new JdbcEnvironmentInstaller().install(context, context.getEditor()));
        } catch (IOException | InterruptedException e) {
            throw new AssertionError(e);
        }
        return context.getResource(JdbcEnvironment.class)
                .orElseThrow(AssertionError::new);
    }
}
