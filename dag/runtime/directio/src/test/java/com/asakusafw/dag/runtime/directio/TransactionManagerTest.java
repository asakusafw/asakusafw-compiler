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
package com.asakusafw.dag.runtime.directio;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Collections;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.lang.compiler.mapreduce.testing.mock.DirectIoContext;
import com.asakusafw.runtime.directio.OutputTransactionContext;
import com.asakusafw.runtime.windows.WindowsSupport;

/**
 * Test for {@link TransactionManager}.
 */
public class TransactionManagerTest {

    /**
     * Support for Windows platform.
     */
    @ClassRule
    public static final WindowsSupport WINDOWS_SUPPORT = new WindowsSupport();

    /**
     * Direct I/O testing context.
     */
    @Rule
    public final DirectIoContext directio = new DirectIoContext();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        TransactionManager tm = new TransactionManager(directio.newConfiguration(), "testing", Collections.emptyMap());

        OutputTransactionContext a = tm.acquire("a");
        assertThat(tm.isCommitted(), is(false));

        tm.begin();
        assertThat(tm.isCommitted(), is(true));

        tm.release(a);
        assertThat(tm.isCommitted(), is(true));

        tm.end();
        assertThat(tm.isCommitted(), is(false));
    }

    /**
     * empty actions.
     * @throws Exception if failed
     */
    @Test
    public void no_action() throws Exception {
        TransactionManager tm = new TransactionManager(directio.newConfiguration(), "testing", Collections.emptyMap());
        assertThat(tm.isCommitted(), is(false));

        tm.begin();
        assertThat(tm.isCommitted(), is(true));

        tm.end();
        assertThat(tm.isCommitted(), is(false));
    }

    /**
     * still running.
     * @throws Exception if failed
     */
    @Test
    public void still_running() throws Exception {
        TransactionManager tm = new TransactionManager(directio.newConfiguration(), "testing", Collections.emptyMap());

        OutputTransactionContext a = tm.acquire("a");
        tm.acquire("orphan");
        assertThat(tm.isCommitted(), is(false));

        tm.begin();
        assertThat(tm.isCommitted(), is(true));

        tm.release(a);
        assertThat(tm.isCommitted(), is(true));

        tm.end();
        assertThat(tm.isCommitted(), is(true));
    }
}
