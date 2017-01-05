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
package com.asakusafw.bridge.broker;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import com.asakusafw.bridge.broker.ResourceBroker.Scope;

/**
 * Test for {@link ResourceSessionContainer}.
 */
public class ResourceSessionContainerTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        ResourceSessionContainer container = new ResourceSessionContainer();
        try (ResourceSessionEntity.Reference session = container.create(Scope.VM)) {
            assertThat(container.create(Scope.VM), is(nullValue()));
            assertThat(container.find(), is(sameInstance(session.getEntity())));
        }
    }

    /**
     * missing session.
     * @throws Exception if failed
     */
    @Test
    public void miss() throws Exception {
        ResourceSessionContainer container = new ResourceSessionContainer();
        assertThat(container.find(), is(nullValue()));
    }

    /**
     * isolated by thread.
     * @throws Exception if failed
     */
    @Test
    public void isolate_vm() throws Exception {
        ResourceSessionContainer container = new ResourceSessionContainer();
        try (ResourceSession session = container.create(Scope.VM)) {
            boolean created = createConcurrent(container, Scope.VM);
            assertThat(created, is(false));
        }
    }

    /**
     * isolated by thread.
     * @throws Exception if failed
     */
    @Test
    public void isolate_thread() throws Exception {
        ResourceSessionContainer container = new ResourceSessionContainer();
        try (ResourceSession entity = container.create(Scope.THREAD)) {
            boolean created = createConcurrent(container, Scope.THREAD);
            assertThat(created, is(true));
        }
    }

    /**
     * inconsistent scope w/ active sessions.
     * @throws Exception if failed
     */
    @Test(expected = IllegalStateException.class)
    public void inconsistent_scope() throws Exception {
        ResourceSessionContainer container = new ResourceSessionContainer();
        try (ResourceSession session = container.create(Scope.VM)) {
            container.create(Scope.THREAD);
        }
    }

    /**
     * inconsistent scope w/o any active sessions.
     * @throws Exception if failed
     */
    @Test
    public void inconsistent_scope_empty() throws Exception {
        ResourceSessionContainer container = new ResourceSessionContainer();
        container.create(Scope.VM).close();
        container.create(Scope.THREAD).close();
        container.create(Scope.VM).close();
        container.create(Scope.THREAD).close();
    }

    private boolean createConcurrent(ResourceSessionContainer container, Scope scope) {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            Future<Boolean> future = executor.submit(() -> {
                try (ResourceSession session = container.create(scope)) {
                    return session != null;
                }
            });
            return future.get();
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            executor.shutdownNow();
        }
    }
}
