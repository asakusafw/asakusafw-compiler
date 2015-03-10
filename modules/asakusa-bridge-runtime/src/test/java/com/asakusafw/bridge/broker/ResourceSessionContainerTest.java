/**
 * Copyright 2011-2015 Asakusa Framework Team.
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

import java.util.concurrent.Callable;
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
     */
    @Test
    public void simple() {
        ResourceSessionContainer container = new ResourceSessionContainer();
        try (ResourceSessionEntity entity = container.create(Scope.VM)) {
            assertThat(container.create(Scope.VM), is(nullValue()));
            assertThat(container.find(), is(sameInstance(entity)));
        }
    }

    /**
     * missing session.
     */
    @Test
    public void miss() {
        ResourceSessionContainer container = new ResourceSessionContainer();
        assertThat(container.find(), is(nullValue()));
    }

    /**
     * isolated by thread.
     */
    @Test
    public void isolate_vm() {
        ResourceSessionContainer container = new ResourceSessionContainer();
        try (ResourceSessionEntity entity = container.create(Scope.VM)) {
            boolean created = createConcurrent(container, Scope.VM);
            assertThat(created, is(false));
        }
    }

    /**
     * isolated by thread.
     */
    @Test
    public void isolate_thread() {
        ResourceSessionContainer container = new ResourceSessionContainer();
        try (ResourceSessionEntity entity = container.create(Scope.THEAD)) {
            boolean created = createConcurrent(container, Scope.THEAD);
            assertThat(created, is(true));
        }
    }

    /**
     * inconsistent scope w/ active sessions.
     */
    @Test(expected = IllegalStateException.class)
    public void inconsistent_scope() {
        ResourceSessionContainer container = new ResourceSessionContainer();
        try (ResourceSessionEntity entity = container.create(Scope.VM)) {
            container.create(Scope.THEAD);
        }
    }

    /**
     * inconsistent scope w/o any active sessions.
     */
    @Test
    public void inconsistent_scope_empty() {
        ResourceSessionContainer container = new ResourceSessionContainer();
        container.create(Scope.VM).close();
        container.create(Scope.THEAD).close();
        container.create(Scope.VM).close();
        container.create(Scope.THEAD).close();
    }

    private boolean createConcurrent(final ResourceSessionContainer container, final Scope scope) {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            Future<Boolean> future = executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    ResourceSessionEntity entity = container.create(scope);
                    if (entity == null) {
                        return false;
                    } else {
                        entity.close();
                        return true;
                    }
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
