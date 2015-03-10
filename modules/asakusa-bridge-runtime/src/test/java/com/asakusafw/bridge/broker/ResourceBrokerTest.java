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

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.asakusafw.bridge.broker.ResourceBroker.Initializer;

/**
 * Test for {@link ResourceBroker}.
 */
public class ResourceBrokerTest {

    /**
     * setup/cleanup the test case.
     */
    @Before
    @After
    public void clean() {
        ResourceBroker.closeAll();
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        try (ResourceSession session = ResourceBroker.start()) {
            session.put(String.class, "Hello, world!");
            assertThat(ResourceBroker.get(String.class), is("Hello, world!"));
        }
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void put() throws Exception {
        try (ResourceSession session = ResourceBroker.start()) {
            ResourceBroker.put(String.class, "Hello, world!");
            assertThat(session.get(String.class), is("Hello, world!"));
        }
    }

    /**
     * re-enter session.
     * @throws Exception if failed
     */
    @Test(expected = IllegalStateException.class)
    public void reentrant() throws Exception {
        try (ResourceSession session = ResourceBroker.start()) {
            ResourceBroker.start();
        }
    }

    /**
     * attach.
     * @throws Exception if failed
     */
    @Test
    public void attach() throws Exception {
        Initializer initializer = new Initializer() {
            @Override
            public void accept(ResourceSession session) throws IOException {
                session.put(String.class, "Hello, world!");
            }
        };
        try (ResourceSession session = ResourceBroker.attach(initializer)) {
            assertThat(session.get(String.class), is("Hello, world!"));
            session.put(Integer.class, 100);
            try (ResourceSession other = ResourceBroker.attach(initializer)) {
                assertThat(other.get(String.class), is("Hello, world!"));
                assertThat(other.get(Integer.class), is(100));
            }
            assertThat(ResourceBroker.get(String.class), is("Hello, world!"));
            assertThat(ResourceBroker.get(Integer.class), is(100));
        }
    }

    /**
     * attach.
     * @throws Exception if failed
     */
    @Test
    public void attach_detached() throws Exception {
        Initializer initializer = new Initializer() {
            @Override
            public void accept(ResourceSession session) throws IOException {
                session.put(String.class, "Hello, world!");
            }
        };
        try (ResourceSession session = ResourceBroker.attach(initializer)) {
            assertThat(session.get(String.class), is("Hello, world!"));
            session.put(Integer.class, 100);
            assertThat(ResourceBroker.get(String.class), is("Hello, world!"));
            assertThat(ResourceBroker.find(Integer.class), is(100));
        }
        try (ResourceSession session = ResourceBroker.attach(initializer)) {
            assertThat(ResourceBroker.get(String.class), is("Hello, world!"));
            assertThat(ResourceBroker.find(Integer.class), is(nullValue()));
        }
    }
}
