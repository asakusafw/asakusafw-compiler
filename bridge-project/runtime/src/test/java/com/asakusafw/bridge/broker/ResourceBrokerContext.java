/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import org.junit.rules.ExternalResource;

import com.asakusafw.bridge.api.activate.ApiActivator;

/**
 * Initializes {@link ResourceBroker} in testing.
 */
public class ResourceBrokerContext extends ExternalResource {

    private final boolean start;

    /**
     * Creates a new instance.
     */
    public ResourceBrokerContext() {
        this(false);
    }

    /**
     * Creates a new instance.
     * @param start {@code true} to start a new session, otherwise {@code false}
     */
    public ResourceBrokerContext(boolean start) {
        this.start = start;
    }

    /**
     * Activates framework APIs.
     */
    public void activateApis() {
        ApiActivator.load(getClass().getClassLoader()).forEach(a -> ResourceBroker.schedule(a.activate()));
    }

    @Override
    protected void before() throws Throwable {
        ResourceBroker.closeAll();
        if (start) {
            ResourceBroker.start();
        }
    }

    @Override
    protected void after() {
        ResourceBroker.closeAll();
    }
}
