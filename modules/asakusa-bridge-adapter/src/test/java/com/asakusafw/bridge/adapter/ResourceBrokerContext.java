package com.asakusafw.bridge.adapter;

import org.junit.rules.ExternalResource;

import com.asakusafw.bridge.broker.ResourceBroker;

/**
 * Initializes {@link ResourceBroker} in testing.
 */
public class ResourceBrokerContext extends ExternalResource {

    @Override
    protected void before() throws Throwable {
        ResourceBroker.closeAll();
    }

    @Override
    protected void after() {
        ResourceBroker.closeAll();
    }
}
