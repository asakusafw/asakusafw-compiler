package com.asakusafw.bridge.broker;

import org.junit.rules.ExternalResource;

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
