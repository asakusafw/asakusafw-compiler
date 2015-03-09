package com.asakusafw.bridge.redirector.mock;


@SuppressWarnings("javadoc")
public class MockCaller {

    @Override
    public String toString() {
        return String.format("%s:%s:%s",
                new MockCallee0().get(),
                MockCallee0.call(),
                MockCallee1.call());
    }
}
