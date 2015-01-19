package com.asakusafw.lang.compiler.api.reference;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.graph.ExternalPort;

/**
 * A symbol of external ports.
 * @param <T> the original I/O port type
 */
public class ExternalPortReference<T extends ExternalPort> implements Reference {

    private final T port;

    private final String path;

    /**
     * Creates a new instance.
     * @param port the original port
     * @param path the resolved path
     */
    public ExternalPortReference(T port, String path) {
        this.port = port;
        this.path = path;
    }

    /**
     * Returns the original I/O port.
     * @return the original I/O port
     */
    public T getPort() {
        return port;
    }

    /**
     * Returns the resolved path (may includes wildcard).
     * @return the resolved path
     */
    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ExternalPort(path={0}, port={1})",
                getPath(),
                getPort());
    }
}
