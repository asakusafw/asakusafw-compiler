package com.asakusafw.lang.compiler.api.reference;

import java.util.Set;

import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;

/**
 * An abstract interface for a symbol of external input or output.
 */
public interface ExternalPortReference extends ExternalPortInfo, Reference {

    /**
     * Returns the original port name.
     * @return the original port name
     */
    String getName();

    /**
     * The internal paths for the target port.
     * The paths may include wildcard characters.
     * @return the paths
     */
    Set<String> getPaths();
}