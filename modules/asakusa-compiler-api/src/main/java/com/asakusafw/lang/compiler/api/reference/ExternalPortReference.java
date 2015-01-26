package com.asakusafw.lang.compiler.api.reference;

import java.util.Set;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An abstract interface for a symbol of external input or output.
 */
public interface ExternalPortReference {

    /**
     * Returns the original port name.
     * @return the original port name
     */
    String getName();

    /**
     * Returns the importer/exporter description class.
     * @return the importer/exporter description class
     */
    ClassDescription getDescriptionClass();

    /**
     * The internal paths for the target port.
     * The paths may include wildcard characters.
     * @return the paths
     */
    Set<String> getPaths();
}