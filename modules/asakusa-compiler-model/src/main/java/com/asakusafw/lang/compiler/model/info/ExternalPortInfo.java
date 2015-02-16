package com.asakusafw.lang.compiler.model.info;

import java.util.Map;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;

/**
 * Structural information of external inputs/outputs.
 */
public interface ExternalPortInfo {

    /**
     * Returns the original importer/exporter description class.
     * @return the importer description class
     */
    ClassDescription getDescriptionClass();

    /**
     * Returns the target data model class.
     * @return the target data model class
     */
    ClassDescription getDataModelClass();

    /**
     * Returns the importer module name.
     * @return the importer module name
     */
    String getModuleName();

    /**
     * Returns the extra properties.
     * @return the extra properties
     */
    Map<String, ValueDescription> getProperties();
}
