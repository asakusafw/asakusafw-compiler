package com.asakusafw.lang.compiler.model.info;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;

/**
 * Structural information of external inputs/outputs.
 */
public interface ExternalPortInfo extends DescriptionInfo {

    /**
     * Returns the original importer/exporter description class.
     * @return the importer description class
     */
    @Override
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
     * Returns the processor specific contents.
     * @return the processor specific contents, or {@code null} if there are no special contents
     */
    ValueDescription getContents();
}
