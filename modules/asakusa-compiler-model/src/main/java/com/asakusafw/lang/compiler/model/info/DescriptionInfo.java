package com.asakusafw.lang.compiler.model.info;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Structural information of DSL element with original description class.
 */
public interface DescriptionInfo {

    /**
     * Returns the original description class.
     * @return the description class
     */
    ClassDescription getDescriptionClass();
}
