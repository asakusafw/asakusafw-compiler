package com.asakusafw.lang.compiler.extension.directio;

import java.io.Serializable;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.vocabulary.directio.DirectFileInputDescription;

/**
 * Persistent direct file input description model.
 */
public class DirectFileInputModel implements Serializable {

    private static final long serialVersionUID = -4304774107461133202L;

    private String basePath;

    private String resourcePattern;

    private String formatClass;

    private boolean optional;

    /**
     * Creates a new instance for serializers.
     */
    protected DirectFileInputModel() {
        return;
    }

    /**
     * Creates a new instance.
     * @param description the original description
     */
    public DirectFileInputModel(DirectFileInputDescription description) {
        this.basePath = description.getBasePath();
        this.resourcePattern = description.getResourcePattern();
        this.formatClass = description.getFormat().getName();
        this.optional = description.isOptional();
    }

    /**
     * Returns the base path.
     * @return the base path
     */
    public String getBasePath() {
        return basePath;
    }

    /**
     * Returns the resource pattern.
     * @return the resource pattern
     */
    public String getResourcePattern() {
        return resourcePattern;
    }

    /**
     * Returns the format class.
     * @return the format class
     */
    public ClassDescription getFormatClass() {
        return new ClassDescription(formatClass);
    }

    /**
     * Returns whether the target input is optional or not.
     * @return {@code true} if the target input is optional, otherwise {@code false}
     */
    public boolean isOptional() {
        return optional;
    }
}
