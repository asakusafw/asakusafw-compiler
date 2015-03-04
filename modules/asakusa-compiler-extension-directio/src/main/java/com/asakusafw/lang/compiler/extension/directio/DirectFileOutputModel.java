package com.asakusafw.lang.compiler.extension.directio;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.vocabulary.directio.DirectFileOutputDescription;

/**
 * Persistent direct file output description model.
 */
public class DirectFileOutputModel implements Serializable {

    private static final long serialVersionUID = -4546238057952201598L;

    private String basePath;

    private String resourcePattern;

    private String[] order;

    private String[] deletePatterns;

    private String formatClass;

    /**
     * Creates a new instance for serializers.
     */
    protected DirectFileOutputModel() {
        return;
    }

    /**
     * Creates a new instance.
     * @param description the original description
     */
    public DirectFileOutputModel(DirectFileOutputDescription description) {
        this.basePath = description.getBasePath();
        this.resourcePattern = description.getResourcePattern();
        this.order = array(description.getOrder());
        this.deletePatterns = array(description.getDeletePatterns());
        this.formatClass = description.getFormat().getName();
    }

    private String[] array(List<String> list) {
        return list.toArray(new String[list.size()]);
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
     * Returns the record order.
     * @return the record order
     */
    public List<String> getOrder() {
        return Arrays.asList(order);
    }

    /**
     * Returns the delete patterns.
     * @return the delete patterns
     */
    public List<String> getDeletePatterns() {
        return Arrays.asList(deletePatterns);
    }

    /**
     * Returns the format class.
     * @return the format class
     */
    public ClassDescription getFormatClass() {
        return new ClassDescription(formatClass);
    }
}
