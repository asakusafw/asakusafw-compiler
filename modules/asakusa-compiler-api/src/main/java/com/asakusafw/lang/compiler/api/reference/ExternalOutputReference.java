package com.asakusafw.lang.compiler.api.reference;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * A symbol of individual external outputs.
 */
public class ExternalOutputReference {

    private final String name;

    private final ClassDescription descriptionClass;

    private final Set<String> paths;

    /**
     * Creates a new instance.
     * @param name the original output name.
     * @param descriptionClass the exporter description class
     * @param paths the actual output paths for tasks
     */
    public ExternalOutputReference(String name, ClassDescription descriptionClass, Collection<String> paths) {
        this.name = name;
        this.descriptionClass = descriptionClass;
        this.paths = Collections.unmodifiableSet(new LinkedHashSet<>(paths));
    }

    /**
     * Returns the original output name.
     * @return the original output name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the exporter description class.
     * @return the exporter description class
     */
    public ClassDescription getDescriptionClass() {
        return descriptionClass;
    }

    /**
     * The actual output paths for tasks.
     * The paths may include wildcard characters
     * @return the paths
     */
    public Set<String> getPaths() {
        return paths;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ExternalOutput(name={0}, paths={1})", //$NON-NLS-1$
                paths);
    }
}
