package com.asakusafw.lang.compiler.api.reference;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * A symbol of individual external inputs.
 */
public class ExternalInputReference implements ExternalPortReference {

    private final String name;

    private final ClassDescription descriptionClass;

    private final Set<String> paths;

    /**
     * Creates a new instance.
     * @param name the original input name.
     * @param descriptionClass the importer description class
     * @param paths the actual input paths for tasks
     */
    public ExternalInputReference(String name, ClassDescription descriptionClass, Collection<String> paths) {
        this.name = name;
        this.descriptionClass = descriptionClass;
        this.paths = Collections.unmodifiableSet(new LinkedHashSet<>(paths));
    }

    /**
     * Returns the original input name.
     * @return the original input name
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns the importer description class.
     * @return the importer description class
     */
    @Override
    public ClassDescription getDescriptionClass() {
        return descriptionClass;
    }

    /**
     * The actual input paths for tasks.
     * The framework will import and put data on the paths before executing tasks in the jobflow.
     * The paths may include wildcard characters.
     * @return the paths
     */
    @Override
    public Set<String> getPaths() {
        return paths;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ExternalInput(name={0}, paths={1})", //$NON-NLS-1$
                paths);
    }
}
