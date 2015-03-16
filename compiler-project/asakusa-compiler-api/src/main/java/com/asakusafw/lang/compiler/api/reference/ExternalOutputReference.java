package com.asakusafw.lang.compiler.api.reference;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.common.BasicAttributeContainer;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;

/**
 * A symbol of individual external outputs.
 */
public class ExternalOutputReference extends BasicAttributeContainer
        implements ExternalOutputInfo, ExternalPortReference {

    private final String name;

    private final ExternalOutputInfo info;

    private final Set<String> paths;

    /**
     * Creates a new instance.
     * @param name the original output name.
     * @param info the structural information of this external output
     * @param paths the actual output paths for tasks
     */
    public ExternalOutputReference(String name, ExternalOutputInfo info, Collection<String> paths) {
        this.name = name;
        this.info = info;
        this.paths = Collections.unmodifiableSet(new LinkedHashSet<>(paths));
    }

    /**
     * Returns the original output name.
     * @return the original output name
     */
    @Override
    public String getName() {
        return name;
    }

    @Override
    public ClassDescription getDescriptionClass() {
        return info.getDescriptionClass();
    }

    @Override
    public ClassDescription getDataModelClass() {
        return info.getDataModelClass();
    }

    @Override
    public String getModuleName() {
        return info.getModuleName();
    }

    @Override
    public ValueDescription getContents() {
        return info.getContents();
    }

    /**
     * The actual output paths from tasks.
     * The framework will export them after executing tasks in the jobflow.
     * The paths may include wildcard characters.
     *
     * @return the paths
     */
    @Override
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
