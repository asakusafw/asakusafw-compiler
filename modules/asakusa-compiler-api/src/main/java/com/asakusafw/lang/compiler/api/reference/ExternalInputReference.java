package com.asakusafw.lang.compiler.api.reference;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import com.asakusafw.lang.compiler.common.BasicAttributeContainer;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;

/**
 * A symbol of individual external inputs.
 */
public class ExternalInputReference extends BasicAttributeContainer
        implements ExternalInputInfo, ExternalPortReference {

    private final String name;

    private final ExternalInputInfo info;

    private final Set<String> paths;

    /**
     * Creates a new instance.
     * @param name the original input name.
     * @param info the structural information of this external input
     * @param paths the actual input paths for tasks
     */
    public ExternalInputReference(String name, ExternalInputInfo info, Collection<String> paths) {
        this.name = name;
        this.info = new ExternalInputInfo.Basic(info);
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

    @Override
    public ClassDescription getDescriptionClass() {
        return info.getDescriptionClass();
    }

    @Override
    public DataSize getDataSize() {
        return info.getDataSize();
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
