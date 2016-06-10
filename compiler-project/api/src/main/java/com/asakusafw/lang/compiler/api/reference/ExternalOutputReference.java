/**
 * Copyright 2011-2016 Asakusa Framework Team.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    public boolean isGenerator() {
        return info.isGenerator();
    }

    @Override
    public Set<String> getParameterNames() {
        return info.getParameterNames();
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
                name, paths);
    }
}
