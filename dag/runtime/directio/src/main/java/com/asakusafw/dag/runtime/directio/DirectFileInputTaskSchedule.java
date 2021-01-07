/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.dag.runtime.directio;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.asakusafw.dag.api.common.ObjectFactory;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.runtime.directio.DataDefinition;
import com.asakusafw.runtime.directio.DataFilter;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.runtime.directio.DirectDataSource;
import com.asakusafw.runtime.directio.DirectDataSourceRepository;
import com.asakusafw.runtime.directio.DirectInputFragment;
import com.asakusafw.runtime.directio.FilePattern;
import com.asakusafw.runtime.directio.ResourcePattern;

/**
 * A {@link TaskSchedule} for Direct I/O file input.
 * @since 0.4.0
 */
public class DirectFileInputTaskSchedule implements TaskSchedule {

    private final DirectDataSourceRepository repository;

    private final ObjectFactory factory;

    private final DataFilter.Context filterContext;

    private final Function<String, String> variables;

    private final List<DirectFileInputTaskInfo<?>> tasks = new ArrayList<>();

    /**
     * Creates a new instance.
     * @param repository the Direct I/O data source repository
     * @param filterContext the data filter context
     * @param factory an object factory
     * @param variableResolver the user variable resolver
     */
    public DirectFileInputTaskSchedule(
            DirectDataSourceRepository repository,
            DataFilter.Context filterContext,
            ObjectFactory factory,
            Function<String, String> variableResolver) {
        Arguments.requireNonNull(repository);
        Arguments.requireNonNull(filterContext);
        Arguments.requireNonNull(factory);
        Arguments.requireNonNull(variableResolver);
        this.repository = repository;
        this.filterContext = filterContext;
        this.factory = factory;
        this.variables = variableResolver;
    }

    /**
     * Returns a resolved input path pattern string.
     * @param basePath the base path
     * @param resourcePattern the resource pattern
     * @return the resolved input path
     * @throws IOException if I/O error was occurred while resolving path
     * @throws InterruptedException if interrupted while resolving path
     */
    public String resolve(String basePath, String resourcePattern) throws IOException, InterruptedException {
        Arguments.requireNonNull(basePath);
        Arguments.requireNonNull(resourcePattern);
        String resolvedBasePath = variables.apply(basePath);
        ResourcePattern resolvedResourcePattern = FilePattern.compile(variables.apply(resourcePattern));
        String containerPath = repository.getContainerPath(resolvedBasePath);
        String componentPath = repository.getComponentPath(resolvedBasePath);
        DirectDataSource source = repository.getRelatedDataSource(containerPath);
        return source.path(componentPath, resolvedResourcePattern);
    }

    /**
     * Adds an input pattern.
     * @param basePath the base path
     * @param resourcePattern the resource pattern
     * @param dataFormat the data format class
     * @param dataFilter the data filter class (optional)
     * @param counters the input counters
     * @return the added number of input fragments
     * @throws IOException if I/O error was occurred while computing input
     * @throws InterruptedException if interrupted while computing input
     */
    public int addInput(
            String basePath,
            String resourcePattern,
            Class<? extends DataFormat<?>> dataFormat,
            Class<? extends DataFilter<?>> dataFilter,
            DirectFileCounterGroup counters) throws IOException, InterruptedException {
        Arguments.requireNonNull(basePath);
        Arguments.requireNonNull(resourcePattern);
        Arguments.requireNonNull(dataFormat);
        Arguments.requireNonNull(counters);
        String resolvedBasePath = variables.apply(basePath);
        ResourcePattern resolvedResourcePattern = FilePattern.compile(variables.apply(resourcePattern));
        DataDefinition<?> definition = BasicDataDefinition.newInstance(factory, dataFormat, dataFilter);
        if (definition.getDataFilter() != null) {
            definition.getDataFilter().initialize(filterContext);
        }
        return addInput0(resolvedBasePath, resolvedResourcePattern, definition, counters);
    }

    private <T> int addInput0(
            String basePath,
            ResourcePattern resourcePattern,
            DataDefinition<T> definition,
            DirectFileCounterGroup counters) throws IOException, InterruptedException {
        String containerPath = repository.getContainerPath(basePath);
        String componentPath = repository.getComponentPath(basePath);
        DirectDataSource source = repository.getRelatedDataSource(containerPath);
        List<DirectInputFragment> fragments = source.findInputFragments(definition, componentPath, resourcePattern);
        fragments.stream()
                .map(fragment -> new DirectFileInputTaskInfo<>(
                        source, definition, fragment,
                        () -> factory.newInstance(definition.getDataClass()),
                        counters))
                .forEach(tasks::add);
        return fragments.size();
    }

    @Override
    public List<DirectFileInputTaskInfo<?>> getTasks() {
        return new ArrayList<>(tasks);
    }
}
