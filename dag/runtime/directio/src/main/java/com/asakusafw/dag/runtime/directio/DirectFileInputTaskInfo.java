/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
import java.util.function.Supplier;

import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.runtime.adapter.ModelInputTaskInfo;
import com.asakusafw.dag.runtime.io.CountingModelInput;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.runtime.directio.DataDefinition;
import com.asakusafw.runtime.directio.DirectDataSource;
import com.asakusafw.runtime.directio.DirectInputFragment;
import com.asakusafw.runtime.io.ModelInput;

/**
 * A {@link TaskInfo} for Direct I/O file input.
 * @param <T> the input data type
 * @since 0.4.0
 */
public class DirectFileInputTaskInfo<T> implements ModelInputTaskInfo<T> {

    private final DirectDataSource dataSource;

    private final DataDefinition<T> dataDefinition;

    private final DirectInputFragment fragment;

    private final Supplier<? extends T> objectFactory;

    private final DirectFileCounterGroup counters;

    /**
     * Creates a new instance.
     * @param dataSource input data source
     * @param dataDefinition input data definition
     * @param fragment input fragment
     * @param objectFactory object factory for creating buffer objects
     * @param counters the counter group
     */
    public DirectFileInputTaskInfo(
            DirectDataSource dataSource,
            DataDefinition<T> dataDefinition,
            DirectInputFragment fragment,
            Supplier<? extends T> objectFactory,
            DirectFileCounterGroup counters) {
        Arguments.requireNonNull(dataSource);
        Arguments.requireNonNull(dataDefinition);
        Arguments.requireNonNull(fragment);
        Arguments.requireNonNull(objectFactory);
        Arguments.requireNonNull(counters);
        this.dataSource = dataSource;
        this.dataDefinition = dataDefinition;
        this.fragment = fragment;
        this.objectFactory = objectFactory;
        this.counters = counters;
    }

    @Override
    public ModelInput<T> open() throws IOException, InterruptedException {
        return new CountingModelInput<>(
                dataSource.openInput(dataDefinition, fragment, counters.getFileSize()),
                counters.getRecordCount()::add);
    }

    @Override
    public T newDataObject() {
        return objectFactory.get();
    }
}
