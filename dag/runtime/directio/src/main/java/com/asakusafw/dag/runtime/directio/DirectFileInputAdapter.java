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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.counter.CounterRepository;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.dag.runtime.adapter.ExtractOperation.Input;
import com.asakusafw.dag.runtime.adapter.InputAdapter;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.io.HadoopObjectFactory;
import com.asakusafw.dag.runtime.skeleton.ModelInputHandler;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.directio.DataFilter;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil;

/**
 * {@link InputAdapter} for Direct I/O.
 * @since 0.4.0
 */
public class DirectFileInputAdapter implements InputAdapter<ExtractOperation.Input> {

    static final Logger LOG = LoggerFactory.getLogger(DirectFileInputAdapter.class);

    private final StageInfo stage;

    private final Configuration configuration;

    private final CounterRepository counterRoot;

    private final DataFilter.Context filterContext;

    private final List<Action<DirectFileInputTaskSchedule, Exception>> actions = new ArrayList<>();

    /**
     * Creates a new instance.
     * @param context the current context
     */
    public DirectFileInputAdapter(VertexProcessorContext context) {
        Arguments.requireNonNull(context);
        this.stage = context.getResource(StageInfo.class)
                .orElseThrow(IllegalStateException::new);
        this.configuration = context.getResource(Configuration.class)
                .orElseThrow(IllegalStateException::new);
        this.counterRoot = context.getResource(CounterRepository.class)
                .orElse(CounterRepository.DETACHED);
        this.filterContext = new DataFilter.Context(stage.getBatchArguments());
    }

    /**
     * Adds an input pattern.
     * @param id the input ID
     * @param basePath the base path
     * @param resourcePattern the resource pattern
     * @param dataFormat the data format class
     * @param dataFilter the data filter class (optional)
     * @param optional {@code true} if this input is optional, otherwise {@code false}
     * @return this
     */
    public final DirectFileInputAdapter bind(
            String id,
            String basePath, String resourcePattern,
            Class<? extends DataFormat<?>> dataFormat, Class<? extends DataFilter<?>> dataFilter,
            boolean optional) {
        Arguments.requireNonNull(basePath);
        Arguments.requireNonNull(resourcePattern);
        Arguments.requireNonNull(dataFormat);
        actions.add(s -> {
            DirectFileCounterGroup counters = counterRoot.get(DirectFileCounterGroup.CATEGORY_INPUT, id);
            int count = s.addInput(basePath, resourcePattern, dataFormat, dataFilter, counters);
            if (count == 0 && optional == false) {
                String path = s.resolve(basePath, resourcePattern);
                throw new FileNotFoundException(path);
            }
        });
        return this;
    }

    @Override
    public TaskSchedule getSchedule() throws IOException, InterruptedException {
        DirectFileInputTaskSchedule schedule = new DirectFileInputTaskSchedule(
                HadoopDataSourceUtil.loadRepository(configuration),
                filterContext,
                new HadoopObjectFactory(configuration),
                stage::resolveUserVariables);
        try {
            Lang.forEach(actions, a -> a.perform(schedule));
        } catch (IOException | InterruptedException | RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return schedule;
    }

    @Override
    public InputHandler<Input, TaskProcessorContext> newHandler() throws IOException, InterruptedException {
        return new ModelInputHandler();
    }
}
