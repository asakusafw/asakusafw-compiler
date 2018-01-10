/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
import java.text.MessageFormat;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.counter.CounterRepository;
import com.asakusafw.dag.api.processor.GroupReader;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.runtime.io.HadoopObjectFactory;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.runtime.directio.Counter;
import com.asakusafw.runtime.directio.DataDefinition;
import com.asakusafw.runtime.directio.DataFormat;
import com.asakusafw.runtime.directio.DirectDataSource;
import com.asakusafw.runtime.directio.DirectDataSourceRepository;
import com.asakusafw.runtime.directio.OutputAttemptContext;
import com.asakusafw.runtime.directio.hadoop.HadoopDataSourceUtil;
import com.asakusafw.runtime.io.ModelOutput;

/**
 * Prepares Direct I/O file output.
 * @since 0.4.0
 */
public class DirectFileOutputPrepare implements VertexProcessor {

    static final Logger LOG = LoggerFactory.getLogger(DirectFileOutputPrepare.class);

    /**
     * The placeholder symbol.
     */
    public static final char PLACEHOLDER = '*';

    /**
     * The input edge name.
     */
    public static final String INPUT_NAME = "input";

    private final AtomicInteger taskCounter = new AtomicInteger();

    private Spec spec;

    private IoCallable<TaskProcessor> lazy;

    /**
     * Binds an output.
     * @param id the output ID
     * @param basePath the base path
     * @param outputPattern the output pattern for flat outputs, or {@code null} for group outputs
     * @param formatType the data format type
     * @return this
     */
    public DirectFileOutputPrepare bind(
            String id, String basePath, String outputPattern,
            Class<? extends DataFormat<?>> formatType) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(basePath);
        Arguments.requireNonNull(formatType);
        Invariants.require(spec == null);
        this.spec = new Spec(id, basePath, outputPattern, formatType);
        return this;
    }

    @Override
    public Optional<? extends TaskSchedule> initialize(
            VertexProcessorContext context) throws IOException, InterruptedException {
        Invariants.requireNonNull(spec);
        StageInfo stage = context.getResource(StageInfo.class).orElseThrow(AssertionError::new);
        String vertexId = context.getVertexId();
        Configuration conf = context.getResource(Configuration.class).orElseThrow(AssertionError::new);
        DirectFileCounterGroup counters = context.getResource(CounterRepository.class)
                .orElse(CounterRepository.DETACHED)
                .get(DirectFileCounterGroup.CATEGORY_OUTPUT, spec.id);
        if (spec.outputPattern != null) {
            String outputPattern = stage.resolveUserVariables(spec.outputPattern);
            int phAt = outputPattern.lastIndexOf(PLACEHOLDER);
            Invariants.require(phAt >= 0, () -> MessageFormat.format(
                    "pattern does not contain any placeholder symbol ({1}): {0}", //$NON-NLS-1$
                    outputPattern,
                    PLACEHOLDER));
            lazy = () -> {
                DirectFileOutputDriver d = resolve(conf, stage, vertexId, counters);
                String resolvedPath = new StringBuilder()
                        .append(outputPattern, 0, phAt)
                        .append(d.getContext().getAttemptId())
                        .append(outputPattern, phAt + 1, outputPattern.length())
                        .toString();
                return new FlatTask(d, d.newInstance(resolvedPath));
            };
        } else {
            lazy = () -> new GroupTask(resolve(conf, stage, vertexId, counters));
        }
        return Optionals.empty();
    }

    private DirectFileOutputDriver resolve(
            Configuration conf, StageInfo stage, String vertexId,
            DirectFileCounterGroup counters) throws IOException, InterruptedException {
        DirectDataSourceRepository repository = HadoopDataSourceUtil.loadRepository(conf);
        String base = stage.resolveUserVariables(spec.basePath);
        String sourceId = repository.getRelatedId(base);
        DirectDataSource dataSource = repository.getRelatedDataSource(base);
        String componentPath = repository.getComponentPath(base);
        DataDefinition<?> definition = BasicDataDefinition.newInstance(
                new HadoopObjectFactory(conf),
                spec.formatType, null);
        String taskId = String.format("%s-%d", vertexId, taskCounter.incrementAndGet()); //$NON-NLS-1$
        DirectFileOutputDriver driver = new DirectFileOutputDriver(
                new OutputAttemptContext(stage.getExecutionId(), taskId, sourceId, new Counter()),
                dataSource, definition, componentPath,
                stage::resolveUserVariables,
                counters);
        return driver;
    }

    @Override
    public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
        Invariants.requireNonNull(lazy);
        return lazy.call();
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "DirectFileOutputPrepare({0})", //$NON-NLS-1$
                spec);
    }

    private static final class Spec {

        final String id;

        final String basePath;

        final String outputPattern;

        final Class<? extends DataFormat<?>> formatType;

        Spec(String outputId,
                String basePath, String outputPattern,
                Class<? extends DataFormat<?>> formatType) {
            this.id = outputId;
            this.basePath = basePath;
            this.outputPattern = outputPattern;
            this.formatType = formatType;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "Spec(id={0}, basePath={1})", //$NON-NLS-1$
                    id, basePath);
        }
    }

    private static final class FlatTask implements TaskProcessor {

        private final DirectFileOutputDriver driver;

        private final ModelOutput<Object> output;

        FlatTask(DirectFileOutputDriver driver, ModelOutput<Object> output) {
            assert driver != null;
            this.driver = driver;
            this.output = output;
        }

        @Override
        public void run(TaskProcessorContext context) throws IOException, InterruptedException {
            try (ObjectReader reader = (ObjectReader) context.getInput(INPUT_NAME)) {
                long count = 0;
                while (reader.nextObject()) {
                    count++;
                    output.write(reader.getObject());
                }
                driver.getRecordCounter().add(count);
            } catch (Throwable t) {
                driver.error(t);
                throw t;
            }
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try {
                output.close();
            } catch (Throwable t) {
                driver.error(t);
            } finally {
                driver.close();
            }
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "Flat({0})", //$NON-NLS-1$
                    driver);
        }
    }

    private static final class GroupTask implements TaskProcessor {

        private final DirectFileOutputDriver driver;

        GroupTask(DirectFileOutputDriver driver) {
            assert driver != null;
            this.driver = driver;
        }

        @Override
        public void run(TaskProcessorContext context) throws IOException, InterruptedException {
            try (GroupReader reader = (GroupReader) context.getInput(INPUT_NAME)) {
                long count = 0;
                while (reader.nextGroup()) {
                    String resource = (String) reader.getGroup().getValue();
                    try (ModelOutput<Object> output = driver.newInstance(resource)) {
                        while (reader.nextObject()) {
                            count++;
                            output.write(reader.getObject());
                        }
                    }
                }
                driver.getRecordCounter().add(count);
            } catch (Throwable t) {
                driver.error(t);
                throw t;
            }
        }

        @Override
        public void close() throws IOException, InterruptedException {
            driver.close();
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "Group({0})", //$NON-NLS-1$
                    driver);
        }
    }
}
