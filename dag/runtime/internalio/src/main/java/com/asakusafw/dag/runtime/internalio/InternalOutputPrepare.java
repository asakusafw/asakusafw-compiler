/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.dag.runtime.internalio;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.runtime.io.ModelOutput;

/**
 * Prepares internal output.
 * @since 0.4.0
 */
public class InternalOutputPrepare implements VertexProcessor {

    static final Logger LOG = LoggerFactory.getLogger(InternalOutputPrepare.class);

    /**
     * The placeholder symbol.
     */
    public static final char PLACEHOLDER = '*';

    /**
     * The input edge name.
     */
    public static final String INPUT_NAME = "input";

    /**
     * The path suffix.
     */
    public static final String PATH_SUFFIX = ".bin";

    private final AtomicInteger taskCounter = new AtomicInteger();

    private Spec spec;

    private IoCallable<TaskProcessor> lazy;

    /**
     * Binds an output.
     * The output pattern must contain wildcard character ({@code "*"}).
     * @param id the output ID
     * @param pathPattern the path pattern
     * @param dataType the data type
     * @return this
     */
    public InternalOutputPrepare bind(String id, String pathPattern, Class<?> dataType) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(pathPattern);
        Arguments.requireNonNull(dataType);
        Arguments.requireNonNull(Writable.class.isAssignableFrom(Writable.class));
        Invariants.require(spec == null);
        this.spec = new Spec(id, pathPattern, dataType.asSubclass(Writable.class));
        return this;
    }

    @Override
    public Optional<? extends TaskSchedule> initialize(
            VertexProcessorContext context) throws IOException, InterruptedException {
        Invariants.requireNonNull(spec);
        StageInfo stage = context.getResource(StageInfo.class).orElseThrow(AssertionError::new);
        Configuration conf = context.getResource(Configuration.class).orElseThrow(AssertionError::new);
        String outputPattern = stage.resolveUserVariables(spec.pathPattern);
        int phAt = outputPattern.lastIndexOf(PLACEHOLDER);
        Invariants.require(phAt >= 0, () -> MessageFormat.format(
                "pattern must contain any placeholder symbol ({1}): {0}", //$NON-NLS-1$
                outputPattern,
                PLACEHOLDER));
        String prefix = outputPattern.substring(0, phAt);
        String suffix = phAt == outputPattern.length() - 1 ? PATH_SUFFIX : outputPattern.substring(phAt + 1);
        lazy = () -> {
            Path path = new Path(prefix + taskCounter.incrementAndGet() + suffix);
            return new Proc(conf, spec.id, path, spec.dataType);
        };
        return Optionals.empty();
    }

    @Override
    public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
        Invariants.requireNonNull(lazy);
        return lazy.call();
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "InternalOutputPrepare({0})", //$NON-NLS-1$
                spec);
    }

    private static final class Spec {

        final String id;

        final String pathPattern;

        final Class<? extends Writable> dataType;

        Spec(String id, String pathPattern, Class<? extends Writable> dataType) {
            this.id = id;
            this.pathPattern = pathPattern;
            this.dataType = dataType;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "Spec(id={0}, dataType={1})", //$NON-NLS-1$
                    id, dataType.getSimpleName());
        }
    }

    private static final class Proc implements TaskProcessor {

        private final Configuration configuration;

        private final String id;

        private final Path path;

        private final Class<? extends Writable> dataType;

        private ModelOutput<Writable> output;

        Proc(Configuration configuration, String id, Path path, Class<? extends Writable> dataType) {
            this.configuration = configuration;
            this.id = id;
            this.path = path;
            this.dataType = dataType;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run(TaskProcessorContext context) throws IOException, InterruptedException {
            if (output == null) {
                LOG.debug("starting internal file output: {} ({})", id, path);
                FileSystem fs = path.getFileSystem(configuration);
                output = (ModelOutput<Writable>) InternalOutputHandler.create(fs.create(path), dataType);
            }
            try (ObjectReader reader = (ObjectReader) context.getInput(INPUT_NAME)) {
                while (reader.nextObject()) {
                    Writable obj = (Writable) reader.getObject();
                    output.write(obj);
                }
            } catch (Throwable t) {
                LOG.error(MessageFormat.format(
                        "error occurred while writing output: {0}",
                        path), t);
                throw t;
            }
        }

        @Override
        public void close() throws IOException, InterruptedException {
            if (output != null) {
                output.close();
                output = null;
            }
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "InternalOutput(path={0}, dataType={1})", //$NON-NLS-1$
                    path, dataType.getSimpleName());
        }
    }
}
