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
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.runtime.adapter.OutputHandler;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Io;
import com.asakusafw.lang.utils.common.Io.Closer;
import com.asakusafw.lang.utils.common.Io.Initializer;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.stage.temporary.TemporaryFileOutput;

/**
 * {@link OutputHandler} for internal outputs.
 * @since 0.4.0
 */
public class InternalOutputHandler implements OutputHandler<TaskProcessorContext> {

    private static final int OUTPUT_INIT_BUFFER_SIZE = 300 * 1024;

    private static final int OUTPUT_PAGE_SIZE = 256 * 1024;

    private final Map<String, Sink<?>> sinks;

    InternalOutputHandler(Configuration configuration, Collection<OutputSpec> specs) {
        Arguments.requireNonNull(configuration);
        Arguments.requireNonNull(specs);
        this.sinks = specs.stream().collect(Collectors.toMap(
                s -> s.id,
                s -> new Sink<>(configuration, s.pathPrefix, s.dataClass)));
    }

    @Override
    public boolean contains(String id) {
        Arguments.requireNonNull(id);
        return sinks.containsKey(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Result<T> getSink(Class<T> outputType, String name) {
        Invariants.require(sinks.containsKey(name));
        return (Result<T>) sinks.get(name);
    }

    @Override
    public Session start(TaskProcessorContext context) throws IOException, InterruptedException {
        try (Initializer<Closer> init = new Initializer<>(new Closer())) {
            for (Sink<?> entry : sinks.values()) {
                entry.open(context.getTaskId());
                init.get().add(entry);
            }
            Closer closer = init.done();
            return () -> closer.close();
        }
    }

    /**
     * Creates a new {@link TemporaryFileOutput}.
     * @param <T> the data type
     * @param stream the target output
     * @param dataType the data type
     * @return the created {@link TemporaryFileOutput}
     */
    public static <T extends Writable> ModelOutput<T> create(OutputStream stream, Class<T> dataType) {
        Arguments.requireNonNull(stream);
        Arguments.requireNonNull(dataType);
        return new TemporaryFileOutput<>(stream, dataType.getName(), OUTPUT_INIT_BUFFER_SIZE, OUTPUT_PAGE_SIZE);
    }

    private static class Sink<T extends Writable> implements Io, Result<T> {

        private final Configuration conf;

        private final String pathPrefix;

        private final Class<T> dataClass;

        private FileSystem fs;

        private ModelOutput<T> output;

        Sink(Configuration conf, String pathPrefix, Class<T> dataClass) {
            this.conf = conf;
            this.pathPrefix = pathPrefix;
            this.dataClass = dataClass;
        }

        void open(String id) throws IOException {
            Invariants.require(output == null);
            Path path = new Path(pathPrefix + id);
            if (fs == null) {
                fs = path.getFileSystem(conf);
            }
            output = create(fs.create(path), dataClass);
        }

        @Override
        public void add(T result) {
            try {
                output.write(result);
            } catch (IOException e) {
                throw new Result.OutputException(e);
            }
        }

        @Override
        public void close() throws IOException {
            if (output != null) {
                output.close();
                output = null;
            }
        }
    }
}
