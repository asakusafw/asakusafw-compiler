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
package com.asakusafw.dag.runtime.internalio;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.common.ObjectFactory;
import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicTaskSchedule;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.dag.runtime.adapter.ExtractOperation.Input;
import com.asakusafw.dag.runtime.adapter.InputAdapter;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.io.HadoopObjectFactory;
import com.asakusafw.dag.runtime.skeleton.ModelInputHandler;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.runtime.stage.temporary.TemporaryFile;
import com.asakusafw.runtime.stage.temporary.TemporaryStorage;

/**
 * {@link InputAdapter} for internal inputs.
 * @since 0.4.0
 * @see TemporaryFile
 */
public class InternalInputAdapter implements InputAdapter<ExtractOperation.Input> {

    private final List<Callable<List<TaskInfo>>> tasks = new ArrayList<>();

    private final StageInfo stage;

    private final Configuration configuration;

    private final ObjectFactory objectFactory;

    /**
     * Creates a new instance.
     * @param context the current context
     */
    public InternalInputAdapter(VertexProcessorContext context) {
        Arguments.requireNonNull(context);
        this.stage = context.getResource(StageInfo.class)
                .orElseThrow(IllegalStateException::new);
        this.configuration = context.getResource(Configuration.class)
                .orElseThrow(IllegalStateException::new);
        this.objectFactory = new HadoopObjectFactory(configuration);
    }

    /**
     * Creates a new instance.
     * @param stage the current stage info
     * @param configuration the Hadoop configuration
     */
    public InternalInputAdapter(StageInfo stage, Configuration configuration) {
        Arguments.requireNonNull(stage);
        Arguments.requireNonNull(configuration);
        this.stage = stage;
        this.configuration = configuration;
        this.objectFactory = new HadoopObjectFactory(configuration);
    }

    /**
     * Adds an input pattern.
     * @param id the input ID
     * @param path the target path expression
     * @param dataClass the data class
     * @return this
     */
    public final InternalInputAdapter bind(String id, String path, Class<? extends Writable> dataClass) {
        return bind(id, new String[] { path }, dataClass);
    }

    /**
     * Adds an input pattern.
     * @param id the input ID
     * @param paths the target path expressions
     * @param dataClass the data class
     * @return this
     */
    public final InternalInputAdapter bind(String id, String[] paths, Class<? extends Writable> dataClass) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(paths);
        Arguments.requireNonNull(dataClass);
        List<Path> resolved = Stream.of(paths)
                .map(stage::resolveUserVariables)
                .map(Path::new)
                .collect(Collectors.toList());
        tasks.add(() -> {
            List<TaskInfo> results = new ArrayList<>();
            resolve(resolved, dataClass, results::add);
            return results;
        });
        return this;
    }

    private <T extends Writable> void resolve(
            List<Path> paths, Class<T> type, Consumer<TaskInfo> sink) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        Supplier<? extends T> supplier = () -> objectFactory.newInstance(type);
        List<FileStatus> stats = new ArrayList<>();
        for (Path path : paths) {
            List<FileStatus> s = TemporaryStorage.listStatus(configuration, path);
            stats.addAll(s);
        }
        for (FileStatus stat : stats) {
            Path p = stat.getPath();
            File local = null;
            URI uri = p.toUri();
            String scheme = uri.getScheme();
            if (scheme != null && scheme.equals("file")) { //$NON-NLS-1$
                local = new File(uri);
            }
            long length = stat.getLen();
            if (length == 0) {
                continue;
            }
            int blocks = (int) ((length + TemporaryFile.BLOCK_SIZE - 1) / TemporaryFile.BLOCK_SIZE);
            for (int i = 0; i < blocks; i++) {
                if (local == null) {
                    sink.accept(new HadoopInternalInputTaskInfo<>(fs, p, i, 1, supplier));
                } else {
                    sink.accept(new LocalInternalInputTaskInfo<>(local, i, 1, supplier));
                }
            }
        }
    }

    @Override
    public TaskSchedule getSchedule() throws IOException, InterruptedException {
        try {
            List<TaskInfo> results = new ArrayList<>();
            for (Callable<List<TaskInfo>> closure : tasks) {
                results.addAll(closure.call());
            }
            return new BasicTaskSchedule(results);
        } catch (IOException | InterruptedException | RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public InputHandler<Input, ? super TaskProcessorContext> newHandler() throws IOException, InterruptedException {
        return new ModelInputHandler();
    }
}
