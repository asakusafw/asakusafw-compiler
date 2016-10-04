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
package com.asakusafw.vanilla.compiler.tester.externalio;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.asakusafw.lang.compiler.api.reference.CommandTaskReference;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.api.reference.TaskReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.tester.JobflowArtifact;
import com.asakusafw.lang.compiler.tester.executor.JobflowExecutor;
import com.asakusafw.lang.compiler.tester.executor.TaskExecutor;
import com.asakusafw.lang.compiler.tester.executor.TaskExecutors;
import com.asakusafw.lang.compiler.tester.externalio.TestExternalPortProcessor;
import com.asakusafw.lang.compiler.tester.externalio.TestInput;
import com.asakusafw.lang.compiler.tester.externalio.TestOutput;
import com.asakusafw.lang.utils.common.Action;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.stage.temporary.TemporaryFileInput;
import com.asakusafw.runtime.stage.temporary.TemporaryFileOutput;

/**
 * Handles {@link TestInput} and {@link TestOutput}.
 * @since 0.4.0
 */
public class TestIoTaskExecutor implements TaskExecutor {

    private static final int OUTPUT_INIT_BUFFER_SIZE = 300 * 1024;

    private static final int OUTPUT_PAGE_SIZE = 256 * 1024;

    private final Configuration configuration;

    private final Map<String, Action<Object, Exception>> inputs = new LinkedHashMap<>();

    private final Map<String, Action<Object, Exception>> outputs = new LinkedHashMap<>();

    /**
     * Creates a new instance.
     */
    public TestIoTaskExecutor() {
        this(new Configuration());
    }

    /**
     * Creates a new instance.
     * @param configuration the configuration
     */
    public TestIoTaskExecutor(Configuration configuration) {
        Arguments.requireNonNull(configuration);
        this.configuration = configuration;
    }

    /**
     * Adds an input handler.
     * @param <T> the data type
     * @param name the input name
     * @param dataType the input data type
     * @param action the preparing input data action
     * @return this
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T> TestIoTaskExecutor input(String name, Class<T> dataType, Action<ModelOutput<T>, Exception> action) {
        inputs.put(name, (Action) action);
        return this;
    }

    /**
     * Adds an output handler.
     * @param <T> the data type
     * @param name the output name
     * @param dataType the output data type
     * @param action the verifying output data action
     * @return this
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T> TestIoTaskExecutor output(String name, Class<T> dataType, Action<List<T>, Exception> action) {
        outputs.put(name, (Action) action);
        return this;
    }

    @Override
    public boolean isSupported(Context context, TaskReference task) {
        return isSupported(task);
    }

    private boolean isSupported(TaskReference task) {
        return task instanceof CommandTaskReference
                && task.getModuleName().equals(TestExternalPortProcessor.MODULE_NAME);
    }

    /**
     * Checks whether the I/Os are complete.
     * @param context the current context
     * @param artifact the target artifact
     * @throws IOException if not complete
     */
    public void check(JobflowExecutor.Context context, JobflowArtifact artifact) throws IOException {
        JobflowReference jobflow = artifact.getReference();
        Set<String> is = jobflow.getTasks(TestExternalPortProcessor.PHASE_INPUT).stream()
                .filter(this::isSupported)
                .map(CommandTaskReference.class::cast)
                .map(this::getName)
                .collect(Collectors.toSet());
        Set<String> os = jobflow.getTasks(TestExternalPortProcessor.PHASE_OUTPUT).stream()
                .filter(this::isSupported)
                .map(CommandTaskReference.class::cast)
                .map(this::getName)
                .collect(Collectors.toSet());
        if (inputs.keySet().equals(is) == false) {
            throw new IOException(MessageFormat.format(
                    "inputs are not completed: expected={0}, actual={1}",
                    is,
                    inputs.keySet()));
        }
        if (outputs.keySet().equals(os) == false) {
            throw new IOException(MessageFormat.format(
                    "outputs are not completed: expected={0}, actual={1}",
                    os,
                    outputs.keySet()));
        }
    }

    private String getName(CommandTaskReference command) {
        return command.getArguments().get(0).getImage();
    }

    @Override
    public void execute(Context context, TaskReference task) throws InterruptedException, IOException {
        CommandTaskReference command = (CommandTaskReference) task;
        List<String> arguments = TaskExecutors.resolveCommandTokens(context, command.getArguments());
        assert arguments.size() >= 1;

        ClassLoader classLoader = context.getTesterContext().getClassLoader();
        String name = getName(command);
        Class<? extends Writable> dataType = Lang.safe(() -> new ClassDescription(arguments.get(1))
                .resolve(classLoader))
                .asSubclass(Writable.class);

        List<Path> paths = new ArrayList<>();
        for (int i = 2, n = arguments.size(); i < n; i++) {
            paths.add(new Path(TaskExecutors.resolvePath(context, arguments.get(i))));
        }
        switch (command.getProfileName()) {
        case TestExternalPortProcessor.INPUT_PROFILE_NAME:
            executeInput(name, dataType, paths);
            break;
        case TestExternalPortProcessor.OUTPUT_PROFILE_NAME:
            executeOutput(name, dataType, paths);
            break;
        default:
            throw new AssertionError(command);
        }
    }

    private <T extends Writable> void executeInput(
            String name, Class<T> dataType, List<Path> paths) throws IOException {
        Action<Object, Exception> action = inputs.get(name);
        Invariants.requireNonNull(action, () -> MessageFormat.format(
                "missing input: {0}",
                name));
        Path path = new Path(paths.get(0).toString().replace('*', '_'));
        FileSystem fs = path.getFileSystem(configuration);
        try (ModelOutput<T> output = new TemporaryFileOutput<>(
                fs.create(path), dataType.getName(),
                OUTPUT_INIT_BUFFER_SIZE, OUTPUT_PAGE_SIZE)) {
            action.perform(output);
        } catch (Error | RuntimeException | IOException e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private <T extends Writable> void executeOutput(
            String name, Class<T> dataType, List<Path> paths) throws IOException {
        Action<Object, Exception> action = outputs.get(name);
        Invariants.requireNonNull(action, () -> MessageFormat.format(
                "missing output: {0}",
                name));
        List<T> results = new ArrayList<>();
        for (Path pattern : paths) {
            FileSystem fs = pattern.getFileSystem(configuration);
            FileStatus[] stats = fs.globStatus(pattern);
            if (stats == null) {
                continue;
            }
            for (FileStatus stat : stats) {
                try (ModelInput<T> in = new TemporaryFileInput<>(fs.open(stat.getPath()), 0)) {
                    while (true) {
                        T instance = dataType.newInstance();
                        if (in.readTo(instance)) {
                            results.add(instance);
                        } else {
                            break;
                        }
                    }
                } catch (Error | RuntimeException | IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }
        }
        try {
            action.perform(results);
        } catch (Error | RuntimeException | IOException e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public String toString() {
        return "Test I/O"; //$NON-NLS-1$
    }
}
