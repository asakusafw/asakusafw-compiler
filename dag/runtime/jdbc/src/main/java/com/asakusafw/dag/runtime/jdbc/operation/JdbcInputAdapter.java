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
package com.asakusafw.dag.runtime.jdbc.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.counter.CounterRepository;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.TaskInfo;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicTaskSchedule;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.dag.runtime.adapter.ExtractOperation.Input;
import com.asakusafw.dag.runtime.adapter.InputAdapter;
import com.asakusafw.dag.runtime.adapter.InputHandler;
import com.asakusafw.dag.runtime.adapter.InputHandler.InputSession;
import com.asakusafw.dag.runtime.jdbc.ConnectionPool;
import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcInputDriver.Partition;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * {@link InputAdapter} for JDBC inputs.
 * @since 0.4.0
 * @version 0.4.2
 */
public class JdbcInputAdapter implements InputAdapter<ExtractOperation.Input> {

    static final Logger LOG = LoggerFactory.getLogger(JdbcInputAdapter.class);

    private final JdbcContext jdbc;

    private final List<Spec> specs = new ArrayList<>();

    private final AtomicReference<String> uniqueProfileName = new AtomicReference<>();

    private final CounterRepository counters;

    private final boolean maxConcurrencyEnabled;

    /**
     * Creates a new instance.
     * @param context the current context
     */
    public JdbcInputAdapter(VertexProcessorContext context) {
        Arguments.requireNonNull(context);
        StageInfo stage = context.getResource(StageInfo.class)
                .orElseThrow(IllegalStateException::new);
        JdbcEnvironment environment = context.getResource(JdbcEnvironment.class)
                .orElseThrow(IllegalStateException::new);
        this.jdbc = new JdbcContext.Basic(environment, stage::resolveUserVariables);
        this.counters = context.getResource(CounterRepository.class)
                .orElse(CounterRepository.DETACHED);
        // TODO remove workaround
        this.maxConcurrencyEnabled = Util.isMaxConcurrencyEnabled(context);
    }

    /**
     * Adds an input.
     * @param id the input ID
     * @param profileName the target profile name
     * @param driver the input driver
     * @return this
     */
    public final JdbcInputAdapter input(String id, String profileName, JdbcInputDriver driver) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(driver);
        return input(id, profileName, (JdbcContext context) -> driver);
    }

    /**
     * Adds an input.
     * @param id the input ID
     * @param profileName the target profile name
     * @param provider the input driver provider
     * @return this
     */
    public final JdbcInputAdapter input(
            String id, String profileName,
            Function<? super JdbcContext, ? extends JdbcInputDriver> provider) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(provider);
        checkProfileName(profileName);
        specs.add(new Spec(id, provider));
        return this;
    }

    private void checkProfileName(String profileName) {
        if (uniqueProfileName.compareAndSet(null, profileName) == false) {
            Invariants.require(uniqueProfileName.get().equals(profileName));
        }
    }

    @Override
    public TaskSchedule getSchedule() throws IOException, InterruptedException {
        String profileName = uniqueProfileName.get();
        if (profileName == null) {
            // no operations
            assert specs.isEmpty();
            return new BasicTaskSchedule();
        }
        JdbcProfile profile = jdbc.getEnvironment().getProfile(profileName);
        List<Task> tasks = collectTasks(profile);
        BasicTaskSchedule result = new BasicTaskSchedule(tasks);
        OptionalInt maxConcurrency = profile.getMaxInputConcurrency();
        if (maxConcurrency.isPresent()) {
            result = result.withMaxConcurrency(maxConcurrency.getAsInt());
        }
        return result;
    }

    private List<Task> collectTasks(JdbcProfile profile) throws IOException, InterruptedException {
        List<SubTask> subs = new ArrayList<>();
        try (ConnectionPool.Handle handle = profile.acquire()) {
            for (Spec spec : specs) {
                JdbcCounterGroup counter = counters.get(JdbcCounterGroup.CATEGORY_INPUT, spec.id);
                JdbcInputDriver driver = spec.provider.apply(jdbc);
                List<? extends Partition> partitions = driver.getPartitions(handle.getConnection());
                for (JdbcInputDriver.Partition partition : partitions) {
                    subs.add(new SubTask(spec, partition, counter));
                }
            }
        }
        // unknown size -> larger tasks first
        subs.sort((a, b) -> {
            OptionalDouble aRow = a.partition.getEsitimatedRowCount();
            OptionalDouble bRow = b.partition.getEsitimatedRowCount();
            if (aRow.isPresent() == false) {
                return bRow.isPresent() ? -1 : 0;
            } else if (bRow.isPresent() == false) {
                return +1;
            } else {
                return Double.compare(bRow.getAsDouble(), aRow.getAsDouble());
            }
        });
        OptionalInt maxConcurrency = profile.getMaxInputConcurrency();
        if (maxConcurrencyEnabled || maxConcurrency.isPresent() == false || subs.size() <= maxConcurrency.getAsInt()) {
            return subs.stream()
                    .map(it -> new LinkedList<>(Collections.singletonList(it)))
                    .map(it -> new Task(profile, it))
                    .collect(Collectors.toList());
        } else {
            // share partitions
            Queue<SubTask> queue = new ConcurrentLinkedQueue<>(subs);
            return IntStream.range(0, maxConcurrency.getAsInt())
                    .mapToObj(i -> new Task(profile, queue))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public InputHandler<Input, TaskProcessorContext> newHandler() throws IOException, InterruptedException {
        return context -> context.getTaskInfo()
                .map(Task.class::cast)
                .orElseThrow(IllegalStateException::new)
                .newDriver();
    }

    @Override
    public String toString() {
        return String.format("JdbcInput(%s)", specs); //$NON-NLS-1$
    }

    private static class Spec {

        final String id;

        final Function<? super JdbcContext, ? extends JdbcInputDriver> provider;

        Spec(String id, Function<? super JdbcContext, ? extends JdbcInputDriver> provider) {
            assert id != null;
            assert provider != null;
            this.id = id;
            this.provider = provider;
        }

        @Override
        public String toString() {
            return id;
        }
    }

    private static class Task implements TaskInfo {

        private final JdbcProfile profile;

        private final Queue<SubTask> tasks;

        Task(JdbcProfile profile, Queue<SubTask> tasks) {
            this.profile = profile;
            this.tasks = tasks;
        }

        Driver newDriver() throws IOException, InterruptedException {
            return new Driver(profile.acquire(), tasks);
        }
    }

    private static final class SubTask {

        private final Spec spec;

        final JdbcInputDriver.Partition partition;

        final JdbcCounterGroup counter;

        SubTask(Spec spec, Partition partition, JdbcCounterGroup counter) {
            this.spec = spec;
            this.partition = partition;
            this.counter = counter;
        }

        @Override
        public String toString() {
            return String.format("%s:%s", spec, partition);
        }
    }

    private static final class Driver
            implements InputSession<ExtractOperation.Input>, ExtractOperation.Input {

        private final ConnectionPool.Handle handle;

        private final Queue<SubTask> rest;

        private ObjectReader reader;

        private JdbcCounterGroup counter;

        private long count;

        Driver(ConnectionPool.Handle handle, Queue<SubTask> tasks) {
            this.handle = handle;
            this.rest = tasks;
        }

        @Override
        public ExtractOperation.Input get() throws IOException, InterruptedException {
            return this;
        }

        @Override
        public boolean next() throws IOException, InterruptedException {
            while (true) {
                if (reader == null) {
                    if (rest.isEmpty()) {
                        return false;
                    } else {
                        SubTask task = rest.poll();
                        reader = task.partition.open(handle.getConnection());
                        counter = task.counter;
                    }
                }
                if (reader.nextObject()) {
                    count++;
                    return true;
                } else {
                    closeCurrent();
                }
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public <S> S getObject() throws IOException, InterruptedException {
            return (S) reader.getObject();
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try {
                closeCurrent();
            } finally {
                handle.close();
            }
        }

        private void closeCurrent() throws IOException, InterruptedException {
            if (reader == null) {
                return;
            }
            reader.close();
            if (count != 0) {
                counter.add(count);
                count = 0;
            }
            reader = null;
            counter = null;
        }
    }
}
