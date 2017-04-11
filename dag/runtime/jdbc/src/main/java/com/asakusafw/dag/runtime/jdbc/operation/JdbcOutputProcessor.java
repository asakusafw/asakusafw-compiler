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
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.dag.api.counter.CounterRepository;
import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.api.processor.TaskProcessor;
import com.asakusafw.dag.api.processor.TaskProcessorContext;
import com.asakusafw.dag.api.processor.TaskSchedule;
import com.asakusafw.dag.api.processor.VertexProcessor;
import com.asakusafw.dag.api.processor.VertexProcessorContext;
import com.asakusafw.dag.api.processor.basic.BasicTaskSchedule;
import com.asakusafw.dag.runtime.io.UnionRecord;
import com.asakusafw.dag.runtime.jdbc.ConnectionPool;
import com.asakusafw.dag.runtime.jdbc.JdbcOperationDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcOutputDriver;
import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.dag.runtime.skeleton.VoidTaskProcessor;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * Processes set of JDBC outputs.
 * @since 0.4.0
 */
public class JdbcOutputProcessor implements VertexProcessor {

    static final Logger LOG = LoggerFactory.getLogger(JdbcOutputProcessor.class);

    static final int DEFAULT_BATCH_INSERT_SIZE = 1024;

    static final int DEFAULT_MAX_CONCURRENCY = -1;

    /**
     * The input edge name.
     */
    public static final String INPUT_NAME = "input"; //$NON-NLS-1$

    private volatile int maxConcurrency = -1;

    private final AtomicReference<String> uniqueProfileName = new AtomicReference<>();

    private final List<Spec<JdbcOperationDriver>> initializeSpecs = new ArrayList<>();

    private final List<Spec<JdbcOutputDriver>> outputSpecs = new ArrayList<>();

    private volatile IoCallable<TaskProcessor> processors;

    private final Closer closer = new Closer();

    /**
     * Adds an initializer.
     * @param id the output ID
     * @param profileName the target profile name
     * @param driver the operation driver
     * @return this
     */
    public JdbcOutputProcessor initialize(String id, String profileName, JdbcOperationDriver driver) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(driver);
        return initialize(id, profileName, (JdbcContext context) -> driver);
    }

    /**
     * Adds an initializer.
     * @param id the output ID
     * @param profileName the target profile name
     * @param provider the operation driver
     * @return this
     */
    public JdbcOutputProcessor initialize(
            String id, String profileName,
            Function<? super JdbcContext, ? extends JdbcOperationDriver> provider) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(provider);
        checkProfileName(profileName);
        initializeSpecs.add(new Spec<>(id, provider));
        return this;
    }

    /**
     * Adds an output.
     * @param id the output ID
     * @param profileName the target profile name
     * @param driver the output driver
     * @return this
     */
    public JdbcOutputProcessor output(String id, String profileName, JdbcOutputDriver driver) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(driver);
        return output(id, profileName, (JdbcContext context) -> driver);
    }

    /**
     * Adds an output.
     * @param id the output ID
     * @param profileName the target profile name
     * @param provider the output driver provider
     * @return this
     */
    public JdbcOutputProcessor output(
            String id, String profileName,
            Function<? super JdbcContext, ? extends JdbcOutputDriver> provider) {
        Arguments.requireNonNull(id);
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(provider);
        checkProfileName(profileName);
        outputSpecs.add(new Spec<>(id, provider));
        return this;
    }

    private void checkProfileName(String profileName) {
        if (uniqueProfileName.compareAndSet(null, profileName) == false) {
            Invariants.require(uniqueProfileName.get().equals(profileName));
        }
    }

    @Override
    public Optional<? extends TaskSchedule> initialize(
            VertexProcessorContext context) throws IOException, InterruptedException {
        Arguments.requireNonNull(context);
        String profileName = uniqueProfileName.get();
        if (profileName == null) {
            // no operations
            assert initializeSpecs.isEmpty();
            assert outputSpecs.isEmpty();
            return getSchedule();
        }

        StageInfo stage = context.getResource(StageInfo.class)
                .orElseThrow(IllegalStateException::new);
        JdbcEnvironment environment = context.getResource(JdbcEnvironment.class)
                .orElseThrow(IllegalStateException::new);
        CounterRepository counters = context.getResource(CounterRepository.class)
                .orElse(CounterRepository.DETACHED);

        JdbcContext jdbc = new JdbcContext.Basic(environment, stage::resolveUserVariables);
        JdbcProfile profile = environment.getProfile(profileName);

        configureProcessor(profile);
        runInitializers(jdbc, profile);
        prepareOutputs(jdbc, profile, counters);
        return getSchedule();
    }

    private Optional<? extends TaskSchedule> getSchedule() {
        if (outputSpecs.isEmpty()) {
            return Optional.of(new BasicTaskSchedule());
        } else {
            return Optional.empty();
        }
    }

    private void configureProcessor(JdbcProfile profile) {
        this.maxConcurrency = computeMaxConcurrency(profile);
        if (LOG.isDebugEnabled()) {
            LOG.debug("JDBC output concurrency: {}", maxConcurrency);
        }
    }

    private static int computeMaxConcurrency(JdbcProfile profile) {
        OptionalInt limit = profile.getConnectionPool().size();
        OptionalInt concurrency = profile.getMaxOutputConcurrency();
        if (limit.isPresent() == false && concurrency.isPresent() == false) {
            return DEFAULT_MAX_CONCURRENCY;
        }
        return Math.max(1, Math.min(limit.orElse(Integer.MAX_VALUE), concurrency.orElse(1)));
    }

    private void runInitializers(JdbcContext context, JdbcProfile profile) throws IOException, InterruptedException {
        try (ConnectionPool.Handle handle = profile.acquire()) {
            for (Spec<JdbcOperationDriver> spec : initializeSpecs) {
                JdbcOperationDriver driver = spec.provider.apply(context);
                driver.perform(handle.getConnection());
            }
            handle.getConnection().commit();
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }

    private void prepareOutputs(
            JdbcContext context, JdbcProfile profile,
            CounterRepository counters) throws IOException, InterruptedException {
        if (outputSpecs.isEmpty()) {
            processors = VoidTaskProcessor::new;
            return;
        }
        List<Tuple<String, JdbcOutputDriver>> resolved = new ArrayList<>();
        for (Spec<JdbcOutputDriver> spec : outputSpecs) {
            resolved.add(new Tuple<String, JdbcOutputDriver>(spec.id, spec.provider.apply(context)));
        }
        processors = () -> {
            try (Closer c = new Closer()) {
                List<CoarseTaskUnit> units = new ArrayList<>();
                for (Tuple<String, JdbcOutputDriver> r : resolved) {
                    String id = r.left();
                    JdbcOutputDriver driver = r.right();
                    JdbcCounterGroup counter = counters.get(JdbcCounterGroup.CATEGORY_OUTPUT, id);
                    units.add(c.add(new CoarseTaskUnit(id, driver, counter)));
                }
                return new CoarseTask(profile, units.toArray(new CoarseTaskUnit[units.size()]), c.move());
            }
        };
        if (maxConcurrency > 0) {
            processors = closer.add(new ConcurrencyLimitter(processors, maxConcurrency));
        }
    }

    @Override
    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    @Override
    public TaskProcessor createTaskProcessor() throws IOException, InterruptedException {
        Invariants.require(processors != null);
        return processors.call();
    }

    @Override
    public void close() throws IOException, InterruptedException {
        closer.close();
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "JdbcOutput({0})", //$NON-NLS-1$
                outputSpecs.size());
    }

    private static final class Spec<T> {

        final String id;

        final Function<? super JdbcContext, ? extends T> provider;

        Spec(String id, Function<? super JdbcContext, ? extends T> provider) {
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

    private static final class CoarseTask implements TaskProcessor {

        private final JdbcProfile profile;

        private final CoarseTaskUnit[] units;

        private final Closer closer;

        private Connection connection;

        private final int windowSize;

        private int windowOffset;

        private boolean sawError;

        CoarseTask(
                JdbcProfile profile,
                CoarseTaskUnit[] units,
                Closer closer) throws IOException, InterruptedException {
            try (Closer c = closer) {
                this.profile = profile;
                this.units = units;
                this.windowSize = profile.getBatchInsertSize().orElse(DEFAULT_BATCH_INSERT_SIZE);
                this.closer = c.move();
            }
        }

        @Override
        public void run(TaskProcessorContext context) throws IOException, InterruptedException {
            if (connection == null) {
                connection = closer.add(profile.acquire()).getConnection();
                windowOffset = 0;
            }
            Connection conn = connection;
            int rest = windowSize - windowOffset;
            CoarseTaskUnit[] us = units;
            try (ObjectReader reader = (ObjectReader) context.getInput(INPUT_NAME)) {
                while (reader.nextObject()) {
                    for (UnionRecord union = (UnionRecord) reader.getObject(); union != null; union = union.next) {
                        us[union.tag].write(conn, union.entity);
                        if (--rest <= 0) {
                            flush();
                            rest = windowSize;
                        }
                    }
                }
            } catch (Throwable t) {
                sawError = true;
                throw t;
            }
            windowOffset = windowSize - rest;
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try {
                if (connection != null && sawError == false && windowOffset > 0) {
                    flush();
                    windowOffset = 0;
                }
            } finally {
                closer.close();
            }
        }

        private void flush() throws IOException, InterruptedException {
            try {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("committing {} records", windowOffset);
                }
                boolean flushed = false;
                CoarseTaskUnit[] us = units;
                for (CoarseTaskUnit u : us) {
                    flushed |= u.flush();
                }
                if (flushed) {
                    assert connection != null;
                    connection.commit();
                }
            } catch (SQLException e) {
                throw JdbcUtil.wrap(e);
            }
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "JdbcOutput({0})", //$NON-NLS-1$
                    units.length);
        }
    }

    private static final class CoarseTaskUnit implements InterruptibleIo {

        private final String id;

        private final JdbcOutputDriver driver;

        private final JdbcCounterGroup counter;

        private JdbcOutputDriver.Sink sink;

        // NOTE: count <= CoarseTask.windowSize (int)
        private int count;

        CoarseTaskUnit(String id, JdbcOutputDriver driver, JdbcCounterGroup counter) {
            Arguments.requireNonNull(id);
            Arguments.requireNonNull(driver);
            Arguments.requireNonNull(counter);
            this.id = id;
            this.driver = driver;
            this.counter = counter;
        }

        void write(Connection connection, Object object) throws IOException, InterruptedException {
            JdbcOutputDriver.Sink s = sink;
            if (s == null) {
                LOG.debug("starting JDBC output: {} ({})", id, driver); //$NON-NLS-1$
                sink = driver.open(connection);
                s = sink;
            }
            s.putObject(object);
            count++;
        }

        boolean flush() throws IOException, InterruptedException {
            if (sink != null) {
                boolean flushed = sink.flush();
                counter.add(count);
                count = 0;
                return flushed;
            }
            return false;
        }

        @Override
        public void close() throws IOException, InterruptedException {
            if (sink != null) {
                sink.close();
                sink = null;
            }
        }
    }
}
