/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.cli;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.cli.ParseException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.lang.compiler.cli.BatchCompilerCli.Configuration;
import com.asakusafw.lang.compiler.cli.mock.DelegateBatchCompiler;
import com.asakusafw.lang.compiler.cli.mock.DummyBatch;
import com.asakusafw.lang.compiler.cli.mock.DummyBatchCompiler;
import com.asakusafw.lang.compiler.cli.mock.DummyBatchProcessor;
import com.asakusafw.lang.compiler.cli.mock.DummyClassAnalyzer;
import com.asakusafw.lang.compiler.cli.mock.DummyCompilerParticipant;
import com.asakusafw.lang.compiler.cli.mock.DummyDataModelProcessor;
import com.asakusafw.lang.compiler.cli.mock.DummyExternalPortProcessor;
import com.asakusafw.lang.compiler.cli.mock.DummyJobflowProcessor;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.Predicate;
import com.asakusafw.lang.compiler.common.Predicates;
import com.asakusafw.lang.compiler.common.testing.FileDeployer;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.util.CompositeElement;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.packaging.ResourceRepository;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;

/**
 * Test for {@link BatchCompilerCli}.
 */
public class BatchCompilerCliTest {

    /**
     * deployer.
     */
    @Rule
    public FileDeployer deployer = new FileDeployer();

    /**
     * empty args.
     * @throws Exception if failed
     */
    @Test(expected = ParseException.class)
    public void parse_empty() throws Exception {
        BatchCompilerCli.parse();
    }

    /**
     * minimal args.
     * @throws Exception if failed
     */
    @Test
    public void parse_minimal() throws Exception {
        File input = deployer.newFolder();
        File output = deployer.newFolder();
        Configuration conf = BatchCompilerCli.parse(strings(new Object[] {
                "--explore", input,
                "--output", output,
        }));
        assertThat(conf.classAnalyzer, containsInAnyOrder(BatchCompilerCli.DEFAULT_CLASS_ANALYZER));
        assertThat(conf.batchCompiler, containsInAnyOrder(BatchCompilerCli.DEFAULT_BATCH_COMPILER));
        assertThat(conf.output, contains(output));
        assertThat(conf.explore, contains(input));
        assertThat(conf.external, isEmpty());
        assertThat(conf.embed, isEmpty());
        assertThat(conf.attach, isEmpty());
        assertThat(conf.sourcePredicate, isEmpty());
        assertThat(conf.runtimeWorkingDirectory, contains(BatchCompilerCli.DEFAULT_RUNTIME_WORKING_DIRECTORY));
        assertThat(conf.properties.entrySet(), is(empty()));
        assertThat(conf.failOnError, contains(false));
        assertThat(conf.batchIdPrefix, isEmpty());
    }

    /**
     * full args.
     * @throws Exception if failed
     */
    @Test
    public void parse_full() throws Exception {
        File input = deployer.newFolder();
        File output = deployer.newFolder();
        File external1 = deployer.newFolder();
        File external2 = deployer.newFolder();
        File embed1 = deployer.newFolder();
        File embed2 = deployer.newFolder();
        File attach1 = deployer.newFolder();
        File attach2 = deployer.newFolder();
        Configuration conf = BatchCompilerCli.parse(strings(new Object[] {
                "--explore", input,
                "--output", output,
                "--classAnalyzer", classes(DummyClassAnalyzer.class),
                "--batchCompiler", classes(DummyBatchCompiler.class),
                "--external", files(external1, external2),
                "--embed", files(embed1, embed2),
                "--attach", files(attach1, attach2),
                "--include", "*Buffer",
                "--exclude", "java.lang.*",
                "--dataModelProcessors", classes(DummyDataModelProcessor.class),
                "--externalPortProcessors", classes(DummyExternalPortProcessor.class),
                "--batchProcessors", classes(DummyBatchProcessor.class),
                "--jobflowProcessors", classes(DummyJobflowProcessor.class),
                "--participants", classes(DummyCompilerParticipant.class),
                "--runtimeWorkingDirectory", "testRuntimeWorkingDirectory",
                "--failOnError", true,
                "--batchIdPrefix", "prefix.",
                "-P", "a=b",
                "-property", "c=d",
        }));
        assertThat(conf.classAnalyzer, containsInAnyOrder(classOf(DummyClassAnalyzer.class)));
        assertThat(conf.batchCompiler, containsInAnyOrder(classOf(DummyBatchCompiler.class)));
        assertThat(conf.output, contains(output));
        assertThat(conf.explore, contains(input));
        assertThat(conf.external, containsInAnyOrder(external1, external2));
        assertThat(conf.embed, containsInAnyOrder(embed1, embed2));
        assertThat(conf.attach, containsInAnyOrder(attach1, attach2));
        assertThat(conf.dataModelProcessors, containsInAnyOrder(classOf(DummyDataModelProcessor.class)));
        assertThat(conf.externalPortProcessors, containsInAnyOrder(classOf(DummyExternalPortProcessor.class)));
        assertThat(conf.batchProcessors, containsInAnyOrder(classOf(DummyBatchProcessor.class)));
        assertThat(conf.jobflowProcessors, containsInAnyOrder(classOf(DummyJobflowProcessor.class)));
        assertThat(conf.compilerParticipants, containsInAnyOrder(classOf(DummyCompilerParticipant.class)));
        assertThat(conf.sourcePredicate, not(isEmpty()));
        assertThat(conf.runtimeWorkingDirectory, contains("testRuntimeWorkingDirectory"));
        assertThat(conf.properties.entrySet(), hasSize(2));
        assertThat(conf.failOnError, contains(true));
        assertThat(conf.batchIdPrefix, contains("prefix."));

        Predicate<? super Class<?>> p = predicate(conf.sourcePredicate);
        assertThat(p.apply(ByteBuffer.class), is(true));
        assertThat(p.apply(ByteChannel.class), is(false));
        assertThat(p.apply(StringBuffer.class), is(false));

        assertThat(conf.properties, hasEntry("a", "b"));
        assertThat(conf.properties, hasEntry("c", "d"));
    }

    /**
     * {@code --include} multiple patterns.
     * @throws Exception if failed
     */
    @Test
    public void parse_include() throws Exception {
        Configuration conf = BatchCompilerCli.parse(strings(new Object[] {
                "--explore", deployer.newFolder(),
                "--output", deployer.newFolder(),
                "--include", "*.String*,*Buffer",
        }));
        Predicate<? super Class<?>> p = predicate(conf.sourcePredicate);
        assertThat(p.apply(String.class), is(true));
        assertThat(p.apply(StringBuilder.class), is(true));
        assertThat(p.apply(ByteBuffer.class), is(true));
        assertThat(p.apply(Integer.class), is(false));
        assertThat(p.apply(ByteChannel.class), is(false));
    }

    /**
     * {@code --exclude} multiple patterns.
     * @throws Exception if failed
     */
    @Test
    public void parse_exclude() throws Exception {
        Configuration conf = BatchCompilerCli.parse(strings(new Object[] {
                "--explore", deployer.newFolder(),
                "--output", deployer.newFolder(),
                "--exclude", "*.String*,*Buffer",
        }));
        Predicate<? super Class<?>> p = predicate(conf.sourcePredicate);
        assertThat(p.apply(String.class), is(false));
        assertThat(p.apply(StringBuilder.class), is(false));
        assertThat(p.apply(ByteBuffer.class), is(false));
        assertThat(p.apply(Integer.class), is(true));
        assertThat(p.apply(ByteChannel.class), is(true));
    }

    /**
     * empty args.
     * @throws Exception if failed
     */
    @Test
    public void execute_empty() throws Exception {
        int status = BatchCompilerCli.execute();
        assertThat(status, is(not(0)));
    }

    /**
     * minimal args.
     * @throws Exception if failed
     */
    @Test
    public void execute_minimal() throws Exception {
        final File output = deployer.newFolder();
        String[] args = strings(new Object[] {
                "--explore", files(ResourceUtil.findLibraryByClass(DummyBatch.class)),
                "--output", output,
                "--classAnalyzer", classes(DummyClassAnalyzer.class),
                "--batchCompiler", classes(DelegateBatchCompiler.class),
                "--include", classes(DummyBatch.class),
                "--externalPortProcessors", classes(DummyExternalPortProcessor.class),
        });
        final AtomicInteger count = new AtomicInteger();
        int status = execute(args, new BatchCompiler() {
            @Override
            public void compile(Context context, Batch batch) {
                count.incrementAndGet();
                assertThat(batch.getBatchId(), is("DummyBatch"));
                assertThat(batch.getDescriptionClass(), is(classOf(DummyBatch.class)));
                assertThat(context.getOutput().getBasePath(), is(new File(output, batch.getBatchId())));
            }
        });
        assertThat(status, is(0));
        assertThat(count.get(), is(1));
    }

    /**
     * full args.
     * @throws Exception if failed
     */
    @Test
    public void execute_full() throws Exception {
        final File output = deployer.newFolder();
        final File explore = prepareLibrary("explore");
        final File external = prepareLibrary("external");
        final File embed = prepareLibrary("embed");
        final File attach = prepareLibrary("attach");
        String[] args = strings(new Object[] {
                "--explore", files(ResourceUtil.findLibraryByClass(DummyBatch.class), explore),
                "--output", output,
                "--classAnalyzer", classes(DummyClassAnalyzer.class),
                "--batchCompiler", classes(DelegateBatchCompiler.class),
                "--external", files(external),
                "--embed", files(embed),
                "--attach", files(attach),
                "--include", classes(DummyBatch.class),
                "--dataModelProcessors", classes(DummyDataModelProcessor.class),
                "--externalPortProcessors", classes(DummyExternalPortProcessor.class),
                "--batchProcessors", classes(DummyBatchProcessor.class),
                "--jobflowProcessors", classes(DummyJobflowProcessor.class),
                "--participants", classes(DummyCompilerParticipant.class),
                "--runtimeWorkingDirectory", "testRuntimeWorkingDirectory",
                "--failOnError", true,
                "--batchIdPrefix", "prefix.",
                "-P", "a=b",
                "-property", "c=d",
        });
        final AtomicInteger count = new AtomicInteger();
        int status = execute(args, new BatchCompiler() {
            @Override
            public void compile(Context context, Batch batch) {
                count.incrementAndGet();
                assertThat(batch.getBatchId(), is("prefix.DummyBatch"));
                assertThat(batch.getDescriptionClass(), is(classOf(DummyBatch.class)));
                assertThat(context.getOutput().getBasePath(), is(new File(output, batch.getBatchId())));

                assertThat(context.getProject().getClassLoader().getResource("explore"), is(notNullValue()));
                assertThat(context.getProject().getClassLoader().getResource("embed"), is(notNullValue()));
                assertThat(context.getProject().getClassLoader().getResource("attach"), is(notNullValue()));
                assertThat(context.getProject().getClassLoader().getResource("external"), is(notNullValue()));

                assertThat(context.getProject().getProjectContents(), includes("explore"));
                assertThat(context.getProject().getProjectContents(), not(includes("embed")));
                assertThat(context.getProject().getProjectContents(), not(includes("attach")));
                assertThat(context.getProject().getProjectContents(), not(includes("external")));

                // --explore -> implicitly embedded
                assertThat(context.getProject().getEmbeddedContents(), includes("explore"));
                assertThat(context.getProject().getEmbeddedContents(), includes("embed"));
                assertThat(context.getProject().getEmbeddedContents(), not(includes("attach")));
                assertThat(context.getProject().getEmbeddedContents(), not(includes("external")));

                assertThat(context.getProject().getAttachedLibraries(), not(deepIncludes("explore")));
                assertThat(context.getProject().getAttachedLibraries(), not(deepIncludes("embed")));
                assertThat(context.getProject().getAttachedLibraries(), deepIncludes("attach"));
                assertThat(context.getProject().getAttachedLibraries(), not(deepIncludes("external")));

                assertThat(context.getTools().getDataModelProcessor(), is(consistsOf(DummyDataModelProcessor.class)));
                assertThat(context.getTools().getExternalPortProcessor(), is(consistsOf(DummyExternalPortProcessor.class)));
                assertThat(context.getTools().getBatchProcessor(), is(consistsOf(DummyBatchProcessor.class)));
                assertThat(context.getTools().getJobflowProcessor(), is(consistsOf(DummyJobflowProcessor.class)));
                assertThat(context.getTools().getParticipant(), is(consistsOf(DummyCompilerParticipant.class)));
            }
        });
        assertThat(status, is(0));
        assertThat(count.get(), is(1));
    }

    /**
     * execute w/ conflict batch ID.
     * @throws Exception if failed
     */
    @Test
    public void execute_conflict_batch() throws Exception {
        final File output = deployer.newFolder();
        String[] args = strings(new Object[] {
                "--explore", files(ResourceUtil.findLibraryByClass(DummyBatch.class)),
                "--output", output,
                "--classAnalyzer", classes(DummyClassAnalyzer.class),
                "--batchCompiler", classes(DelegateBatchCompiler.class),
                "--include", DummyBatch.class.getName() + "*",
                "--externalPortProcessors", classes(DummyExternalPortProcessor.class),
                "--failOnError",
        });
        int status = execute(args, new BatchCompiler() {
            @Override
            public void compile(Context context, Batch batch) {
                // do nothing
            }
        });
        assertThat(status, is(not(0)));
    }

    private File prepareLibrary(String id) {
        File base = deployer.newFolder();
        try {
            assertThat(new File(base, id).createNewFile(), is(true));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return base;
    }

    static Matcher<List<ResourceRepository>> includes(String id) {
        final Location location = Location.of(id);
        return new BaseMatcher<List<ResourceRepository>>() {
            @Override
            public boolean matches(Object item) {
                List<?> elements = (List<?>) item;
                for (Object element : elements) {
                    ResourceRepository repo = (ResourceRepository) element;
                    try (ResourceRepository.Cursor cursor = repo.createCursor()) {
                        while (cursor.next()) {
                            if (cursor.getLocation().equals(location)) {
                                return true;
                            }
                        }
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
                return false;
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("includes ").appendValue(location);
            }
        };
    }

    static Matcher<List<ResourceRepository>> deepIncludes(final String id) {
        return new BaseMatcher<List<ResourceRepository>>() {
            @Override
            public boolean matches(Object item) {
                List<?> elements = (List<?>) item;
                for (Object element : elements) {
                    ResourceRepository repo = (ResourceRepository) element;
                    try (ResourceRepository.Cursor cursor = repo.createCursor()) {
                        while (cursor.next()) {
                            try (ZipInputStream input = new ZipInputStream(cursor.openResource())) {
                                while (true) {
                                    ZipEntry entry = input.getNextEntry();
                                    if (entry == null) {
                                        break;
                                    }
                                    if (entry.getName().equals(id)) {
                                        return true;
                                    }
                                }
                            }
                        }
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
                return false;
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("deep includes ").appendValue(id);
            }
        };
    }

    static Matcher<Object> consistsOf(final Class<?> type) {
        return new BaseMatcher<Object>() {
            @Override
            public boolean matches(Object item) {
                return consistsOf(item, type);
            }
            @Override
            public void describeTo(Description description) {
                description.appendText("consists of ").appendText(type.getName());
            }
        };
    }

    static boolean consistsOf(Object item, Class<?> type) {
        if (type.isInstance(item)) {
            return true;
        } else if (item instanceof CompositeElement<?>) {
            for (Object element : ((CompositeElement<?>) item).getElements()) {
                if (consistsOf(element, type)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * w/ compile error.
     * @throws Exception if failed
     */
    @Test
    public void compile_error() throws Exception {
        final File output = deployer.newFolder();
        String[] args = strings(new Object[] {
                "--explore", files(ResourceUtil.findLibraryByClass(DummyBatch.class)),
                "--output", output,
                "--classAnalyzer", classes(DummyClassAnalyzer.class),
                "--batchCompiler", classes(DelegateBatchCompiler.class),
                "--include", classes(DummyBatch.class),
                "--externalPortProcessors", classes(DummyExternalPortProcessor.class),
        });
        int status = execute(args, new BatchCompiler() {
            @Override
            public void compile(Context context, Batch batch) {
                throw new DiagnosticException(Diagnostic.Level.ERROR, "testing");
            }
        });
        assertThat(status, is(not(0)));
    }

    private int execute(String[] args, BatchCompiler delegate) {
        DelegateBatchCompiler.DELEGATE.set(delegate);
        try {
            return BatchCompilerCli.execute(args);
        } finally {
            DelegateBatchCompiler.DELEGATE.remove();
        }
    }

    private String classes(Class<?>... classes) {
        StringBuilder buf = new StringBuilder();
        for (Class<?> aClass : classes) {
            if (buf.length() > 0) {
                buf.append(BatchCompilerCli.CLASS_SEPARATOR);
            }
            buf.append(aClass.getName());
        }
        return buf.toString();
    }

    private Predicate<? super Class<?>> predicate(Iterable<? extends Predicate<? super Class<?>>> elements) {
        Predicate<? super Class<?>> current = Predicates.anything();
        for (Predicate<? super Class<?>> p : elements) {
            current = Predicates.and(current, p);
        }
        return current;
    }

    private String files(File... files) {
        StringBuilder buf = new StringBuilder();
        for (File file : files) {
            if (buf.length() > 0) {
                buf.append(File.pathSeparatorChar);
            }
            buf.append(file.getPath());
        }
        return buf.toString();
    }

    private Matcher<? super Holder<?>> isEmpty() {
        return new FeatureMatcher<Holder<?>, Boolean>(is(true), "is empty", "isEmpty") {
            @Override
            protected Boolean featureValueOf(Holder<?> actual) {
                return actual.isEmpty();
            }
        };
    }

    private String[] strings(Object... values) {
        String[] results = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            results[i] = String.valueOf(values[i]);
        }
        return results;
    }
}
