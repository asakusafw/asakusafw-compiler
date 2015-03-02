package com.asakusafw.lang.compiler.core;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.DataModelProcessor;
import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.core.dummy.DummyBatchProcessor;
import com.asakusafw.lang.compiler.core.dummy.DummyCompilerParticipant;
import com.asakusafw.lang.compiler.core.dummy.DummyDataModelProcessor;
import com.asakusafw.lang.compiler.core.dummy.DummyElement;
import com.asakusafw.lang.compiler.core.dummy.DummyExternalPortProcessor;
import com.asakusafw.lang.compiler.core.dummy.DummyJobflowProcessor;
import com.asakusafw.lang.compiler.core.util.CompositeElement;

/**
 * Test for {@link ToolRepository}.
 */
public class ToolRepositoryTest {

    /**
     * temporary folder for testing.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    /**
     * simple case.
     */
    @Test
    public void simple() {
        ToolRepository tools = ToolRepository.builder(getClass().getClassLoader())
            .use(new DummyDataModelProcessor())
            .use(new DummyExternalPortProcessor())
            .use(new DummyBatchProcessor())
            .use(new DummyJobflowProcessor())
            .use(new DummyCompilerParticipant())
            .build();
        assertThat(tools.getDataModelProcessor(), hasIds("default"));
        assertThat(tools.getBatchProcessor(), hasIds("default"));
        assertThat(tools.getJobflowProcessor(), hasIds("default"));
        assertThat(tools.getExternalPortProcessor(), hasIds("default"));
        assertThat(tools.getParticipant(), hasIds("default"));
    }

    /**
     * multiplex elements.
     */
    @Test
    public void multiplex() {
        ToolRepository tools = ToolRepository.builder(getClass().getClassLoader())
            .use(new DummyDataModelProcessor("a"))
            .use(new DummyDataModelProcessor("b"))
            .use(new DummyExternalPortProcessor("c"))
            .use(new DummyExternalPortProcessor("d"))
            .use(new DummyBatchProcessor("e"))
            .use(new DummyBatchProcessor("f"))
            .use(new DummyJobflowProcessor("g"))
            .use(new DummyJobflowProcessor("h"))
            .use(new DummyCompilerParticipant("i"))
            .use(new DummyCompilerParticipant("j"))
            .build();
        assertThat(tools.getDataModelProcessor(), hasIds("a", "b"));
        assertThat(tools.getExternalPortProcessor(), hasIds("c", "d"));
        assertThat(tools.getBatchProcessor(), hasIds("e", "f"));
        assertThat(tools.getJobflowProcessor(), hasIds("g", "h"));
        assertThat(tools.getParticipant(), hasIds("i", "j"));
    }

    /**
     * optional tools.
     */
    @Test
    public void optional() {
        ToolRepository tools = ToolRepository.builder(getClass().getClassLoader())
            .use(new DummyDataModelProcessor())
            .use(new DummyExternalPortProcessor())
            .build();
        assertThat(tools.getDataModelProcessor(), hasIds("default"));
        assertThat(tools.getExternalPortProcessor(), hasIds("default"));
        assertThat(tools.getBatchProcessor(), hasIds());
        assertThat(tools.getJobflowProcessor(), hasIds());
        assertThat(tools.getParticipant(), hasIds());
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void defaults() throws Exception {
        register(DataModelProcessor.class, DummyDataModelProcessor.class);
        register(ExternalPortProcessor.class, DummyExternalPortProcessor.class);
        register(BatchProcessor.class, DummyBatchProcessor.class);
        register(JobflowProcessor.class, DummyJobflowProcessor.class);
        register(CompilerParticipant.class, DummyCompilerParticipant.class);
        try (URLClassLoader loader = loader()) {
            ToolRepository tools = ToolRepository.builder(loader)
                    .useDefaults(DataModelProcessor.class)
                    .useDefaults(ExternalPortProcessor.class)
                    .useDefaults(BatchProcessor.class)
                    .useDefaults(JobflowProcessor.class)
                    .useDefaults(CompilerParticipant.class)
                    .build();
                assertThat(tools.getDataModelProcessor(), hasIds("default"));
                assertThat(tools.getBatchProcessor(), hasIds("default"));
                assertThat(tools.getJobflowProcessor(), hasIds("default"));
                assertThat(tools.getExternalPortProcessor(), hasIds("default"));
                assertThat(tools.getParticipant(), hasIds("default"));
        }
    }

    /**
     * missing mandatory.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_unknown() {
        ToolRepository.builder(getClass().getClassLoader())
            .use(new DummyDataModelProcessor())
            .use(new DummyExternalPortProcessor())
            .use(new DummyBatchProcessor())
            .use(new DummyJobflowProcessor())
            .use(new DummyCompilerParticipant())
            .useDefaults(Object.class) // unknown
            .build();
    }

    /**
     * missing mandatory.
     */
    @Test(expected = RuntimeException.class)
    public void invalid_missing() {
        ToolRepository.builder(getClass().getClassLoader())
            .use(new DummyExternalPortProcessor())
            .use(new DummyBatchProcessor())
            .use(new DummyJobflowProcessor())
            .use(new DummyCompilerParticipant())
            .build();
    }

    private <T> void register(Class<T> base, Class<? extends T> impl) {
        File services = new File(folder.getRoot(), "META-INF/services");
        services.mkdirs();
        File service = new File(services, base.getName());
        try (OutputStream output = new FileOutputStream(service, true);
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, Charset.forName("UTF-8")))) {
            writer.println(impl.getName());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private URLClassLoader loader() {
        try {
            return URLClassLoader.newInstance(new URL[] { folder.getRoot().toURI().toURL() });
        } catch (MalformedURLException e) {
            throw new AssertionError(e);
        }
    }

    static Matcher<Object> hasIds(String... ids) {
        Matcher<? super List<? extends String>> sub;
        if (ids.length == 0) {
            sub = Matchers.<String>empty();
        } else {
            sub = contains(ids);
        }
        return new FeatureMatcher<Object, List<String>>(sub, "has ids", "hasIds") {
            @Override
            protected List<String> featureValueOf(Object actual) {
                return ids(actual);
            }
        };
    }

    static List<String> ids(Object object) {
        if (object instanceof CompositeElement<?>) {
            List<String> results = new ArrayList<>();
            for (Object element : ((CompositeElement<?>) object).getElements()) {
                results.addAll(ids(element));
            }
            return results;
        } else if (object instanceof DummyElement) {
            return Collections.singletonList(((DummyElement) object).getId());
        } else {
            return Collections.emptyList();
        }
    }
}
