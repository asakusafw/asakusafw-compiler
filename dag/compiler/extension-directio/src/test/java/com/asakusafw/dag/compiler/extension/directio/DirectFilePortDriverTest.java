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
package com.asakusafw.dag.compiler.extension.directio;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import com.asakusafw.dag.compiler.extension.directio.testing.DirectFileInput;
import com.asakusafw.dag.compiler.extension.directio.testing.DirectFileOutput;
import com.asakusafw.dag.compiler.extension.directio.testing.DirectIoTestHelper;
import com.asakusafw.dag.compiler.extension.directio.testing.MockDataFormat;
import com.asakusafw.dag.runtime.testing.MockDataModel;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.extension.directio.DirectFileIoPortProcessor;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;
import com.asakusafw.lang.compiler.tester.CompilerProfile;
import com.asakusafw.lang.compiler.tester.executor.JobflowExecutor;
import com.asakusafw.lang.compiler.tester.externalio.TestInput;
import com.asakusafw.lang.compiler.tester.externalio.TestOutput;
import com.asakusafw.runtime.directio.DataFilter;
import com.asakusafw.runtime.windows.WindowsSupport;
import com.asakusafw.vanilla.compiler.core.VanillaCompilerTesterRoot;
import com.asakusafw.vanilla.compiler.tester.InProcessVanillaTaskExecutor;
import com.asakusafw.vanilla.compiler.tester.externalio.TestIoTaskExecutor;

/**
 * Test for {@link DirectFilePortDriver}.
 */
public class DirectFilePortDriverTest extends VanillaCompilerTesterRoot {

    static final File WORKING = new File("target/" + DirectFilePortDriverTest.class.getSimpleName());

    static final Location LOCATION_CORE_CONFIGURATION = Location.of("core/conf/asakusa-resources.xml"); //$NON-NLS-1$

    /**
     * Support for Windows platform.
     */
    @ClassRule
    public static final WindowsSupport WINDOWS_SUPPORT = new WindowsSupport();

    /**
     * profile helper.
     */
    @Rule
    public final ExternalResource initializer = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            profile.forToolRepository()
                .useDefaults();
            profile.forCompilerOptions()
                .withRuntimeWorkingDirectory(WORKING.getAbsolutePath(), false);
        }
    };

    final CompilerProfile profile = new CompilerProfile(getClass().getClassLoader());

    /**
     * Direct I/O helper.
     */
    @Rule
    public final DirectIoTestHelper helper = new DirectIoTestHelper();

    final TestIoTaskExecutor testio = new TestIoTaskExecutor();

    final JobflowExecutor executor = new JobflowExecutor(Arrays.asList(new InProcessVanillaTaskExecutor(), testio))
            .withBefore(testio::check)
            .withBefore((a, c) -> ResourceUtil.delete(WORKING))
            .withAfter((a, c) -> ResourceUtil.delete(WORKING));

    /**
     * input.
     * @throws Exception if failed
     */
    @Test
    public void input() throws Exception {
        helper.input("input/a.bin", MockDataFormat.class, o -> {
            o.write(new MockDataModel(0, "Hello, world!"));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "Hello, world!")));
        });
        enableDirectIo();
        run(profile, executor, g -> g
                .input("in", DirectFileInput.of("input", "*.bin", MockDataFormat.class))
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "out"));
    }

    /**
     * input w/ filter.
     * @throws Exception if failed
     */
    @Test
    public void input_filter() throws Exception {
        helper.input("input/a.bin", MockDataFormat.class, o -> {
            o.write(new MockDataModel(0, "Hello0"));
            o.write(new MockDataModel(1, "Hello1"));
            o.write(new MockDataModel(2, "Hello2"));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, contains(
                    new MockDataModel(0, "Hello0"),
                    new MockDataModel(2, "Hello2")));
        });
        enableDirectIo();
        run(profile, executor, g -> g
                .input("in", DirectFileInput.of("input", "*.bin", MockDataFormat.class).withFilter(MockFilter.class))
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "out"));
    }

    /**
     * input w/ filter but the filter is disabled.
     * @throws Exception if failed
     */
    @Test
    public void input_filter_disabled() throws Exception {
        profile.forCompilerOptions()
            .withProperty(DirectFileIoPortProcessor.OPTION_FILTER_ENABLED, "false");
        helper.input("input/a.bin", MockDataFormat.class, o -> {
            o.write(new MockDataModel(0, "Hello0"));
            o.write(new MockDataModel(1, "Hello1"));
            o.write(new MockDataModel(2, "Hello2"));
        });
        testio.output("t", MockDataModel.class, o -> {
            assertThat(o, contains(
                    new MockDataModel(0, "Hello0"),
                    new MockDataModel(1, "Hello1"),
                    new MockDataModel(2, "Hello2")));
        });
        enableDirectIo();
        run(profile, executor, g -> g
                .input("in", DirectFileInput.of("input", "*.bin", MockDataFormat.class).withFilter(MockFilter.class))
                .output("out", TestOutput.of("t", MockDataModel.class))
                .connect("in", "out"));
    }

    /**
     * flat output.
     * @throws Exception if failed
     */
    @Test
    public void output_flat() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello, world!"));
        });
        enableDirectIo();
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .output("out", DirectFileOutput.of("output", "*.bin", MockDataFormat.class))
                .connect("in", "out"));
        helper.output("output", "*.bin", MockDataFormat.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "Hello, world!")));
        });
    }

    /**
     * group output.
     * @throws Exception if failed
     */
    @Test
    public void output_group() throws Exception {
        testio.input("t", MockDataModel.class, o -> {
            o.write(new MockDataModel(0, "Hello0"));
            o.write(new MockDataModel(1, "Hello1a"));
            o.write(new MockDataModel(1, "Hello1b"));
        });
        enableDirectIo();
        run(profile, executor, g -> g
                .input("in", TestInput.of("t", MockDataModel.class))
                .output("out", DirectFileOutput.of("output", "{key}.bin", MockDataFormat.class).withOrder("-value"))
                .connect("in", "out"));
        helper.output("output", "0.bin", MockDataFormat.class, o -> {
            assertThat(o, contains(new MockDataModel(0, "Hello0")));
        });
        helper.output("output", "1.bin", MockDataFormat.class, o -> {
            assertThat(o, contains(new MockDataModel(1, "Hello1b"), new MockDataModel(1, "Hello1a")));
        });
    }

    /**
     * orphaned output.
     * @throws Exception if failed
     */
    @Test
    public void output_orphaned() throws Exception {
        enableDirectIo();
        run(profile, executor, g -> g
                .output("out", DirectFileOutput.of("output", "*.bin", MockDataFormat.class)
                        .withDeletePatterns("*.bin")));
        helper.output("output", "*.bin", MockDataFormat.class, o -> {
            assertThat(o, hasSize(0));
        });
    }

    private void enableDirectIo() {
        Configuration configuration = helper.getContext().newConfiguration();
        profile.forFrameworkInstallation().add(LOCATION_CORE_CONFIGURATION, o -> configuration.writeXml(o));
    }

    @SuppressWarnings("javadoc")
    public static class MockFilter extends DataFilter<MockDataModel> {

        @Override
        public boolean acceptsData(MockDataModel data) {
            return data.getKey() != 1;
        }
    }
}
