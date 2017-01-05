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
package com.asakusafw.lang.compiler.extension.hive;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipFile;

import org.junit.Rule;
import org.junit.Test;

import com.asakusafw.directio.hive.info.InputInfo;
import com.asakusafw.directio.hive.info.LocationInfo;
import com.asakusafw.directio.hive.info.OutputInfo;
import com.asakusafw.directio.hive.info.TableInfo;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.extension.hive.testing.DirectInput;
import com.asakusafw.lang.compiler.extension.hive.testing.DirectOutput;
import com.asakusafw.lang.compiler.extension.hive.testing.HiveSchemaProcessorTester;
import com.asakusafw.lang.compiler.extension.hive.testing.InternalInput;
import com.asakusafw.lang.compiler.extension.hive.testing.InternalOutput;
import com.asakusafw.lang.compiler.extension.hive.testing.MockDataFormat;
import com.asakusafw.lang.compiler.extension.hive.testing.MockDataModel;

/**
 * {@link HiveSchemaAggregatorProcessor}.
 */
public class HiveSchemaAggregatorProcessorTest {

    /**
     * processor tester.
     */
    @Rule
    public final HiveSchemaProcessorTester tester = new HiveSchemaProcessorTester();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        tester.add(
                DirectInput.of("in", "*.bin", MockDataFormat.A.class),
                DirectOutput.of("out", "*.bin", MockDataFormat.A.class))
            .compile();
        check(new InputInfo[] {
                new InputInfo(new LocationInfo("in", "*.bin"), new MockDataFormat.A().getSchema()),
        });
        check(new OutputInfo[] {
                new OutputInfo(new LocationInfo("out", "*.bin"), new MockDataFormat.A().getSchema()),
        });
    }

    /**
     * multiple inputs/outputs.
     * @throws Exception if failed
     */
    @Test
    public void multiple() throws Exception {
        tester
            .add(
                DirectInput.of("a", "*.bin", MockDataFormat.A.class),
                DirectOutput.of("b", "*.bin", MockDataFormat.B.class))
            .add(
                DirectInput.of("c", "*.bin", MockDataFormat.C.class),
                DirectOutput.of("d", "*.bin", MockDataFormat.D.class))
            .compile();
        check(new InputInfo[] {
                new InputInfo(new LocationInfo("a", "*.bin"), new MockDataFormat.A().getSchema()),
                new InputInfo(new LocationInfo("c", "*.bin"), new MockDataFormat.C().getSchema()),
        });
        check(new OutputInfo[] {
                new OutputInfo(new LocationInfo("b", "*.bin"), new MockDataFormat.B().getSchema()),
                new OutputInfo(new LocationInfo("d", "*.bin"), new MockDataFormat.D().getSchema()),
        });
    }

    /**
     * other inputs/outputs.
     * @throws Exception if failed
     */
    @Test
    public void other() throws Exception {
        tester.add(
                InternalInput.of(MockDataModel.class, "in-*"),
                InternalOutput.of(MockDataModel.class, "out-*"))
            .compile();
        try (ZipFile dir = new ZipFile(tester.getJobflow())) {
            check(new InputInfo[0]);
            check(new OutputInfo[0]);
        }
    }

    private void check(InputInfo[] elements) throws IOException {
        check(HiveSchemaAggregatorProcessor.PATH_INPUT, InputInfo.class, Arrays.asList(elements));
    }

    private void check(OutputInfo[] elements) throws IOException {
        check(HiveSchemaAggregatorProcessor.PATH_OUTPUT, OutputInfo.class, Arrays.asList(elements));
    }

    private <T extends TableInfo.Provider> void check(
            Location entry, Class<T> type, List<T> elements) throws IOException {
        File file = tester.get(entry);
        List<T> results;
        try (InputStream input = new FileInputStream(file)) {
            results = Persistent.read(type, input);
        }
        assertThat(results, equalTo(elements));
    }
}
