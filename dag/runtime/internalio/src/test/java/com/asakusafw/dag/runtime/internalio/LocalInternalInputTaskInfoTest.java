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

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.runtime.io.ModelInput;
import com.asakusafw.runtime.io.ModelOutput;
import com.asakusafw.runtime.windows.WindowsSupport;

/**
 * Test for {@link LocalInternalInputTaskInfo}.
 */
public class LocalInternalInputTaskInfoTest {

    /**
     * Support for Windows platform.
     */
    @ClassRule
    public static final WindowsSupport WINDOWS_SUPPORT = new WindowsSupport();

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        File file = temporary.newFile();
        put(file, "Hello, world!");
        List<String> results = new ArrayList<>();
        LocalInternalInputTaskInfo<Text> info = new LocalInternalInputTaskInfo<>(file, 0, 0, Text::new);
        try (ModelInput<Text> in = info.open()) {
            Text buf = info.newDataObject();
            while (in.readTo(buf)) {
                results.add(buf.toString());
            }
        }
        assertThat(results, containsInAnyOrder("Hello, world!"));
    }

    private static void put(File file, String... values) throws IOException {
        try (ModelOutput<Text> out = InternalOutputHandler.create(new FileOutputStream(file), Text.class)) {
            Text buf = new Text();
            for (String value : values) {
                buf.set(value);
                out.write(buf);
            }
        }
    }
}
