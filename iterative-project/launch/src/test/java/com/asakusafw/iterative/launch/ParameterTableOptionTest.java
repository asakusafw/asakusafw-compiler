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
package com.asakusafw.iterative.launch;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.asakusafw.bridge.launch.LaunchConfigurationException;
import com.asakusafw.iterative.common.IterativeExtensions;
import com.asakusafw.iterative.common.ParameterSet;
import com.asakusafw.iterative.common.ParameterTable;

/**
 * Test for {@link ParameterTableOption}.
 */
public class ParameterTableOptionTest {

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
        ParameterTableOption option = new ParameterTableOption();
        assertThat(option.getCommands(), hasItem(ParameterTableOption.COMMAND));

        File file = dump(IterativeExtensions.builder()
                .next().put("a", "A")
                .build());
        option.accept(ParameterTableOption.COMMAND, "@" + file.getAbsolutePath());

        ParameterTable table = option.resolve();
        assertThat(table, is(notNullValue()));

        List<ParameterSet> rows = table.getRows();
        assertThat(rows, hasSize(1));
        assertThat(rows.get(0).toMap(), is(map("a", "A")));
    }

    /**
     * reentrant.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void invalid_file() throws Exception {
        File file = temporary.newFile();
        ParameterTableOption option = new ParameterTableOption();
        option.accept(ParameterTableOption.COMMAND, "@" + file.getAbsolutePath());
    }

    /**
     * reentrant.
     * @throws Exception if failed
     */
    @Test(expected = LaunchConfigurationException.class)
    public void reentrant() throws Exception {
        File file = dump(IterativeExtensions.builder()
                .next().put("a", "A")
                .build());
        ParameterTableOption option = new ParameterTableOption();
        option.accept(ParameterTableOption.COMMAND, "@" + file.getAbsolutePath());
        option.accept(ParameterTableOption.COMMAND, "@" + file.getAbsolutePath());
    }

    private File dump(ParameterTable table) throws IOException {
        File file = temporary.newFile("testing.json");
        try (OutputStream output = new FileOutputStream(file)) {
            IterativeExtensions.save(output, table);
        }
        return file;
    }

    private Map<String, String> map(String... pairs) {
        assertThat(pairs.length % 2, is(0));
        Map<String, String> results = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            results.put(pairs[i + 0], pairs[i + 1]);
        }
        return results;
    }
}
