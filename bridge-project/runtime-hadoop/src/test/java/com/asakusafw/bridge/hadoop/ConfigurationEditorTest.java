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
package com.asakusafw.bridge.hadoop;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;

/**
 * Test for {@link ConfigurationEditor}.
 */
public class ConfigurationEditorTest {

    /**
     * test for merge.
     */
    @Test
    public void merge() {
        Map<String, String> entries = new LinkedHashMap<>();
        entries.put("a", "A");
        entries.put("b", "B");
        entries.put("c", "C");

        Configuration conf = new Configuration(false);
        ConfigurationEditor.merge(conf, entries);

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            assertThat(conf.get(entry.getKey()), is(entry.getValue()));
        }
    }

    /**
     * test for {@link StageInfo}.
     */
    @Test
    public void stage_info() {
        Map<String, String> args = new LinkedHashMap<>();
        args.put("a", "A");
        args.put("b", "B");
        args.put("c", "C");

        StageInfo info = new StageInfo("u", "b", "f", "s", "e", args);

        Configuration conf = new Configuration(false);
        assertThat(ConfigurationEditor.findStageInfo(conf), is(nullValue()));

        ConfigurationEditor.putStageInfo(conf, info);
        StageInfo restored = ConfigurationEditor.findStageInfo(conf);
        assertThat(restored, is(info));
    }
}
