/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.bridge.stage;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Test for {@link StageInfo}.
 */
public class StageInfoTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        StageInfo info = new StageInfo("u", "a", "b", "c", "d", kvs("e", "f"));
        assertThat(info.toString(), info.getUserName(), is("u"));
        assertThat(info.toString(), info.getBatchId(), is("a"));
        assertThat(info.toString(), info.getFlowId(), is("b"));
        assertThat(info.toString(), info.getStageId(), is("c"));
        assertThat(info.toString(), info.getExecutionId(), is("d"));
        assertThat(info.toString(), info.getBatchArguments(), is(kvs("e", "f")));
    }

    /**
     * w/ serialized batch arguments.
     */
    @Test
    public void serialized_batch_args() {
        StageInfo info = new StageInfo("u", "a", "b", "c", "d", "e=f");
        assertThat(info.toString(), info.getUserName(), is("u"));
        assertThat(info.toString(), info.getBatchId(), is("a"));
        assertThat(info.toString(), info.getFlowId(), is("b"));
        assertThat(info.toString(), info.getStageId(), is("c"));
        assertThat(info.toString(), info.getExecutionId(), is("d"));
        assertThat(info.toString(), info.getBatchArguments(), is(kvs("e", "f")));
    }

    /**
     * w/ nulls.
     */
    @Test
    public void nulls() {
        StageInfo info = new StageInfo(null, null, null, null, null, (String) null);
        assertThat(info.toString(), info.getUserName(), is(nullValue()));
        assertThat(info.toString(), info.getBatchId(), is(nullValue()));
        assertThat(info.toString(), info.getFlowId(), is(nullValue()));
        assertThat(info.toString(), info.getStageId(), is(nullValue()));
        assertThat(info.toString(), info.getExecutionId(), is(nullValue()));
        assertThat(info.toString(), info.getBatchArguments().entrySet(), is(empty()));
    }

    /**
     * check equality.
     */
    @Test
    public void equality() {
        StageInfo info = new StageInfo("u", "a", "b", "c", "d", kvs("e", "f"));
        StageInfo copy = new StageInfo("u", "a", "b", "c", "d", kvs("e", "f"));
        StageInfo diff = new StageInfo("u", "a", "b", "c", "d", kvs("e", "f", "g", "h"));
        assertThat(copy.toString(), copy, is(info));
        assertThat(copy.toString(), copy.hashCode(), is(info.hashCode()));
        assertThat(copy.toString(), copy, is(not(diff)));
    }

    /**
     * resolve variables.
     */
    @Test
    public void resolve() {
        StageInfo info = new StageInfo("u", "a", "b", "c", "d", kvs());
        String resolved = info.resolveVariables("${user}/${batch_id}/${flow_id}/${stage_name}/${execution_id}");
        assertThat(resolved, is("u/a/b/c/d"));
    }

    /**
     * serialize / deserialize.
     */
    @Test
    public void serde() {
        StageInfo info = new StageInfo("u", "a", "b", "c", "d", kvs("e", "f"));
        StageInfo restored = StageInfo.deserialize(info.serialize());
        assertThat(restored, is(info));
    }

    /**
     * serialize / deserialize.
     */
    @Test
    public void serde_escape() {
        StageInfo info = new StageInfo("u", "a", "b", "c", "d", kvs("e", "f|g", "h", "i\\j"));
        StageInfo restored = StageInfo.deserialize(info.serialize());
        assertThat(restored, is(info));
    }

    private static Map<String, String> kvs(String... keyAndValues) {
        assertThat(keyAndValues.length % 2, is(0));
        Map<String, String> results = new LinkedHashMap<>();
        for (int i = 0; i < keyAndValues.length; i += 2) {
            results.put(keyAndValues[i + 0], keyAndValues[i + 1]);
        }
        return results;
    }
}
