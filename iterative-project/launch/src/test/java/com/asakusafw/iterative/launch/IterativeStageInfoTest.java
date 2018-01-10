/**
 * Copyright 2011-2018 Asakusa Framework Team.
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

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.bridge.stage.StageInfo;
import com.asakusafw.iterative.common.IterativeExtensions;
import com.asakusafw.iterative.common.ParameterTable;
import com.asakusafw.iterative.launch.IterativeStageInfo.Cursor;

/**
 * Test for {@link IterativeStageInfo}.
 */
public class IterativeStageInfoTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        StageInfo stage = stage("a", "A");
        ParameterTable table = IterativeExtensions.builder()
            .next().put("b", "B0")
            .next().put("b", "B1")
            .build();
        IterativeStageInfo info = new IterativeStageInfo(stage, table);

        assertThat(info.getOrigin().getBatchArguments(), is(map("a", "A")));
        assertThat(info.getParameterTable().getAvailable(), containsInAnyOrder("b"));

        assertThat(info.isIterative(), is(true));
        assertThat(info.getRoundCount(), is(2));
        assertThat(info.getAvailableParameters(), containsInAnyOrder("a", "b"));
        assertThat(info.getConstantParameters(), containsInAnyOrder("a"));
        assertThat(info.getPartialParameters(), is(empty()));

        int index = 0;
        for (Cursor c = info.newCursor(); c.next();) {
            assertThat(c.getRoundIndex(), is(index));
            if (index == 0) {
                assertThat(c.getDifferences(), containsInAnyOrder("a", "b"));
            } else {
                assertThat(c.getDifferences(), containsInAnyOrder("b"));
            }
            assertThat(c.get().getBatchArguments(), is(map("a", "A", "b", "B" + index)));
            index++;
        }
        assertThat(index, is(2));
    }

    /**
     * parameter table has only one row.
     */
    @Test
    public void singleton_table() {
        StageInfo stage = stage("a", "A");
        ParameterTable table = IterativeExtensions.builder()
                .next().put("b", "B")
                .build();
        IterativeStageInfo info = new IterativeStageInfo(stage, table);

        assertThat(info.isIterative(), is(false));
        assertThat(info.getRoundCount(), is(1));
        assertThat(info.getAvailableParameters(), containsInAnyOrder("a", "b"));
        assertThat(info.getConstantParameters(), is(info.getAvailableParameters()));
        assertThat(info.getPartialParameters(), is(empty()));

        Cursor c = info.newCursor();
        assertThat(c.next(), is(true));
        assertThat(c.getRoundIndex(), is(0));
        assertThat(c.getDifferences(), containsInAnyOrder("a", "b"));
        assertThat(c.get().getBatchArguments(), is(map("a", "A", "b", "B")));

        assertThat(c.next(), is(false));
    }

    /**
     * parameter table has no rows.
     */
    @Test
    public void empty_table() {
        StageInfo stage = stage("a", "A");
        ParameterTable table = IterativeExtensions.builder()
                .build();
        IterativeStageInfo info = new IterativeStageInfo(stage, table);

        assertThat(info.isIterative(), is(false));
        assertThat(info.getRoundCount(), is(1));
        assertThat(info.getAvailableParameters(), containsInAnyOrder("a"));
        assertThat(info.getConstantParameters(), is(info.getAvailableParameters()));
        assertThat(info.getPartialParameters(), is(empty()));

        Cursor c = info.newCursor();
        assertThat(c.next(), is(true));
        assertThat(c.getRoundIndex(), is(0));
        assertThat(c.getDifferences(), containsInAnyOrder("a"));
        assertThat(c.get().getBatchArguments(), is(map("a", "A")));

        assertThat(c.next(), is(false));
    }

    /**
     * overrides the original batch arguments.
     */
    @Test
    public void override_origin() {
        StageInfo stage = stage("a", "A");
        ParameterTable table = IterativeExtensions.builder()
            .next().put("a", "A0")
            .next()
            .next().put("a", "A2")
            .build();
        IterativeStageInfo info = new IterativeStageInfo(stage, table);

        assertThat(info.getOrigin().getBatchArguments(), is(map("a", "A")));
        assertThat(info.getParameterTable().getAvailable(), containsInAnyOrder("a"));

        assertThat(info.isIterative(), is(true));
        assertThat(info.getRoundCount(), is(3));
        assertThat(info.getAvailableParameters(), containsInAnyOrder("a"));
        assertThat(info.getConstantParameters(), is(empty()));
        assertThat(info.getPartialParameters(), is(empty()));

        Cursor c = info.newCursor();
        assertThat(c.next(), is(true));
        assertThat(c.get().getBatchArguments(), is(map("a", "A0")));

        assertThat(c.next(), is(true));
        assertThat(c.get().getBatchArguments(), is(map("a", "A")));

        assertThat(c.next(), is(true));
        assertThat(c.get().getBatchArguments(), is(map("a", "A2")));

        assertThat(c.next(), is(false));
    }

    private StageInfo stage(String... pairs) {
        return new StageInfo(
                "u",
                "b", "f", null, "e",
                map(pairs));
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
