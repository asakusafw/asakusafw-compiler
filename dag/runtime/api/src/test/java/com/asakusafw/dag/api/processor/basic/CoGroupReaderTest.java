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
package com.asakusafw.dag.api.processor.basic;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import com.asakusafw.dag.api.common.ObjectCursor;
import com.asakusafw.dag.api.processor.testing.CollectionGroupReader;
import com.asakusafw.lang.utils.common.Lang;

/**
 * Test for {@link CoGroupReader}.
 */
public class CoGroupReaderTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        SortedMap<Integer, List<String>> as = new TreeMap<>();
        as.put(1, Arrays.asList("a"));

        SortedMap<Integer, List<String>> bs = new TreeMap<>();
        bs.put(1, Arrays.asList("b"));

        try (CoGroupReader reader = reader(as, bs)) {
            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { "a" },
                { "b" },
            }));

            assertThat(reader.nextCoGroup(), is(false));
        }
    }

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void multi_group() throws Exception {
        SortedMap<Integer, List<String>> as = new TreeMap<>();
        as.put(1, Arrays.asList("a1"));
        as.put(2, Arrays.asList("a2"));
        as.put(3, Arrays.asList("a3"));

        SortedMap<Integer, List<String>> bs = new TreeMap<>();
        bs.put(1, Arrays.asList("b1"));
        bs.put(2, Arrays.asList("b2"));
        bs.put(3, Arrays.asList("b3"));

        try (CoGroupReader reader = reader(as, bs)) {
            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { "a1" },
                { "b1" },
            }));

            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { "a2" },
                { "b2" },
            }));

            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { "a3" },
                { "b3" },
            }));

            assertThat(reader.nextCoGroup(), is(false));
        }
    }

    /**
     * left group is empty.
     * @throws Exception if failed
     */
    @Test
    public void left_empty() throws Exception {
        SortedMap<Integer, List<String>> as = new TreeMap<>();

        SortedMap<Integer, List<String>> bs = new TreeMap<>();
        bs.put(1, Arrays.asList("a", "b"));

        try (CoGroupReader reader = reader(as, bs)) {
            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { },
                { "a", "b" },
            }));

            assertThat(reader.nextCoGroup(), is(false));
        }
    }

    /**
     * right group is empty.
     * @throws Exception if failed
     */
    @Test
    public void right_empty() throws Exception {
        SortedMap<Integer, List<String>> as = new TreeMap<>();
        as.put(1, Arrays.asList("a", "b"));

        SortedMap<Integer, List<String>> bs = new TreeMap<>();

        try (CoGroupReader reader = reader(as, bs)) {
            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { "a", "b" },
                { },
            }));

            assertThat(reader.nextCoGroup(), is(false));
        }
    }

    /**
     * both groups are empty.
     * @throws Exception if failed
     */
    @Test
    public void both_empty() throws Exception {
        SortedMap<Integer, List<String>> as = new TreeMap<>();
        SortedMap<Integer, List<String>> bs = new TreeMap<>();

        try (CoGroupReader reader = reader(as, bs)) {
            assertThat(reader.nextCoGroup(), is(false));
        }
    }

    /**
     * sparse groups.
     * @throws Exception if failed
     */
    @Test
    public void sparse() throws Exception {
        SortedMap<Integer, List<String>> as = new TreeMap<>();
        as.put(1, Arrays.asList("a"));

        SortedMap<Integer, List<String>> bs = new TreeMap<>();
        bs.put(2, Arrays.asList("b"));

        SortedMap<Integer, List<String>> cs = new TreeMap<>();
        cs.put(3, Arrays.asList("c"));

        SortedMap<Integer, List<String>> ds = new TreeMap<>();
        ds.put(4, Arrays.asList("d"));

        try (CoGroupReader reader = reader(as, bs, cs, ds)) {
            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { "a" }, { }, { }, { },
            }));

            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { }, { "b" }, { }, { },
            }));

            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { }, { }, { "c" }, { },
            }));

            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { }, { }, { }, { "d" },
            }));

            assertThat(reader.nextCoGroup(), is(false));
        }
    }

    /**
     * sparse groups.
     * @throws Exception if failed
     */
    @Test
    public void sparse_reverse() throws Exception {
        SortedMap<Integer, List<String>> as = new TreeMap<>();
        as.put(4, Arrays.asList("a"));

        SortedMap<Integer, List<String>> bs = new TreeMap<>();
        bs.put(3, Arrays.asList("b"));

        SortedMap<Integer, List<String>> cs = new TreeMap<>();
        cs.put(2, Arrays.asList("c"));

        SortedMap<Integer, List<String>> ds = new TreeMap<>();
        ds.put(1, Arrays.asList("d"));

        try (CoGroupReader reader = reader(as, bs, cs, ds)) {
            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { }, { }, { }, { "d" },
            }));

            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { }, { }, { "c" }, { },
            }));

            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { }, { "b" }, { }, { },
            }));

            assertThat(reader.nextCoGroup(), is(true));
            assertThat(collect(reader), is(new Object[][] {
                { "a" }, { }, { }, { },
            }));

            assertThat(reader.nextCoGroup(), is(false));
        }
    }

    private Object[][] collect(CoGroupReader reader) throws IOException, InterruptedException {
        List<List<Object>> results = new ArrayList<>();
        for (int i = 0, n = reader.getGroupCount(); i < n; i++) {
            List<Object> sink = new ArrayList<>();
            results.add(sink);
            ObjectCursor r = reader.getGroup(i);
            while (r.nextObject()) {
                sink.add(r.getObject());
            }
        }
        return results.stream().map(s -> s.stream().toArray()).toArray(Object[][]::new);
    }

    @SafeVarargs
    private static CoGroupReader reader(SortedMap<?, ? extends Collection<?>>... maps) {
        return new CoGroupReader(Lang.project(maps, CollectionGroupReader::new));
    }
}
