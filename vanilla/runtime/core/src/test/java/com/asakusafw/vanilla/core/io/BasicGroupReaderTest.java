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
package com.asakusafw.vanilla.core.io;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.junit.Test;

import com.asakusafw.vanilla.core.testing.ShortPairSerDe;

/**
 * Test for {@link BasicGroupReader}.
 */
public class BasicGroupReaderTest {

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        int[] inputs = {
                0x0010_0001,
        };
        try (BasicGroupReader r = new BasicGroupReader(cursor(inputs), new ShortPairSerDe())) {
            assertThat(r.nextGroup(), is(true));

            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(0x0010_0001));

            assertThat(r.nextObject(), is(false));
            assertThat(r.nextGroup(), is(false));
        }
    }

    /**
     * w/o records.
     * @throws Exception if failed
     */
    @Test
    public void empty_records() throws Exception {
        int[] inputs = {
        };
        try (BasicGroupReader r = new BasicGroupReader(cursor(inputs), new ShortPairSerDe())) {
            assertThat(r.nextGroup(), is(false));
        }
    }

    /**
     * w/ multiple records in the same group.
     * @throws Exception if failed
     */
    @Test
    public void multiple_records() throws Exception {
        int[] inputs = {
                0x0010_0001, 0x0010_0002, 0x0010_0003,
        };
        try (BasicGroupReader r = new BasicGroupReader(cursor(inputs), new ShortPairSerDe())) {
            assertThat(r.nextGroup(), is(true));

            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(0x0010_0001));

            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(0x0010_0002));

            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(0x0010_0003));

            assertThat(r.nextObject(), is(false));
            assertThat(r.nextGroup(), is(false));
        }
    }

    /**
     * w/ multiple groups.
     * @throws Exception if failed
     */
    @Test
    public void multiple_groups() throws Exception {
        int[] inputs = {
                0x0010_0001,
                0x0011_0002,
                0x0012_0003
        };
        try (BasicGroupReader r = new BasicGroupReader(cursor(inputs), new ShortPairSerDe())) {
            assertThat(r.nextGroup(), is(true));
            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(0x0010_0001));
            assertThat(r.nextObject(), is(false));

            assertThat(r.nextGroup(), is(true));
            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(0x0011_0002));
            assertThat(r.nextObject(), is(false));

            assertThat(r.nextGroup(), is(true));
            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(0x0012_0003));
            assertThat(r.nextObject(), is(false));

            assertThat(r.nextGroup(), is(false));
        }
    }

    /**
     * w/ skip records in group.
     * @throws Exception if failed
     */
    @Test
    public void skip_groups() throws Exception {
        int[] inputs = {
                0x0010_0000,
                0x0011_0001, 0x0011_0002,
                0x0012_0003, 0x0012_0004, 0x0012_0005,
                0x0013_0006,
        };
        try (BasicGroupReader r = new BasicGroupReader(cursor(inputs), new ShortPairSerDe())) {
            assertThat(r.nextGroup(), is(true));
            assertThat(r.getGroup().getValue(), is((short) 0x0010));

            assertThat(r.nextGroup(), is(true));
            assertThat(r.getGroup().getValue(), is((short) 0x0011));

            assertThat(r.nextGroup(), is(true));
            assertThat(r.getGroup().getValue(), is((short) 0x0012));

            assertThat(r.nextGroup(), is(true));
            assertThat(r.getGroup().getValue(), is((short) 0x0013));
            assertThat(r.nextObject(), is(true));
            assertThat(r.getObject(), is(0x0013_0006));
            assertThat(r.nextObject(), is(false));

            assertThat(r.nextGroup(), is(false));
        }
    }

    private static KeyValueCursor cursor(int[] records){
        return ShortPairSerDe.cursor(records, 0, records.length);
    }
}
