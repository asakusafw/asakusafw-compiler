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
package com.asakusafw.dag.runtime.table;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import com.asakusafw.dag.runtime.adapter.KeyBuffer;

/**
 * Test for {@link HeapKeyBuffer}.
 */
public class HeapKeyBufferTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        KeyBuffer a = newBuffer();
        KeyBuffer b = newBuffer();
        assertThat(a.getView(), is(a.getView()));
        assertThat(a.getView(), is(b.getView()));
        assertThat(a.getView().hashCode(), is(b.getView().hashCode()));

        append(a, 1);
        assertThat(a.getView(), is(not(b.getView())));

        append(b, 1);
        assertThat(a.getView().hashCode(), is(b.getView().hashCode()));

        append(a, 2);
        append(b, 3);
        assertThat(a.getView(), is(not(b.getView())));
    }

    /**
     * clear.
     */
    @Test
    public void clear() {
        KeyBuffer a = newBuffer();
        KeyBuffer b = newBuffer();

        append(a, 1, 2, 3);
        append(b, 1, 2, 3);
        assertThat(a.getView(), is(b.getView()));

        b.clear();
        assertThat(a.getView(), is(not(b.getView())));

        append(b, 1, 2, 3);
        assertThat(a.getView(), is(b.getView()));
    }

    /**
     * views.
     */
    @Test
    public void views() {
        KeyBuffer a = newBuffer();
        KeyBuffer b = newBuffer();

        KeyBuffer.View a1 = append(a, 1).getFrozen();
        KeyBuffer.View b1 = append(b, 1).getFrozen();
        assertThat(a1, is(a1));
        assertThat(a1, is(b1));
        assertThat(a1, is(a.getView()));
        assertThat(a1, is(b.getView()));
        assertThat(a.getView(), is(b1));
        assertThat(a.getView(), is(b1));
        assertThat(a1.hashCode(), is(b1.hashCode()));

        KeyBuffer.View a12 = append(a, 2).getFrozen();
        KeyBuffer.View b123 = append(b, 2, 3).getFrozen();
        assertThat(a12, is(not(b123)));

        append(a, 3);
        KeyBuffer.View a123 = a.getFrozen();
        assertThat(a12, is(not(a123)));
        assertThat(a123, is(b123));
    }

    private KeyBuffer newBuffer() {
        return new HeapKeyBuffer();
    }

    private KeyBuffer append(KeyBuffer buffer, int... values) {
        for (int value : values) {
            buffer.append(new IntWritable(value));
        }
        return buffer;
    }
}
